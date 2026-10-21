// Copyright 2026 Kalshi Trading.
// Copyright 2026 Adaptive Financial Consulting Limited (Java reference implementation).
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package persistentsubscription_test

import (
	"encoding/binary"
	"fmt"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lirm/aeron-go/aeron"
	aeronatomic "github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/logbuffer"
	"github.com/lirm/aeron-go/aeron/logbuffer/term"
	"github.com/lirm/aeron-go/archive"
	"github.com/lirm/aeron-go/archive/codecs"
	ps "github.com/lirm/aeron-go/archive/persistentsubscription"
)

// E2E tests run against the docker Java 1.53.3 ArchivingMediaDriver (docker/docker-compose.yml):
//
//	docker compose -f docker/docker-compose.yml up -d --build archiving-driver
//	AERON_E2E=1 AERON_DIR=/dev/shm/aeron-compose go test ./archive/persistentsubscription/ -run E2E -v

const e2eTermLength = 64 * 1024

func e2eAeronDir(t *testing.T) string {
	if os.Getenv("AERON_E2E") != "1" {
		t.Skip("set AERON_E2E=1 to run against the docker archiving media driver")
	}
	if dir := os.Getenv("AERON_DIR"); dir != "" {
		return dir
	}
	return "/dev/shm/aeron-compose"
}

type e2eEnv struct {
	t        *testing.T
	aeronDir string
	aeron    *aeron.Aeron
	archive  *archive.Archive
	streamID int32
	channel  string
}

func newE2EEnv(t *testing.T) *e2eEnv {
	dir := e2eAeronDir(t)
	a, err := aeron.Connect(aeron.NewContext().AeronDir(dir))
	if err != nil {
		t.Fatalf("aeron connect: %v", err)
	}
	t.Cleanup(func() { _ = a.Close() })
	arch, err := archive.NewArchive(archive.DefaultOptions(), aeron.NewContext().AeronDir(dir))
	if err != nil {
		t.Fatalf("archive connect: %v", err)
	}
	t.Cleanup(func() { _ = arch.Close() })
	return &e2eEnv{
		t:        t,
		aeronDir: dir,
		aeron:    a,
		archive:  arch,
		streamID: int32(100000 + rand.Intn(1000000)),
		channel:  fmt.Sprintf("aeron:ipc?term-length=%d", e2eTermLength),
	}
}

func (e *e2eEnv) addPublication(channel string) *aeron.ExclusivePublication {
	pub, err := e.aeron.AddExclusivePublication(channel, e.streamID)
	if err != nil {
		e.t.Fatalf("add publication: %v", err)
	}
	return pub
}

func (e *e2eEnv) awaitRecording(sessionID int32) *codecs.RecordingDescriptor {
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		descriptors, err := e.archive.ListRecordingsForUri(0, 1000, "aeron:ipc", e.streamID)
		if err == nil {
			for _, d := range descriptors {
				if d.SessionId == sessionID {
					return d
				}
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	e.t.Fatalf("recording for session %d not found", sessionID)
	return nil
}

// resumeChannel builds a publication channel continuing exactly at the recording's stop position.
func (e *e2eEnv) resumeChannel(recordingID int64) string {
	deadline := time.Now().Add(10 * time.Second)
	for {
		d, err := e.archive.ListRecording(recordingID)
		if err != nil {
			e.t.Fatalf("list recording: %v", err)
		}
		if d.StopPosition != aeron.NullValue {
			termLength := int64(d.TermBufferLength)
			termID := int64(d.InitialTermId) + d.StopPosition/termLength
			return fmt.Sprintf("%s|init-term-id=%d|term-id=%d|term-offset=%d", e.channel, d.InitialTermId,
				int32(termID), d.StopPosition%termLength)
		}
		if time.Now().After(deadline) {
			e.t.Fatalf("recording %d did not stop", recordingID)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// message layout: seq int64 | payload length int32 | payload bytes (derived from seq).
func encodeMessage(seq int64) []byte {
	size := 64
	if seq%50 == 0 {
		size = 5000 // larger than the MTU: exercises fragment reassembly, including across replay->live
	}
	msg := make([]byte, 12+size)
	binary.LittleEndian.PutUint64(msg, uint64(seq))
	binary.LittleEndian.PutUint32(msg[8:], uint32(size))
	for i := 0; i < size; i++ {
		msg[12+i] = byte(seq + int64(i))
	}
	return msg
}

func offer(t *testing.T, pub *aeron.ExclusivePublication, msg []byte) {
	buf := aeronatomic.MakeBuffer(msg, len(msg))
	deadline := time.Now().Add(10 * time.Second)
	for pub.Offer(buf, 0, int32(len(msg)), nil) < 0 {
		if time.Now().After(deadline) {
			t.Fatalf("offer timed out")
		}
		time.Sleep(50 * time.Microsecond)
	}
}

type checker struct {
	mu       sync.Mutex
	next     int64
	errors   []string
	received atomic.Int64
}

func (c *checker) onMessage(buffer *aeronatomic.Buffer, offset int32, length int32, header *logbuffer.Header) {
	msg := buffer.GetBytesArray(offset, length)
	seq := int64(binary.LittleEndian.Uint64(msg))
	c.mu.Lock()
	defer c.mu.Unlock()
	// Reassembled messages carry Java's BufferBuilder.completeHeader: BEGIN|END flags, whole-message frame length.
	if header.Flags() != 0xC0 || header.FrameLength() != logbuffer.DataFrameHeader.Length+length {
		c.errors = append(c.errors, fmt.Sprintf("seq %d: header flags=%#x frameLength=%d for length %d", seq,
			header.Flags(), header.FrameLength(), length))
	}
	if seq != c.next {
		kind := "gap"
		if seq < c.next {
			kind = "duplicate"
		}
		c.errors = append(c.errors, fmt.Sprintf("%s: expected seq %d got %d", kind, c.next, seq))
	}
	expected := encodeMessage(seq)
	if string(expected) != string(msg) {
		c.errors = append(c.errors, fmt.Sprintf("corrupt payload for seq %d (len %d, want %d)", seq, len(msg),
			len(expected)))
	}
	c.next = seq + 1
	c.received.Add(1)
}

type recordingListener struct {
	joined, left atomic.Int32
	mu           sync.Mutex
	errs         []error
}

func (l *recordingListener) OnLiveJoined() { l.joined.Add(1) }
func (l *recordingListener) OnLiveLeft()   { l.left.Add(1) }
func (l *recordingListener) OnError(err error) {
	l.mu.Lock()
	l.errs = append(l.errs, err)
	l.mu.Unlock()
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func waitFor(t *testing.T, what string, timeout time.Duration, cond func() bool) {
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

// TestE2EReplayLiveAndPublisherRestart records a stream, consumes it from the start with a PersistentSubscription
// while publishing, restarts the publisher twice (forcing the subscription off live and back through replay) and
// verifies every message is received exactly once, in order and intact.
func TestE2EReplayLiveAndPublisherRestart(t *testing.T) {
	cases := []struct {
		name          string
		replayChannel string
		controlled    bool
	}{
		{"ipc-replay/poll", "aeron:ipc", false},
		{"udp-dynamic-port-replay/controlled-poll", "aeron:udp?endpoint=localhost:0", true},
		{"udp-response-channel-replay/poll",
			fmt.Sprintf("aeron:udp?control-mode=response|control=localhost:%d", 20000+rand.Intn(20000)), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) { runReplayLiveAndRestart(t, tc.replayChannel, tc.controlled) })
	}
}

func runReplayLiveAndRestart(t *testing.T, replayChannel string, controlled bool) {
	env := newE2EEnv(t)
	if _, err := env.archive.StartRecording(env.channel, env.streamID, true, true); err != nil {
		t.Fatalf("start recording: %v", err)
	}
	t.Cleanup(func() { _ = env.archive.StopRecording(env.channel, env.streamID) })

	pub := env.addPublication(env.channel)
	recording := env.awaitRecording(pub.SessionID())
	recordingID := recording.RecordingId

	var seq int64
	publish := func(p *aeron.ExclusivePublication, n int) {
		for i := 0; i < n; i++ {
			offer(t, p, encodeMessage(seq))
			seq++
		}
	}
	publish(pub, 2000) // recorded history the subscription must replay

	listener := &recordingListener{}
	options := archive.DefaultOptions()
	options.Timeout = 2 * time.Second
	ctx := ps.NewContext()
	ctx.AeronDir = env.aeronDir
	ctx.Aeron = env.aeron
	ctx.RecordingID = recordingID
	ctx.StartPosition = ps.FromStart
	ctx.LiveChannel = env.channel
	ctx.LiveStreamID = env.streamID
	ctx.ReplayChannel = replayChannel
	ctx.ReplayStreamID = env.streamID + 1
	ctx.Listener = listener
	ctx.ArchiveOptions = options
	sub, err := ps.New(ctx)
	if err != nil {
		t.Fatalf("create persistent subscription: %v", err)
	}

	c := &checker{}
	var state atomic.Int32
	var replays atomic.Int32
	var paused, pauseAck atomic.Bool
	controlledHandler := func(buffer *aeronatomic.Buffer, offset int32, length int32,
		header *logbuffer.Header) term.ControlledPollAction {
		c.onMessage(buffer, offset, length, header)
		return term.ControlledPollActionContinue
	}
	var failed atomic.Value
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer sub.Close()
		for {
			select {
			case <-stop:
				return
			default:
			}
			if paused.Load() {
				pauseAck.Store(true)
				time.Sleep(time.Millisecond)
				continue
			}
			pauseAck.Store(false)
			var work int
			if controlled {
				work = sub.ControlledPoll(controlledHandler, 10)
			} else {
				work = sub.Poll(c.onMessage, 10)
			}
			if work == 0 {
				time.Sleep(50 * time.Microsecond)
			}
			if s := sub.State(); s != ps.State(state.Load()) {
				if s == ps.StateReplay {
					replays.Add(1)
				}
				state.Store(int32(s))
			}
			if sub.HasFailed() {
				failed.Store(sub.FailureReason())
				return
			}
		}
	}()
	defer func() { close(stop); <-done }()
	isLive := func() bool { return ps.State(state.Load()) == ps.StateLive }
	checkFailed := func() {
		if err, _ := failed.Load().(error); err != nil {
			t.Fatalf("persistent subscription failed: %v", err)
		}
	}

	// Publish concurrently while the subscription replays, then catches up and joins live.
	publishDone := make(chan struct{})
	go func() {
		defer close(publishDone)
		for i := 0; i < 20; i++ {
			publish(pub, 100)
			time.Sleep(5 * time.Millisecond)
		}
	}()
	<-publishDone
	waitFor(t, "live", 30*time.Second, func() bool { checkFailed(); return isLive() })
	waitFor(t, "all messages", 30*time.Second, func() bool { checkFailed(); return c.received.Load() == seq })
	t.Logf("joined live after replay: received=%d joined=%d", c.received.Load(), listener.joined.Load())

	for restart := 0; restart < 2; restart++ {
		// Kill the live publisher: the live image goes away and the recording stops. On the second restart the
		// subscriber is paused while the publisher is down and restarted, so when it resumes the live stream is
		// ahead and it must fall back to replaying the gap from the archive before switching back to live.
		replaysBefore := replays.Load()
		forceReplay := restart == 1
		if forceReplay {
			paused.Store(true)
			waitFor(t, "pause", 5*time.Second, pauseAck.Load)
		}
		_ = pub.Close()
		if !forceReplay {
			waitFor(t, "live left", 30*time.Second, func() bool {
				checkFailed()
				return int(listener.left.Load()) > restart
			})
		}

		// Restart the publisher exactly at the stop position and extend the recording, publishing immediately so
		// the live stream gets ahead of the subscription, which must replay the gap before re-joining live.
		channel := env.resumeChannel(recordingID)
		pub = env.addPublication(channel)
		if _, err := env.archive.ExtendRecording(recordingID, env.streamID, codecs.SourceLocation.LOCAL, true,
			channel); err != nil {
			t.Fatalf("extend recording: %v", err)
		}
		// While paused, the old live subscription also holds an image of the new IPC publication, which flow
		// controls the publisher, so stay within the publisher window.
		if forceReplay {
			publish(pub, 100)
		} else {
			publish(pub, 1500)
		}
		paused.Store(false)
		waitFor(t, "rejoin live", 30*time.Second, func() bool {
			checkFailed()
			return int(listener.joined.Load()) > restart+1 && isLive()
		})
		publish(pub, 500)
		waitFor(t, "all messages", 30*time.Second, func() bool { checkFailed(); return c.received.Load() == seq })
		if forceReplay && replays.Load() == replaysBefore {
			t.Fatalf("expected the subscription to fall back to replay after the publisher restart")
		}
		t.Logf("restart %d: received=%d joined=%d left=%d replays=%d", restart, c.received.Load(),
			listener.joined.Load(), listener.left.Load(), replays.Load())
	}
	_ = pub.Close()

	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.errors) > 0 {
		t.Fatalf("%d sequence errors, first: %v", len(c.errors), c.errors[:minInt(5, len(c.errors))])
	}
	if c.received.Load() != seq {
		t.Fatalf("received %d messages, published %d", c.received.Load(), seq)
	}
	listener.mu.Lock()
	for _, err := range listener.errs {
		t.Logf("listener error (non-terminal): %v", err)
	}
	listener.mu.Unlock()
}
