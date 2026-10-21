// Copyright 2026 Kalshi Trading
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

// Package imagee2e holds end-to-end tests of aeron.Subscription / aeron.Image against a Java 1.53.3 media driver
// (docker/docker-compose.yml) with a Java publisher. Run with:
//
//	AERON_E2E=1 AERON_DIR=/dev/shm/aeron-compose AERON_JAR=~/jars/aeron-all-1.53.3.jar go test ./systests/imagee2e/
package imagee2e

import (
	"bufio"
	"io"
	"math"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lirm/aeron-go/aeron"
	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/logbuffer"
	"github.com/lirm/aeron-go/aeron/logbuffer/term"
	"github.com/stretchr/testify/require"
)

const (
	messageCount  = 40
	messageLength = 32
	frameLength   = 64 // 32 byte header + 32 byte payload, aligned
	timeout       = 20 * time.Second
)

type javaPublisher struct {
	cmd   *exec.Cmd
	stdin io.WriteCloser
	mu    sync.Mutex
	lines []string
}

func startJavaPublisher(t *testing.T, aeronDir, channel, streamID string) *javaPublisher {
	jar := os.Getenv("AERON_JAR")
	if jar == "" {
		t.Skip("AERON_JAR must point at aeron-all-1.53.3.jar")
	}
	cmd := exec.Command("java",
		"--add-opens=java.base/jdk.internal.misc=ALL-UNNAMED", "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
		"-cp", jar, "-Daeron.dir="+aeronDir, "testdata/E2EPublisher.java",
		channel, streamID, strconv.Itoa(messageCount), strconv.Itoa(messageLength))
	stdin, err := cmd.StdinPipe()
	require.NoError(t, err)
	stdout, err := cmd.StdoutPipe()
	require.NoError(t, err)
	cmd.Stderr = os.Stderr
	require.NoError(t, cmd.Start())
	p := &javaPublisher{cmd: cmd, stdin: stdin}
	go func() {
		scanner := bufio.NewScanner(stdout)
		for scanner.Scan() {
			t.Logf("java: %s", scanner.Text())
			p.mu.Lock()
			p.lines = append(p.lines, scanner.Text())
			p.mu.Unlock()
		}
	}()
	t.Cleanup(func() {
		_ = stdin.Close()
		done := make(chan struct{})
		go func() { _ = cmd.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			_ = cmd.Process.Kill()
		}
	})
	return p
}

func (p *javaPublisher) hasLine(prefix string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, l := range p.lines {
		if strings.HasPrefix(l, prefix) {
			return true
		}
	}
	return false
}

func (p *javaPublisher) close(t *testing.T) {
	_, err := io.WriteString(p.stdin, "close\n")
	require.NoError(t, err)
}

func eventually(t *testing.T, msg string, cond func() bool) {
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", msg)
		}
		time.Sleep(time.Millisecond)
	}
}

func connect(t *testing.T) (*aeron.Aeron, string, chan aeron.Image, chan aeron.Image) {
	if os.Getenv("AERON_E2E") != "1" {
		t.Skip("set AERON_E2E=1 to run against the docker Java media driver")
	}
	dir := os.Getenv("AERON_DIR")
	if dir == "" {
		dir = "/dev/shm/aeron-compose"
	}
	available := make(chan aeron.Image, 8)
	unavailable := make(chan aeron.Image, 8)
	ctx := aeron.NewContext().AeronDir(dir).MediaDriverTimeout(10 * time.Second).
		AvailableImageHandler(func(image aeron.Image) { available <- image }).
		UnavailableImageHandler(func(image aeron.Image) { unavailable <- image })
	a, err := aeron.Connect(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { _ = a.Close() })
	return a, dir, available, unavailable
}

func payloadIndex(buffer *atomic.Buffer, offset int32) int {
	s := string(buffer.GetBytesArray(offset, 9))
	n := 0
	for _, c := range s[4:] {
		n = n*10 + int(c-'0')
	}
	return n
}

func awaitImage(t *testing.T, ch chan aeron.Image) aeron.Image {
	select {
	case image := <-ch:
		return image
	case <-time.After(timeout):
		t.Fatal("timed out waiting for image")
		return nil
	}
}

// TestJavaPublisherToGoImagePolls drives controlled, bounded, peek and block polls over real data from a Java
// publisher, then verifies end of stream after the publisher closes.
func TestJavaPublisherToGoImagePolls(t *testing.T) {
	a, dir, available, unavailable := connect(t)
	channel := "aeron:udp?endpoint=localhost:20131"
	sub, err := a.AddSubscription(channel, 2001)
	require.NoError(t, err)
	defer sub.Close()

	pub := startJavaPublisher(t, dir, channel, "2001")
	image := awaitImage(t, available)
	require.Equal(t, sub.RegistrationID(), image.SubscriptionRegistrationID())
	require.Same(t, sub, image.Subscription())
	require.Equal(t, image, sub.ImageBySessionID(image.SessionID()))
	require.Equal(t, image, sub.ImageAtIndex(0))
	require.Equal(t, 1, sub.ImageCount())
	require.True(t, sub.IsConnected())
	require.Equal(t, aeron.ChannelStatusActive, sub.ChannelStatus())
	require.Equal(t, "127.0.0.1:20131", sub.ResolvedEndpoint())
	require.Equal(t, []string{"127.0.0.1:20131"}, sub.LocalSocketAddresses())
	require.Contains(t, image.SourceIdentity(), "127.0.0.1:")
	require.Equal(t, int32(1408), image.MtuLength())
	require.Greater(t, image.TermBufferLength(), int32(0))
	require.Greater(t, image.SubscriberPositionID(), int32(0))
	require.Equal(t, int64(math.MaxInt64), image.EndOfStreamPosition())

	join := image.JoinPosition()
	require.Equal(t, join, image.Position())

	// Wait until all messages are in the log without consuming them.
	eventually(t, "all messages", func() bool {
		count := 0
		_, err := image.ControlledPeek(image.Position(), func(*atomic.Buffer, int32, int32, *logbuffer.Header) term.ControlledPollAction {
			count++
			return term.ControlledPollActionContinue
		}, math.MaxInt64)
		require.NoError(t, err)
		return count == messageCount
	})
	require.Equal(t, join, image.Position(), "peek does not consume")
	eventually(t, "publisher done", func() bool { return pub.hasLine("PUBLISHED") })

	var received []int
	record := func(buffer *atomic.Buffer, offset int32, length int32, header *logbuffer.Header) {
		require.Equal(t, int32(messageLength), length)
		require.Equal(t, image.SessionID(), header.SessionID())
		require.Equal(t, int32(2001), header.StreamID())
		require.Equal(t, header.TermOffset()+frameLength, header.NextTermOffset())
		require.Equal(t, uint8(0xC0), header.Flags())
		received = append(received, payloadIndex(buffer, offset))
	}

	// ControlledPoll: ABORT leaves the fragment unconsumed and ends the poll.
	fragments := image.ControlledPoll(func(*atomic.Buffer, int32, int32, *logbuffer.Header) term.ControlledPollAction {
		return term.ControlledPollActionAbort
	}, 10)
	require.Equal(t, 0, fragments)
	require.Equal(t, join, image.Position())

	// ControlledPoll: CONTINUE then BREAK.
	n := 0
	fragments = image.ControlledPoll(func(b *atomic.Buffer, o int32, l int32, h *logbuffer.Header) term.ControlledPollAction {
		n++
		switch n {
		case 1:
			record(b, o, l, h)
			return term.ControlledPollActionContinue
		default:
			record(b, o, l, h)
			return term.ControlledPollActionBreak
		}
	}, 10)
	require.Equal(t, 2, fragments)
	require.Equal(t, join+2*frameLength, image.Position())

	// ControlledPoll with COMMIT updates the position as it goes.
	fragments = image.ControlledPoll(func(b *atomic.Buffer, o int32, l int32, h *logbuffer.Header) term.ControlledPollAction {
		record(b, o, l, h)
		require.Equal(t, h.Position()-frameLength, image.Position())
		return term.ControlledPollActionCommit
	}, 3)
	require.Equal(t, 3, fragments)

	// BoundedControlledPoll and BoundedPoll stop at the limit position.
	limit := image.Position() + 4*frameLength
	fragments = image.BoundedControlledPoll(func(b *atomic.Buffer, o int32, l int32, h *logbuffer.Header) term.ControlledPollAction {
		record(b, o, l, h)
		return term.ControlledPollActionContinue
	}, limit, 100)
	require.Equal(t, 4, fragments)
	require.Equal(t, limit, image.Position())
	limit += 5 * frameLength
	require.Equal(t, 5, image.BoundedPoll(record, limit, 100))
	require.Equal(t, limit, image.Position())

	// ControlledPeek + SetPosition consumes after the fact.
	peeked := 0
	newPosition, err := image.ControlledPeek(image.Position(), func(b *atomic.Buffer, o int32, l int32, h *logbuffer.Header) term.ControlledPollAction {
		peeked++
		record(b, o, l, h)
		if peeked == 6 {
			return term.ControlledPollActionBreak
		}
		return term.ControlledPollActionContinue
	}, math.MaxInt64)
	require.NoError(t, err)
	require.Equal(t, image.Position()+6*frameLength, newPosition)
	require.NoError(t, image.SetPosition(newPosition))
	require.Error(t, image.SetPosition(newPosition-frameLength))

	// BlockPoll the rest in blocks of whole frames.
	for len(received) < messageCount {
		bytes := image.BlockPoll(func(buffer *atomic.Buffer, offset int32, length int32, sessionID int32, termID int32) {
			require.Equal(t, image.SessionID(), sessionID)
			require.Equal(t, int32(0), length%frameLength)
			for o := offset; o < offset+length; o += frameLength {
				require.Equal(t, termID, buffer.GetInt32(o+logbuffer.DataFrameHeader.TermIDFieldOffset))
				received = append(received, payloadIndex(buffer, o+logbuffer.DataFrameHeader.Length))
			}
		}, 3*frameLength+10)
		require.LessOrEqual(t, bytes, int32(3*frameLength))
		require.Greater(t, bytes, int32(0))
	}
	for i := 0; i < messageCount; i++ {
		require.Equal(t, i, received[i])
	}
	require.Equal(t, join+messageCount*frameLength, image.Position())
	require.False(t, image.IsEndOfStream())

	// Publisher close: EOS position is published and the image goes unavailable.
	pub.close(t)
	eventually(t, "end of stream", image.IsEndOfStream)
	require.Equal(t, image.Position(), image.EndOfStreamPosition())
	gone := awaitImage(t, unavailable)
	require.Equal(t, image.CorrelationID(), gone.CorrelationID())
	require.True(t, image.IsClosed())
	require.True(t, image.IsEndOfStream())
	require.Equal(t, join+messageCount*frameLength, image.Position())
	require.Equal(t, 0, image.Poll(record, 10))
	require.True(t, sub.HasNoImages())
	require.False(t, sub.IsConnected())
}

// TestRejectImage rejects an image from a Java publisher: the driver removes it from the Go subscription and the
// Java publisher receives the error frame with the reason.
func TestRejectImage(t *testing.T) {
	a, dir, available, unavailable := connect(t)
	channel := "aeron:udp?endpoint=localhost:20132"
	sub, err := a.AddSubscription(channel, 2002)
	require.NoError(t, err)
	defer sub.Close()

	pub := startJavaPublisher(t, dir, channel, "2002")
	image := awaitImage(t, available)
	eventually(t, "messages", func() bool {
		image.Poll(func(*atomic.Buffer, int32, int32, *logbuffer.Header) {}, 10)
		return image.Position() >= image.JoinPosition()+messageCount*frameLength
	})

	require.NoError(t, image.Reject("go e2e reject"))
	gone := awaitImage(t, unavailable)
	require.Equal(t, image.CorrelationID(), gone.CorrelationID())
	require.True(t, image.IsClosed())
	eventually(t, "error frame at publisher", func() bool { return pub.hasLine("REJECTED: go e2e reject") })

	spy, err := a.AddSubscription("aeron-spy:"+channel, 2002)
	require.NoError(t, err)
	defer spy.Close()
	require.ErrorIs(t, spy.RejectImage(image.CorrelationID(), 0, "spy"), aeron.ErrSpyRejectImage)
}
