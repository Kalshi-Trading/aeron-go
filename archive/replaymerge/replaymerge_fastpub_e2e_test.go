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

package replaymerge_test

import (
	"fmt"
	"os"
	"sync"
	syncatomic "sync/atomic"
	"testing"
	"time"

	"github.com/lirm/aeron-go/aeron"
	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/logbuffer"
	"github.com/lirm/aeron-go/archive"
	"github.com/lirm/aeron-go/archive/replaymerge"
	"github.com/stretchr/testify/require"
)

// A publisher goroutine offers variable length messages as fast as the publication accepts them, before, during and
// after the merge, while the test goroutine drives ReplayMerge. Every fragment must arrive in sequence, with the
// expected payload, at exactly the stream position following the previous fragment: no gaps, no duplicates and no
// reordering across the replay -> live switch.

const (
	fastPubTermLength = 256 * 1024
	fastPubMTU        = 1408
	fastPubTimeout    = 90 * time.Second
	// Before the merge the publisher is limited to fastPubRatePerMs so that the replay can catch up with it (an
	// unthrottled publisher with no live receiver is only limited by the recorder, which a replay can never outrun).
	// After the merge it runs unthrottled when min flow control holds it back to the merged subscriber. fastPubMaxMessages bounds the volume
	// written to the archive.
	fastPubRatePerMs   = 100
	fastPubMaxMessages = 3_000_000
)

type fastPubCase struct {
	name            string
	basePort        int
	streamID        int32
	flowControl     string
	ephemeralReplay bool
	preloadMessages int64
	midStart        bool
}

func TestReplayMergeFastPublisherE2E(t *testing.T) {
	if os.Getenv("AERON_E2E") != "1" {
		t.Skip("set AERON_E2E=1 to run against a Java archiving media driver")
	}
	dir := os.Getenv("AERON_DIR")
	if dir == "" {
		dir = "/dev/shm/aeron-compose"
	}

	ctx := aeron.NewContext().AeronDir(dir).MediaDriverTimeout(10 * time.Second)
	arch, err := archive.NewArchive(archive.DefaultOptions(), ctx)
	require.NoError(t, err)
	defer arch.Close()

	base := int32(time.Now().Unix()%1000) * 10
	cases := []fastPubCase{
		{name: "MinFlowControlEphemeralPort", basePort: 24350, streamID: 42000 + base, flowControl: "min",
			ephemeralReplay: true, preloadMessages: 20_000},
		{name: "MaxFlowControlMidPosition", basePort: 24360, streamID: 42001 + base, preloadMessages: 100_000,
			midStart: true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) { runFastPublisherMerge(t, arch, c) })
	}
}

func fastPubMessageLength(seq int64) int32 {
	return 16 + int32(seq%11)*24 // 16..256 bytes, frame lengths vary and are not all 32 byte multiples
}

func runFastPublisherMerge(t *testing.T, arch *archive.Archive, c fastPubCase) {
	controlPort, livePort, replayPort, recordingPort := c.basePort, c.basePort+1, c.basePort+2, c.basePort+3

	pubChannel := fmt.Sprintf("aeron:udp?control=localhost:%d|control-mode=dynamic|term-length=%d|mtu=%d",
		controlPort, fastPubTermLength, fastPubMTU)
	if c.flowControl != "" {
		pubChannel += "|fc=" + c.flowControl
	}
	pub, err := arch.AddExclusivePublication(pubChannel, c.streamID)
	require.NoError(t, err)
	defer pub.Close()
	sessionID := pub.SessionID()

	recordingChannel := fmt.Sprintf("aeron:udp?endpoint=localhost:%d|control=localhost:%d|session-id=%d",
		recordingPort, controlPort, sessionID)
	_, err = arch.StartRecording(recordingChannel, c.streamID, false, true)
	require.NoError(t, err)
	defer func() { _ = arch.StopRecording(recordingChannel, c.streamID) }()
	recordingID := awaitRecordingID(t, arch, c.streamID, sessionID)

	var (
		published    syncatomic.Int64
		pubPosition  syncatomic.Int64
		stop         syncatomic.Bool
		unthrottled  syncatomic.Bool
		offerFailure syncatomic.Value
		wg           sync.WaitGroup
	)
	publishUntil := func(limit int64) {
		buffer := atomic.MakeBuffer(make([]byte, 512))
		start, startSeq := time.Now(), published.Load()
		for seq := startSeq; seq < limit && seq < fastPubMaxMessages && !stop.Load(); {
			if !unthrottled.Load() && seq-startSeq >= int64(time.Since(start).Milliseconds()+1)*fastPubRatePerMs {
				time.Sleep(50 * time.Microsecond)
				continue
			}
			length := fastPubMessageLength(seq)
			buffer.PutInt64(0, seq)
			for i := int32(8); i < length; i++ {
				buffer.PutUInt8(i, uint8(seq)+uint8(i))
			}
			r := pub.Offer(buffer, 0, length, nil)
			switch {
			case r > 0:
				seq++
				published.Store(seq)
				pubPosition.Store(r)
			case r == aeron.PublicationClosed || r == aeron.MaxPositionExceeded || r == aeron.NotConnected:
				offerFailure.Store(fmt.Errorf("offer failed: %d at seq %d", r, seq))
				return
			}
		}
	}

	deadline := time.Now().Add(fastPubTimeout)
	unthrottled.Store(true)
	publishUntil(c.preloadMessages)
	unthrottled.Store(false)
	require.Nil(t, offerFailure.Load())
	awaitRecordedPosition(t, arch, recordingID, pubPosition.Load())

	startPosition := int64(0)
	expected := int64(0)
	if c.midStart {
		// Start from the beginning of a message half way into the recording.
		var pos int64
		for seq := int64(0); seq < c.preloadMessages/2; seq++ {
			_, pos = fastPubFrame(pos, fastPubMessageLength(seq))
		}
		startPosition = pos
		expected = c.preloadMessages / 2
	}
	firstExpected := expected

	subChannel := fmt.Sprintf("aeron:udp?control-mode=manual|session-id=%d", sessionID)
	sub, err := arch.AddSubscription(subChannel, c.streamID)
	require.NoError(t, err)
	defer sub.Close()

	replayDestination := fmt.Sprintf("aeron:udp?endpoint=localhost:%d", replayPort)
	if c.ephemeralReplay {
		replayDestination = "aeron:udp?endpoint=localhost:0"
	}
	liveDestination := fmt.Sprintf("aeron:udp?endpoint=localhost:%d|control=localhost:%d", livePort, controlPort)
	replayChannel := fmt.Sprintf("aeron:udp?session-id=%d", sessionID)

	merge, err := replaymerge.NewReplayMergeWithDefaults(sub, arch, replayChannel, replayDestination,
		liveDestination, recordingID, startPosition)
	require.NoError(t, err)
	defer merge.Close()

	// From here on the publisher runs flat out in its own goroutine.
	wg.Add(1)
	go func() {
		defer wg.Done()
		publishUntil(1 << 62)
	}()
	defer func() {
		stop.Store(true)
		wg.Wait()
	}()

	nextPosition := startPosition
	var failure error
	sawLive, sawReplay := false, false
	onMessage := func(buf *atomic.Buffer, offset int32, length int32, header *logbuffer.Header) {
		if failure != nil {
			return
		}
		seq := buf.GetInt64(offset)
		if seq != expected || length != fastPubMessageLength(expected) {
			failure = fmt.Errorf("expected seq %d len %d, got seq %d len %d at position %d (merged=%t)", expected,
				fastPubMessageLength(expected), seq, length, header.Position(), merge.IsMerged())
			return
		}
		for i := int32(8); i < length; i++ {
			if buf.GetUInt8(offset+i) != uint8(seq)+uint8(i) {
				failure = fmt.Errorf("corrupt payload for seq %d at byte %d", seq, i)
				return
			}
		}
		wantStart, wantEnd := fastPubFrame(nextPosition, length)
		if header.Position() != wantEnd {
			failure = fmt.Errorf("seq %d ends at position %d, expected frame [%d, %d)", seq, header.Position(),
				wantStart, wantEnd)
			return
		}
		nextPosition = wantEnd
		expected++
		if merge.IsLiveAdded() {
			sawLive = true
		} else {
			sawReplay = true
		}
	}
	poll := func() int {
		n, err := merge.Poll(onMessage, 50)
		require.NoError(t, err, merge.String())
		require.NoError(t, failure)
		if f := offerFailure.Load(); f != nil {
			require.NoError(t, f.(error))
		}
		return n
	}

	for !merge.IsMerged() {
		require.True(t, time.Now().Before(deadline), "timed out merging: %s, received=%d published=%d",
			merge.String(), expected, published.Load())
		if poll() == 0 {
			time.Sleep(10 * time.Microsecond)
		}
	}
	// With max flow control the publisher is only held back by the fastest receiver (the recorder), so a subscriber
	// that cannot keep up with an unthrottled publisher is overrun regardless of how it joined.
	unthrottled.Store(c.flowControl == "min")
	mergedAt, publishedAtMerge := expected, published.Load()
	t.Logf("merged: %s, received=%d published=%d", merge.String(), mergedAt, publishedAtMerge)
	require.False(t, merge.HasFailed())
	if c.ephemeralReplay {
		uri, err := aeron.ParseChannelUri(merge.ReplayChannel())
		require.NoError(t, err)
		require.NotContains(t, uri.Get(aeron.EndpointParamName), ":0")
	}

	// Keep the publisher going flat out on the live stream only, then stop it and drain.
	liveUntil := time.Now().Add(time.Second)
	for time.Now().Before(liveUntil) {
		require.True(t, time.Now().Before(deadline))
		if poll() == 0 {
			time.Sleep(10 * time.Microsecond)
		}
	}
	stop.Store(true)
	wg.Wait()
	total := published.Load()
	for expected < total {
		require.True(t, time.Now().Before(deadline), "timed out draining: received=%d published=%d", expected, total)
		if poll() == 0 {
			time.Sleep(10 * time.Microsecond)
		}
	}
	require.NoError(t, failure)
	require.Equal(t, total, expected)
	require.Equal(t, pubPosition.Load(), nextPosition)
	require.Equal(t, nextPosition, merge.Image().Position())
	require.Greater(t, total-publishedAtMerge, int64(10_000), "publisher was not running fast after the merge")
	require.True(t, sawReplay || mergedAt > firstExpected)
	require.True(t, sawLive)
	require.Equal(t, int32(1), merge.Image().ActiveTransportCount())
	t.Logf("received %d messages [%d, %d) in order, %d after the merge", expected-firstExpected, firstExpected,
		expected, expected-mergedAt)

	require.NoError(t, merge.Close())
	require.True(t, merge.IsClosed())
}

// fastPubFrame returns the start and end stream positions of an unfragmented message appended at position, taking
// the padding frame the publication inserts at the end of a term into account.
func fastPubFrame(position int64, length int32) (int64, int64) {
	aligned := int64((length + logbuffer.DataFrameHeader.Length + logbuffer.FrameAlignment - 1) &^
		(logbuffer.FrameAlignment - 1))
	if termOffset := position & (fastPubTermLength - 1); termOffset+aligned > fastPubTermLength {
		position += fastPubTermLength - termOffset
	}
	return position, position + aligned
}
