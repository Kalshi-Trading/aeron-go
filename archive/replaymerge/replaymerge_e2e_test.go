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
	"testing"
	"time"

	"github.com/lirm/aeron-go/aeron"
	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/logbuffer"
	"github.com/lirm/aeron-go/aeron/logbuffer/term"
	"github.com/lirm/aeron-go/archive"
	"github.com/lirm/aeron-go/archive/replaymerge"
	"github.com/stretchr/testify/require"
)

// End-to-end ReplayMerge tests against a real Java ArchivingMediaDriver, e.g. the one in docker/:
//
//	docker compose -f docker/docker-compose.yml up -d --build archiving-driver
//	AERON_E2E=1 AERON_DIR=/dev/shm/aeron-compose go test ./archive/replaymerge/ -run E2E -v
//
// A dynamic MDC publication is recorded by the archive (remote recording via an MDC destination) while it is
// published to continuously. ReplayMerge replays the recording into a manual MDC subscription and merges it with
// the live stream. Every message carries a sequence number and must be received in order with no gaps or dupes.

const (
	e2eTermLength  = 64 * 1024
	e2eMessageLen  = 32 // 32 byte payload + 32 byte header = 64 byte frames, so term ends never need padding.
	e2eFrameLen    = 64
	e2eMsgInterval = 50 * time.Microsecond // ~20k msgs/s, ~1.3MB/s
	e2eTimeout     = 60 * time.Second
)

type e2eCase struct {
	name            string
	basePort        int
	streamID        int32
	ephemeralReplay bool
	preloadMessages int64
	midStart        bool
	controlled      bool
}

func TestReplayMergeE2E(t *testing.T) {
	if os.Getenv("AERON_E2E") != "1" {
		t.Skip("set AERON_E2E=1 to run against a Java archiving media driver")
	}
	dir := os.Getenv("AERON_DIR")
	if dir == "" {
		dir = "/dev/shm/aeron-compose"
	}

	ctx := aeron.NewContext().AeronDir(dir).MediaDriverTimeout(10 * time.Second)
	opts := archive.DefaultOptions()
	arch, err := archive.NewArchive(opts, ctx)
	require.NoError(t, err)
	defer arch.Close()

	base := int32(time.Now().Unix()%1000) * 10
	cases := []e2eCase{
		{name: "FromStartFixedPort", basePort: 24310, streamID: 41000 + base, preloadMessages: 100},
		{name: "FromStartEphemeralPort", basePort: 24320, streamID: 41001 + base, ephemeralReplay: true,
			preloadMessages: 100, controlled: true},
		{name: "LateStartEphemeralPort", basePort: 24330, streamID: 41002 + base, ephemeralReplay: true,
			preloadMessages: 50_000},
		{name: "LateStartMidPosition", basePort: 24340, streamID: 41003 + base, preloadMessages: 50_000,
			midStart: true, controlled: true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) { runReplayMergeE2E(t, arch, c) })
	}
}

func runReplayMergeE2E(t *testing.T, arch *archive.Archive, c e2eCase) {
	controlPort, livePort, replayPort, recordingPort := c.basePort, c.basePort+1, c.basePort+2, c.basePort+3

	pubChannel := fmt.Sprintf("aeron:udp?control=localhost:%d|control-mode=dynamic|term-length=%d", controlPort,
		e2eTermLength)
	pub, err := arch.AddPublication(pubChannel, c.streamID)
	require.NoError(t, err)
	defer pub.Close()
	sessionID := pub.SessionID()

	recordingChannel := fmt.Sprintf("aeron:udp?endpoint=localhost:%d|control=localhost:%d|session-id=%d",
		recordingPort, controlPort, sessionID)
	_, err = arch.StartRecording(recordingChannel, c.streamID, false, true)
	require.NoError(t, err)
	defer func() { _ = arch.StopRecording(recordingChannel, c.streamID) }()

	recordingID := awaitRecordingID(t, arch, c.streamID, sessionID)

	buffer := atomic.MakeBuffer(make([]byte, e2eMessageLen))
	var published int64
	offer := func() bool {
		buffer.PutInt64(0, published)
		if r := pub.Offer(buffer, 0, e2eMessageLen, nil); r > 0 {
			published++
			return true
		} else if r == aeron.PublicationClosed || r == aeron.MaxPositionExceeded {
			t.Fatalf("offer failed: %d", r)
		}
		return false
	}
	publishStart := time.Now()
	paced := func() int {
		due := int64(time.Since(publishStart)/e2eMsgInterval) + c.preloadMessages
		n := 0
		for published < due && n < 100 && offer() {
			n++
		}
		return n
	}

	// Pre-load the recording before the merge starts.
	deadline := time.Now().Add(e2eTimeout)
	for published < c.preloadMessages {
		if !offer() {
			require.True(t, time.Now().Before(deadline), "timed out pre-loading")
			time.Sleep(time.Millisecond)
		}
	}
	publishStart = time.Now()
	awaitRecordedPosition(t, arch, recordingID, published*e2eFrameLen)

	startPosition := int64(0)
	expected := int64(0)
	if c.midStart {
		startPosition = (published * e2eFrameLen / 2) / e2eFrameLen * e2eFrameLen
		expected = startPosition / e2eFrameLen
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

	var failure error
	onMessage := func(buf *atomic.Buffer, offset int32, length int32, _ *logbuffer.Header) {
		if failure != nil {
			return
		}
		seq := buf.GetInt64(offset)
		if length != e2eMessageLen || seq != expected {
			failure = fmt.Errorf("expected seq %d len %d, got seq %d len %d", expected, e2eMessageLen, seq, length)
			return
		}
		expected++
	}
	onControlled := func(buf *atomic.Buffer, offset int32, length int32, h *logbuffer.Header) term.ControlledPollAction {
		onMessage(buf, offset, length, h)
		return term.ControlledPollActionContinue
	}
	pollMerge := func() int {
		var n int
		var err error
		if c.controlled {
			n, err = merge.ControlledPoll(onControlled, 20)
		} else {
			n, err = merge.Poll(onMessage, 20)
		}
		require.NoError(t, err, merge.String())
		require.NoError(t, failure)
		return n
	}

	sawCatchup, sawLiveAdded := false, false
	for !merge.IsMerged() {
		require.True(t, time.Now().Before(deadline), "timed out merging: %s, received=%d published=%d",
			merge.String(), expected, published)
		work := paced() + pollMerge()
		sawCatchup = sawCatchup || merge.State() == replaymerge.StateCatchup
		sawLiveAdded = sawLiveAdded || merge.IsLiveAdded()
		if work == 0 {
			time.Sleep(100 * time.Microsecond)
		}
	}
	require.True(t, sawCatchup)
	require.True(t, sawLiveAdded)
	require.False(t, merge.HasFailed())
	require.NotNil(t, merge.Image())
	if c.ephemeralReplay {
		uri, err := aeron.ParseChannelUri(merge.ReplayChannel())
		require.NoError(t, err)
		require.NotEqual(t, "localhost:0", uri.Get(aeron.EndpointParamName))
	}
	mergedAt := expected
	t.Logf("merged: %s, received=%d published=%d", merge.String(), expected, published)

	// Keep publishing live after the merge, then drain everything.
	target := published + 5_000
	for published < target || expected < published {
		require.True(t, time.Now().Before(deadline), "timed out after merge: received=%d published=%d",
			expected, published)
		work := 0
		if published < target {
			work += paced()
		}
		work += pollMerge()
		if work == 0 {
			time.Sleep(100 * time.Microsecond)
		}
	}
	require.NoError(t, failure)
	require.Equal(t, published, expected)
	require.Greater(t, expected, mergedAt)
	require.True(t, merge.IsMerged())
	t.Logf("received %d messages [%d, %d) with no gaps or duplicates", expected-firstExpected, firstExpected, expected)

	require.NoError(t, merge.Close())
	require.True(t, merge.IsClosed())
}

func awaitRecordingID(t *testing.T, arch *archive.Archive, streamID int32, sessionID int32) int64 {
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		descriptors, err := arch.ListRecordingsForUri(0, 1000, "", streamID)
		require.NoError(t, err)
		for _, d := range descriptors {
			if d.SessionId == sessionID {
				return d.RecordingId
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("recording for stream %d session %d not found", streamID, sessionID)
	return 0
}

func awaitRecordedPosition(t *testing.T, arch *archive.Archive, recordingID int64, position int64) {
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		p, err := arch.GetRecordingPosition(recordingID)
		require.NoError(t, err)
		if p >= position {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("recording %d did not reach position %d", recordingID, position)
}
