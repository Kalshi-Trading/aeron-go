package e2e

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/lirm/aeron-go/aeron"
	"github.com/lirm/aeron-go/archive"
	"github.com/lirm/aeron-go/archive/codecs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// These tests need the Java 1.53.3 archiving driver from docker/docker-compose.yml:
//
//	make e2e   (or: docker compose -f docker/docker-compose.yml up -d --wait archiving-driver &&
//	            AERON_E2E=1 AERON_DIR=/dev/shm/aeron-compose go test ./e2e/)

func requireE2E(t *testing.T) {
	t.Helper()
	if !Enabled() {
		t.Skipf("set %s=1 and start docker/docker-compose.yml archiving-driver", Env)
	}
}

func connect(t *testing.T) *aeron.Aeron {
	t.Helper()
	a, err := Connect(AeronDir())
	require.NoError(t, err)
	t.Cleanup(func() { _ = a.Close() })
	return a
}

func connectArchive(t *testing.T) *archive.Archive {
	t.Helper()
	arch, err := ConnectArchive(AeronDir(), DefaultControlChannel, DefaultControlStream)
	require.NoError(t, err)
	t.Cleanup(func() { _ = arch.Close() })
	return arch
}

func TestE2ELivePubSub(t *testing.T) {
	requireE2E(t)
	for _, tc := range []struct {
		name    string
		channel string
		stream  int32
	}{
		{"udp", "aeron:udp?endpoint=localhost:21121", 4001},
		{"ipc", "aeron:ipc", 4002},
		{"udp-fragmented", "aeron:udp?endpoint=localhost:21124|mtu=1408", 4007},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a := connect(t)
			sub, err := a.AddSubscription(tc.channel, tc.stream)
			require.NoError(t, err)
			defer sub.Close()
			pub, err := a.AddPublication(tc.channel, tc.stream)
			require.NoError(t, err)
			defer pub.Close()
			require.NoError(t, WaitConnected(pub, 10*time.Second))

			const count = 20000
			size := 256
			poll := SubscriptionPoller(sub)
			if tc.name == "udp-fragmented" {
				size = 5000
				poll = ReassemblingPoller(sub)
			}
			errs := make(chan error, 1)
			go func() { errs <- Publish(pub, PublishOptions{Count: count, Size: size}) }()
			checker := NewSequenceChecker(0)
			require.NoError(t, Consume(poll, checker, count, 30*time.Second))
			require.NoError(t, <-errs)
			assert.Equal(t, int64(count), checker.Received)
		})
	}
}

func TestE2ERecordReplay(t *testing.T) {
	requireE2E(t)
	const (
		channel       = "aeron:udp?endpoint=localhost:21122"
		stream        = int32(4003)
		replayChannel = "aeron:udp?endpoint=localhost:21123"
		replayStream  = int32(4004)
		count         = 5000
	)
	a := connect(t)
	arch := connectArchive(t)

	subscriptionID, err := arch.StartRecording(channel, stream, false, true)
	require.NoError(t, err)
	t.Logf("recording subscription %d", subscriptionID)

	pub, err := a.AddPublication(channel, stream)
	require.NoError(t, err)
	require.NoError(t, WaitConnected(pub, 10*time.Second))
	require.NoError(t, Publish(pub, PublishOptions{Count: count, Size: 200}))

	rec, err := WaitForRecording(arch, channel, stream, 10*time.Second, func(d *codecs.RecordingDescriptor) bool {
		return d.SessionId == pub.SessionID()
	})
	require.NoError(t, err)
	end := pub.Position()
	require.NoError(t, WaitRecordingPosition(arch, rec.RecordingId, end, 10*time.Second))
	require.NoError(t, pub.Close())
	stop, err := WaitRecordingStopped(arch, rec.RecordingId, 20*time.Second)
	require.NoError(t, err)
	assert.Equal(t, end, stop)

	t.Run("full", func(t *testing.T) {
		sub, replaySession, err := Replay(arch, rec.RecordingId, rec.StartPosition, archive.RecordingPositionNull, replayChannel, replayStream)
		require.NoError(t, err)
		defer sub.Close()
		t.Logf("replay session %d of recording %d [%d, %d)", replaySession, rec.RecordingId, rec.StartPosition, stop)
		checker := NewSequenceChecker(0)
		require.NoError(t, Consume(SubscriptionPoller(sub), checker, count, 20*time.Second))
		assert.Equal(t, int64(count), checker.Received)
	})

	t.Run("bounded-from-middle", func(t *testing.T) {
		// Messages are 200 bytes + 32 byte header = 232 aligned to 256 per frame.
		const frame, from, n = 256, 1000, 500
		sub, _, err := Replay(arch, rec.RecordingId, rec.StartPosition+from*frame, n*frame, replayChannel, replayStream+1)
		require.NoError(t, err)
		defer sub.Close()
		checker := NewSequenceChecker(from)
		require.NoError(t, Consume(SubscriptionPoller(sub), checker, from+n, 20*time.Second))
		// Bounded replay must end at the requested length.
		time.Sleep(200 * time.Millisecond)
		sub.Poll(checker.Handler(), 100)
		assert.Equal(t, int64(n), checker.Received)
		assert.NoError(t, checker.Err())
	})
}

func TestE2EReplayMerge(t *testing.T) {
	requireE2E(t)
	const (
		control          = "localhost:21131"
		pubChannel       = "aeron:udp?control=" + control + "|control-mode=dynamic"
		recordingChannel = "aeron:udp?endpoint=localhost:21132|control=" + control
		liveDestination  = "aeron:udp?endpoint=localhost:21133|control=" + control
		replayDest       = "aeron:udp?endpoint=localhost:0"
		stream           = int32(4005)
		count            = 8000
		mergeAfter       = 2000
	)
	a := connect(t)
	arch := connectArchive(t)

	_, err := arch.StartRecording(recordingChannel, stream, false, true)
	require.NoError(t, err)
	pub, err := a.AddPublication(pubChannel, stream)
	require.NoError(t, err)
	defer pub.Close()
	require.NoError(t, WaitConnected(pub, 10*time.Second))

	var sent atomic.Int64
	errs := make(chan error, 1)
	go func() {
		errs <- Publish(pub, PublishOptions{Count: count, Size: 128, Interval: 100 * time.Microsecond,
			OnSent: func(seq int64) { sent.Store(seq + 1) }})
	}()
	for sent.Load() < mergeAfter {
		time.Sleep(time.Millisecond)
	}

	rec, err := WaitForRecording(arch, recordingChannel, stream, 10*time.Second, func(d *codecs.RecordingDescriptor) bool {
		return d.SessionId == pub.SessionID()
	})
	require.NoError(t, err)

	merge, sub, err := StartReplayMerge(arch, ReplayMergeOptions{
		Recording: rec, StartPosition: rec.StartPosition, ReplayDestination: replayDest, LiveDestination: liveDestination,
	})
	require.NoError(t, err)
	defer sub.Close()
	defer merge.Close()

	checker := NewSequenceChecker(0)
	require.NoError(t, Consume(MergePoller(merge), checker, count, 60*time.Second))
	require.NoError(t, <-errs)
	assert.True(t, merge.IsMerged(), "replay merge did not merge with the live stream (state %v)", merge)
	assert.Equal(t, int64(count), checker.Received)
}
