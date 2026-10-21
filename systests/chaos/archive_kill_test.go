package chaos

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/lirm/aeron-go/aeron"
	ab "github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/archive"
	"github.com/lirm/aeron-go/archive/codecs"
	"github.com/lirm/aeron-go/e2e"
	"github.com/stretchr/testify/require"
)

func bySession(id int32) func(*codecs.RecordingDescriptor) bool {
	return func(d *codecs.RecordingDescriptor) bool { return d.SessionId == id }
}

func timed(t *testing.T, what string, limit time.Duration, f func() error) error {
	t.Helper()
	start := time.Now()
	err := f()
	t.Logf("%s after %v: %v", what, time.Since(start), err)
	require.Less(t, time.Since(start), limit, "%s hung", what)
	return err
}

// TestChaosArchiveKillMidRecording kills the archive while it records and replays a live stream. Archive calls on the
// dead session must fail within the message timeout (not hang), the replay image must go away, the client must
// keep running, and after the archive restarts the recording must be listed with a stop position and replay without
// gaps.
func TestChaosArchiveKillMidRecording(t *testing.T) {
	requireChaos(t)
	restartStack(t)
	a := connect(t, chaosDir)
	arch := connectArchive(t, chaosDir, chaosControlChannel)

	const channel = "aeron:udp?endpoint=localhost:24510|term-length=65536"
	stream := stream(5400)
	pub, err := a.AddExclusivePublication(channel, stream)
	require.NoError(t, err)
	subID, err := arch.StartRecording(channel, stream, false, false)
	require.NoError(t, err)
	t.Logf("recording subscription %d", subID)

	var sent atomic.Int64
	stopPub := make(chan struct{})
	pubDone := make(chan struct{})
	go func() {
		defer close(pubDone)
		buf := make([]byte, 256)
		abuf := ab.MakeBuffer(buf)
		for seq := int64(0); ; {
			select {
			case <-stopPub:
				return
			default:
			}
			e2e.EncodeMessage(buf, seq)
			if r := pub.Offer(abuf, 0, int32(len(buf)), nil); r > 0 {
				seq++
				sent.Store(seq)
				time.Sleep(50 * time.Microsecond)
			} else if r == aeron.PublicationClosed {
				return
			}
		}
	}()
	defer func() { close(stopPub); <-pubDone }()

	rec, err := e2e.WaitForRecording(arch, "endpoint=localhost:24510", stream, 10*time.Second, bySession(pub.SessionID()))
	require.NoError(t, err)
	recordingID := rec.RecordingId
	require.NoError(t, err)
	require.NoError(t, e2e.WaitRecordingPosition(arch, recordingID, 64*1024, 10*time.Second))

	replaySub, _, err := e2e.Replay(arch, recordingID, 0, archive.RecordingPositionNull,
		"aeron:udp?endpoint=localhost:24511", 5401)
	require.NoError(t, err)
	checker := e2e.NewSequenceChecker(0)
	require.NoError(t, e2e.Consume(e2e.SubscriptionPoller(replaySub), checker, 500, 10*time.Second))

	docker(t, "kill", chaosArchive)
	t.Cleanup(func() { restartStack(t) })

	// Replay image goes unavailable (image liveness 3s); polling stays safe and the data seen has no gaps.
	awaitTrue(t, "replay image unavailable", 15*time.Second, func() bool {
		replaySub.Poll(checker.Handler(), 64)
		return !replaySub.IsConnected()
	})
	require.NoError(t, checker.Err())

	_ = timed(t, "GetRecordingPosition on dead archive", 10*time.Second, func() error {
		_, err := arch.GetRecordingPosition(recordingID)
		return err
	})
	require.False(t, a.IsClosed(), "client must survive the archive dying")

	restartStack(t)
	arch2, err := connectArchiveErr(chaosDir, chaosControlChannel, 60*time.Second)
	require.NoError(t, err)
	defer arch2.Close()

	// The old control session is gone: requests must fail rather than hang, and Close must return.
	err = timed(t, "ListRecording on stale session", 10*time.Second, func() error {
		_, err := arch.ListRecording(recordingID)
		return err
	})
	require.Error(t, err)
	_ = timed(t, "Close stale archive", 10*time.Second, arch.Close)

	desc, err := arch2.ListRecording(recordingID)
	require.NoError(t, err)
	require.NotNil(t, desc)
	t.Logf("recovered recording: start=%d stop=%d", desc.StartPosition, desc.StopPosition)
	require.NotEqual(t, aeron.NullValue, desc.StopPosition)
	require.Positive(t, desc.StopPosition)

	replaySub2, _, err := e2e.Replay(arch2, recordingID, 0, desc.StopPosition, "aeron:udp?endpoint=localhost:24512", 5402)
	require.NoError(t, err)
	defer replaySub2.Close()
	checker2 := e2e.NewSequenceChecker(0)
	awaitTrue(t, "replay of recovered recording", 30*time.Second, func() bool {
		replaySub2.Poll(checker2.Handler(), 256)
		images := replaySub2.Images()
		return checker2.Received > 0 && (len(images) == 0 || images[0].IsEndOfStream() || images[0].Position() >= desc.StopPosition)
	})
	require.NoError(t, checker2.Err())
	t.Logf("replayed %s", checker2)
	require.Greater(t, checker2.Next, int64(500))
}

// TestChaosArchiveKillDuringReplayMerge kills the archive while a ReplayMerge is still replaying. Poll must return an
// error once the merge stops making progress (Java throws TimeoutException), not spin forever, and Close must return.
func TestChaosArchiveKillDuringReplayMerge(t *testing.T) {
	requireChaos(t)
	restartStack(t)
	a := connect(t, chaosDir)
	arch := connectArchive(t, chaosDir, chaosControlChannel)

	const channel = "aeron:udp?control=localhost:24520|control-mode=dynamic|term-length=65536"
	stream := stream(5410)
	_, err := arch.StartRecording("aeron:udp?endpoint=localhost:24521|control=localhost:24520", stream, false, false)
	require.NoError(t, err)
	pub, err := a.AddExclusivePublication(channel, stream)
	require.NoError(t, err)
	require.NoError(t, e2e.WaitConnected(pub, 10*time.Second))
	rec, err := e2e.WaitForRecording(arch, "control=localhost:24520", stream, 10*time.Second, bySession(pub.SessionID()))
	require.NoError(t, err)
	recordingID := rec.RecordingId
	require.NoError(t, err)
	require.NoError(t, e2e.Publish(pub, e2e.PublishOptions{Count: 20000, Size: 512, Timeout: 20 * time.Second}))
	require.NoError(t, e2e.WaitRecordingPosition(arch, recordingID, pub.Position(), 10*time.Second))
	desc, err := arch.ListRecording(recordingID)
	require.NoError(t, err)

	merge, sub, err := e2e.StartReplayMerge(arch, e2e.ReplayMergeOptions{
		Recording:         desc,
		StartPosition:     0,
		ReplayDestination: "aeron:udp?endpoint=localhost:0",
		LiveDestination:   "aeron:udp?endpoint=localhost:24522|control=localhost:24520",
		ProgressTimeoutMs: 3000,
	})
	require.NoError(t, err)
	defer sub.Close()
	checker := e2e.NewSequenceChecker(0)
	poll := e2e.MergePoller(merge)
	// Consume part of the backlog slowly so the merge is still replaying.
	require.NoError(t, e2e.Consume(poll, checker, 100, 10*time.Second))
	require.False(t, merge.IsMerged())

	docker(t, "kill", chaosArchive)
	t.Cleanup(func() { restartStack(t) })

	var pollErr error
	start := time.Now()
	for pollErr == nil && time.Since(start) < 30*time.Second && !merge.IsMerged() {
		_, pollErr = poll(checker.Handler(), 1)
		time.Sleep(time.Millisecond)
	}
	t.Logf("merge after kill: err=%v merged=%v state=%v after %v; %s", pollErr, merge.IsMerged(), merge.State(),
		time.Since(start), checker)
	require.NoError(t, checker.Err())
	if !merge.IsMerged() {
		require.Error(t, pollErr, "replay merge must fail instead of spinning after the archive dies")
		require.True(t, merge.HasFailed())
	}
	_ = timed(t, "ReplayMerge.Close", 10*time.Second, merge.Close)
}
