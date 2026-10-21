package archivecrash

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/lirm/aeron-go/aeron"
	"github.com/lirm/aeron-go/aeron/counters"
	"github.com/lirm/aeron-go/archive"
	"github.com/lirm/aeron-go/archive/codecs"
	"github.com/lirm/aeron-go/e2e"
	"github.com/stretchr/testify/require"
)

// signalLog collects recording signals and async errors of a destination archive client.
type signalLog struct {
	mu      sync.Mutex
	signals []*codecs.RecordingSignalEvent
	errs    []error
}

func (l *signalLog) find(s codecs.RecordingSignalEnum) *codecs.RecordingSignalEvent {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, e := range l.signals {
		if e.Signal == s {
			return e
		}
	}
	return nil
}

func (l *signalLog) reset() { l.mu.Lock(); l.signals, l.errs = nil, nil; l.mu.Unlock() }

func connectDst(t *testing.T, log *signalLog) *archive.Archive {
	t.Helper()
	o := archiveOptions(controlB)
	o.RecordingSignalConsumer = func(e *codecs.RecordingSignalEvent) {
		c := *e
		log.mu.Lock()
		log.signals = append(log.signals, &c)
		log.mu.Unlock()
	}
	o.ErrorHandler = func(err error) { log.mu.Lock(); log.errs = append(log.errs, err); log.mu.Unlock() }
	var arch *archive.Archive
	var err error
	deadline := time.Now().Add(30 * time.Second)
	for {
		if arch, err = archive.NewArchive(o, newContext(nil)); err == nil || time.Now().After(deadline) {
			break
		}
		time.Sleep(300 * time.Millisecond)
	}
	require.NoError(t, err)
	t.Cleanup(func() { _ = arch.Close() })
	return arch
}

func awaitSignal(t *testing.T, arch *archive.Archive, log *signalLog, s codecs.RecordingSignalEnum,
	timeout time.Duration) *codecs.RecordingSignalEvent {
	t.Helper()
	var e *codecs.RecordingSignalEvent
	awaitTrue(t, fmt.Sprint("signal ", s), timeout, func() bool {
		_, err := arch.PollForErrorResponse()
		if err != nil {
			log.mu.Lock()
			log.errs = append(log.errs, err)
			log.mu.Unlock()
		}
		e = log.find(s)
		return e != nil
	})
	return e
}

// TestArchiveKillDuringReplication replicates an active recording from archive a to archive b (follow-live) and
// SIGKILLs the source or the destination archive mid-replication. Source kill: the destination must end the
// replication (REPLICATE_END or a replication error, as Java's ReplicationSession does) and stop its copy at a
// valid position. Destination kill: the source must clean up the replication replay session. After the restart,
// replicating into the existing destination recording must resume it to the source stop position, and the copy
// must replay gap-free and byte-identical to what was published.
func TestArchiveKillDuringReplication(t *testing.T) {
	for i, victim := range []string{archiveA, archiveB} {
		victim, stream := victim, int32(9360+i)
		name := map[string]string{archiveA: "kill-source", archiveB: "kill-destination"}[victim]
		t.Run(name, func(t *testing.T) { killDuringReplication(t, victim, stream) })
	}
}

func killDuringReplication(t *testing.T, victim string, stream int32) {
	freshStack(t)
	base := snapshot(t)
	channel := "aeron:udp?endpoint=localhost:27385|term-length=65536"
	src := connectArchive(t, controlA, nil)
	_, err := src.StartRecording(channel, stream, false, true)
	require.NoError(t, err)
	a := connect(t, nil)
	pub, err := a.AddExclusivePublication(channel, stream)
	require.NoError(t, err)
	r := runPublisher(pub, 0, -1, 400, goFormat, false, 200*time.Microsecond)
	srcDesc, err := e2e.WaitForRecording(src, "27385", stream, 10*time.Second, nil)
	require.NoError(t, err)

	log := &signalLog{}
	dst := connectDst(t, log)
	replicationID, err := dst.Replicate(srcDesc.RecordingId, aeron.NullValue, controlStream, controlA, "")
	require.NoError(t, err)
	rep := awaitSignal(t, dst, log, codecs.RecordingSignal.REPLICATE, 10*time.Second)
	dstID := rep.RecordingId
	require.NoError(t, e2e.WaitRecordingPosition(dst, dstID, 256*1024, 20*time.Second))

	kill(t, victim)
	var published map[int64]int64
	if victim == archiveA {
		// The destination archive notices the source is gone and ends the replication.
		awaitTrue(t, "replication ended by the destination", 20*time.Second, func() bool {
			_, _ = dst.PollForErrorResponse()
			log.mu.Lock()
			defer log.mu.Unlock()
			if len(log.errs) > 0 {
				return true
			}
			for _, e := range log.signals {
				if e.Signal == codecs.RecordingSignal.REPLICATE_END {
					return true
				}
			}
			return false
		})
		log.mu.Lock()
		for _, err := range log.errs {
			t.Logf("destination error: %v", err)
			var ae *archive.ArchiveException
			if errors.As(err, &ae) {
				require.Contains(t, []archive.ArchiveErrorCode{archive.ErrorCodeGeneric,
					archive.ErrorCodeReplicationConnectionFailure}, ae.ErrorCode)
			}
		}
		log.mu.Unlock()
		_, err = e2e.WaitRecordingStopped(dst, dstID, 20*time.Second)
		require.NoError(t, err)
		ok, err := dst.TryStopReplication(replicationID)
		require.NoError(t, err)
		require.False(t, ok, "replication still active after the source died")
		requireBounded(t, "stale source Close", msgTimeout+slack, func() error { return src.Close() })
		published = r.halt()
		require.NoError(t, pub.Close())
		start(t, archiveA)
		src = connectArchive(t, controlA, nil)
	} else {
		awaitTrue(t, "source cleaned up the replication replay", 20*time.Second, func() bool {
			return archiveCounter(a, counters.ArchiveReplaySessionCountTypeId) == 0 &&
				archiveCounter(a, counters.ArchiveControlSessionsTypeId) == 1
		})
		requireBounded(t, "stale destination Close", msgTimeout+slack, func() error { return dst.Close() })
		published = r.halt()
		require.NoError(t, pub.Close())
		start(t, archiveB)
		dst = connectDst(t, log)
	}
	srcStop, err := e2e.WaitRecordingStopped(src, srcDesc.RecordingId, 20*time.Second)
	require.NoError(t, err)
	dd, err := dst.ListRecording(dstID)
	require.NoError(t, err)
	require.NotEqual(t, int64(aeron.NullValue), dd.StopPosition)
	require.LessOrEqual(t, dd.StopPosition, srcStop)
	t.Logf("source stop %d, partial copy stop %d", srcStop, dd.StopPosition)

	// Resume into the same destination recording.
	log.reset()
	params := archive.NewReplicationParams()
	params.DstRecordingID = dstID
	_, err = dst.ReplicateWithParams(srcDesc.RecordingId, controlStream, controlA, params)
	require.NoError(t, err)
	awaitSignal(t, dst, log, codecs.RecordingSignal.REPLICATE_END, 30*time.Second)
	dd, err = dst.ListRecording(dstID)
	require.NoError(t, err)
	require.Equal(t, srcStop, dd.StopPosition)

	v := &verifier{format: goFormat, size: 400, endPos: published}
	replayVerify(t, dst, dstID, dd.StartPosition, srcStop, stream, v)
	v2 := &verifier{format: goFormat, size: 400, endPos: published}
	replayVerify(t, src, srcDesc.RecordingId, srcDesc.StartPosition, srcStop, stream, v2)
	require.Equal(t, v2.next, v.next, "copy differs from the source")

	require.NoError(t, dst.Close())
	require.NoError(t, src.Close())
	require.NoError(t, a.Close())
	requireNoStreamCounters(t, stream)
	requireNoLeaks(t, base)
}
