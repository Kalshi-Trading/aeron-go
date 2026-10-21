package archivecrash

import (
	"errors"
	"fmt"
	"strings"
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

// archiveCounter returns the value of the archive-a counter of typeID (control sessions, replay session count).
func archiveCounter(a *aeron.Aeron, typeID int32) int64 {
	v := int64(-1)
	a.CounterReader().Scan(func(c counters.Counter) {
		if c.TypeId == typeID && strings.Contains(c.Label, "archiveId=9300") {
			v = c.Value
		}
	})
	return v
}

// TestGoClientSIGKILLHoldingReplay SIGKILLs a separate Go client process while it holds a follow-live replay of an
// active recording. Like Java, the archive must close the orphaned control session and replay session and the
// driver must free the replay publication and the dead client, while the recording carries on unaffected and a new
// client can replay it.
func TestGoClientSIGKILLHoldingReplay(t *testing.T) {
	freshStack(t)
	const stream = 9350
	channel := "aeron:udp?endpoint=localhost:27375"
	arch := connectArchive(t, controlA, nil)
	_, err := arch.StartRecording(channel, stream, false, true)
	require.NoError(t, err)
	a := connect(t, nil)
	pub, err := a.AddExclusivePublication(channel, stream)
	require.NoError(t, err)
	r := runPublisher(pub, 0, -1, 200, goFormat, false, time.Millisecond)
	desc, err := e2e.WaitForRecording(arch, "27375", stream, 10*time.Second, nil)
	require.NoError(t, err)

	replayPubs := func() int { return countCounters(a, 2, "adv-child-replay") + countCounters(a, 1, "adv-child-replay") }
	for round := 0; round < 2; round++ {
		c := startChild(t, "replay", "ADV_CONTROL="+controlA, fmt.Sprintf("ADV_RECORDING=%d", desc.RecordingId),
			fmt.Sprintf("ADV_STREAM=%d", stream+1))
		kv := c.awaitReady()
		awaitTrue(t, "child replay session active", 10*time.Second, func() bool {
			return archiveCounter(a, counters.ArchiveReplaySessionCountTypeId) == 1 && replayPubs() > 0
		})
		require.Equal(t, int64(2), archiveCounter(a, counters.ArchiveControlSessionsTypeId))
		require.NotEqual(t, aeron.NullValue, counters.FindHeartbeatCounterIDByRegistrationID(a.CounterReader(),
			counters.DriverHeartbeatTypeId, kv["clientId"]))

		c.kill()
		awaitTrue(t, "archive cleaned up the dead client's replay and control session", 20*time.Second, func() bool {
			return archiveCounter(a, counters.ArchiveReplaySessionCountTypeId) == 0 &&
				archiveCounter(a, counters.ArchiveControlSessionsTypeId) == 1 && replayPubs() == 0 &&
				counters.FindHeartbeatCounterIDByRegistrationID(a.CounterReader(), counters.DriverHeartbeatTypeId,
					kv["clientId"]) == aeron.NullValue
		})
		// Java's ArchiveConductor.stopReplay answers OK for an unknown replay session; Go must not hang or error.
		require.NoError(t, requireBounded(t, "StopReplay of the cleaned up session", msgTimeout+slack, func() error {
			return arch.StopReplay(kv["replaySession"])
		}))
	}
	// The recording was unaffected and replays everything published so far. The publisher is halted first:
	// ExclusivePublication.Position is single-threaded, like Java's.
	positions := r.halt()
	before := pub.Position()
	require.NoError(t, e2e.WaitRecordingPosition(arch, desc.RecordingId, before, 10*time.Second))
	v := &verifier{format: goFormat, size: 200, endPos: positions}
	replayVerify(t, arch, desc.RecordingId, desc.StartPosition, before, stream, v)
}

// TestPublisherProcessSIGKILLDuringRecording SIGKILLs the publishing process (a Go child process or a Java
// interop container) mid-recording. The image times out, the auto-stop recording stops at the last received frame,
// and everything up to its stop position replays gap-free and byte-identically.
func TestPublisherProcessSIGKILLDuringRecording(t *testing.T) {
	for i, java := range []bool{false, true} {
		java, stream := java, int32(9352+i)
		t.Run(map[bool]string{false: "go-child", true: "java"}[java], func(t *testing.T) {
			k := pubKind{java: java}
			k.skipIfUnsupported(t)
			freshStack(t)
			channel := fmt.Sprintf("aeron:udp?endpoint=localhost:%d|term-length=65536", 27376+i)
			arch := connectArchive(t, controlA, nil)
			_, err := arch.StartRecording(channel, stream, false, true)
			require.NoError(t, err)
			var killPub func()
			f := goFormat
			if java {
				j := startJavaPub(t, channel, stream, 300)
				killPub, f = j.kill, javaFormat
			} else {
				c := startChild(t, "publish", "ADV_CHANNEL="+channel, fmt.Sprintf("ADV_STREAM=%d", stream), "ADV_SIZE=300")
				c.awaitReady()
				killPub = c.kill
			}
			desc, err := e2e.WaitForRecording(arch, fmt.Sprint(27376+i), stream, 20*time.Second, nil)
			require.NoError(t, err)
			require.NoError(t, e2e.WaitRecordingPosition(arch, desc.RecordingId, 128*1024, 20*time.Second))
			killPub()
			stop, err := e2e.WaitRecordingStopped(arch, desc.RecordingId, 20*time.Second)
			require.NoError(t, err)
			require.GreaterOrEqual(t, stop, int64(128*1024))
			v := &verifier{format: f, size: 300}
			replayVerify(t, arch, desc.RecordingId, desc.StartPosition, stop, stream, v)
			require.Equal(t, stop, v.lastPos, "stop position is not the end of the last recorded message")
		})
	}
}

// TestRecordingEventsAndSignalsSurviveArchiveRestart keeps one RecordingEventsAdapter on one events subscription
// across an archive SIGKILL and restart: it must see the old recording start, nothing broken while the archive is
// gone, and the new archive's start/progress/stop events without being recreated. The archive client's
// RecordingSignal consumer fails cleanly on the dead archive and a reconnected client gets START/EXTEND/STOP.
func TestRecordingEventsAndSignalsSurviveArchiveRestart(t *testing.T) {
	freshStack(t)
	base := snapshot(t)
	const stream = 9355
	channel := "aeron:udp?endpoint=localhost:27378"
	a := connect(t, nil)
	var mu sync.Mutex
	starts, stops := map[int64]int{}, map[int64]int64{}
	progress := map[int64]int64{}
	listener := archive.RecordingEventsListenerFuncs{
		Start: func(id, _ int64, _, s int32, _, _ string) {
			if s == stream {
				mu.Lock()
				starts[id]++
				mu.Unlock()
			}
		},
		Progress: func(id, _, pos int64) { mu.Lock(); progress[id] = pos; mu.Unlock() },
		Stop:     func(id, _, pos int64) { mu.Lock(); stops[id] = pos; mu.Unlock() },
	}
	ev := &imageEvents{}
	eventsSub, err := a.AddSubscriptionWithHandlers(eventsA, eventsStream, ev.onAvailable, ev.onUnavailable)
	require.NoError(t, err)
	adapter := archive.NewRecordingEventsAdapter(listener, eventsSub, 10)
	pollEvents := func(cond func() bool) bool {
		_, err := adapter.Poll()
		require.NoError(t, err)
		mu.Lock()
		defer mu.Unlock()
		return cond()
	}
	awaitTrue(t, "events image", 10*time.Second, func() bool { return pollEvents(func() bool { return true }) && eventsSub.IsConnected() })

	var sigMu sync.Mutex
	var signals []codecs.RecordingSignalEnum
	newArch := func() *archive.Archive {
		o := archiveOptions(controlA)
		o.RecordingSignalConsumer = func(e *codecs.RecordingSignalEvent) {
			sigMu.Lock()
			signals = append(signals, e.Signal)
			sigMu.Unlock()
		}
		arch, err := archive.NewArchive(o, newContext(nil))
		require.NoError(t, err)
		t.Cleanup(func() { _ = arch.Close() })
		return arch
	}
	hasSignal := func(arch *archive.Archive, s codecs.RecordingSignalEnum) func() bool {
		return func() bool {
			_, _ = arch.PollForErrorResponse()
			sigMu.Lock()
			defer sigMu.Unlock()
			for _, x := range signals {
				if x == s {
					return true
				}
			}
			return false
		}
	}

	arch := newArch()
	_, err = arch.StartRecording(channel, stream, false, true)
	require.NoError(t, err)
	pub, err := a.AddExclusivePublication(channel, stream)
	require.NoError(t, err)
	r := runPublisher(pub, 0, -1, 200, goFormat, false, 200*time.Microsecond)
	awaitTrue(t, "START signal", 10*time.Second, hasSignal(arch, codecs.RecordingSignal.START))
	awaitTrue(t, "recording 0 started+progress events", 10*time.Second, func() bool {
		return pollEvents(func() bool { return starts[0] == 1 && progress[0] > 0 })
	})

	kill(t, archiveA)
	_, _, err = requireBounded3(t, "signal poll on killed archive", msgTimeout+slack, func() (int, int, error) {
		deadline := time.Now().Add(msgTimeout)
		for time.Now().Before(deadline) {
			if _, err := arch.PollForErrorResponse(); err != nil {
				return 0, 0, err
			}
			time.Sleep(10 * time.Millisecond)
		}
		return 0, 0, nil
	})
	t.Logf("signal poll on dead archive: %v", err)
	awaitTrue(t, "events image unavailable", 15*time.Second, func() bool {
		pollEvents(func() bool { return true })
		return ev.unavailable.Load() >= 1
	})
	published := r.halt()
	require.NoError(t, pub.Close())
	require.NoError(t, arch.Close())
	start(t, archiveA)

	// Same adapter, same subscription: events of the restarted archive arrive.
	arch2 := newArch()
	d, err := arch2.ListRecording(0)
	require.NoError(t, err)
	_, err = arch2.ExtendRecording(0, stream, codecs.SourceLocation.REMOTE, true, extendChannel)
	require.NoError(t, err)
	uri, err := aeron.ParseChannelUri(extendChannel)
	require.NoError(t, err)
	require.NoError(t, uri.InitialPosition(d.StopPosition, d.InitialTermId, d.TermBufferLength))
	pub2, err := a.AddExclusivePublication(uri.String(), stream)
	require.NoError(t, err)
	awaitTrue(t, "EXTEND signal", 10*time.Second, hasSignal(arch2, codecs.RecordingSignal.EXTEND))
	awaitTrue(t, "events after restart", 15*time.Second, func() bool {
		return pollEvents(func() bool { return starts[0] == 2 })
	})
	r2 := runPublisher(pub2, 0, 100, 200, goFormat, false, 0)
	<-r2.done
	end := pub2.Position()
	require.NoError(t, pub2.Close())
	awaitTrue(t, "STOP signal", 15*time.Second, hasSignal(arch2, codecs.RecordingSignal.STOP))
	awaitTrue(t, "stop event at the extended position", 15*time.Second, func() bool {
		return pollEvents(func() bool { return stops[0] == end })
	})
	require.Greater(t, len(published), 0)
	require.NoError(t, eventsSub.Close())
	require.NoError(t, arch2.Close())
	require.NoError(t, a.Close())
	requireNoLeaks(t, base)
}

// TestArchiveStorageFullThenRecovery fills the small archive (8MiB tmpfs, 2MiB low-storage threshold). The
// recording must stop at a valid position, a new StartRecording must be rejected with STORAGE_SPACE (code 11) as
// in Java, the partial recording must replay intact, and after PurgeRecording frees space recording works again.
func TestArchiveStorageFullThenRecovery(t *testing.T) {
	freshStack(t)
	const stream = 9356
	channel := "aeron:udp?endpoint=localhost:27379|term-length=65536"
	arch := connectArchive(t, controlSml, nil)
	_, err := arch.StartRecording(channel, stream, false, true)
	require.NoError(t, err)
	a := connect(t, nil)
	pub, err := a.AddExclusivePublication(channel, stream)
	require.NoError(t, err)
	r := runPublisher(pub, 0, -1, 1000, goFormat, false, 0)
	desc, err := e2e.WaitForRecording(arch, "27379", stream, 10*time.Second, nil)
	require.NoError(t, err)
	stop, err := e2e.WaitRecordingStopped(arch, desc.RecordingId, 60*time.Second)
	require.NoError(t, err, "recording did not stop when the archive ran out of space")
	positions := r.halt()
	require.NoError(t, pub.Close())
	t.Logf("storage full: recording %d stopped at %d", desc.RecordingId, stop)
	require.Greater(t, stop, int64(1<<20))

	_, err = arch.StartRecording("aeron:udp?endpoint=localhost:27380", stream+1, false, true)
	var ae *archive.ArchiveException
	require.True(t, errors.As(err, &ae), "StartRecording on a full archive: %T %v", err, err)
	require.Equal(t, archive.ErrorCodeStorageSpace, ae.ErrorCode, "%v", err)
	require.ErrorIs(t, err, archive.NewArchiveException("", archive.ErrorCodeStorageSpace, aeron.NullValue))

	v := &verifier{format: goFormat, size: 1000, endPos: positions}
	replayVerify(t, arch, desc.RecordingId, desc.StartPosition, stop, stream, v)
	require.Equal(t, stop, v.lastPos)

	err = arch.PurgeRecording(desc.RecordingId)
	require.NoError(t, err)
	recordAndVerify(t, arch, "aeron:udp?endpoint=localhost:27381", stream+2, 500)
}
