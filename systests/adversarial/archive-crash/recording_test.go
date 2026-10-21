package archivecrash

import (
	"errors"
	"fmt"
	"math/rand"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lirm/aeron-go/aeron"
	aatomic "github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/logbuffer"
	"github.com/lirm/aeron-go/aeron/logbuffer/term"
	"github.com/lirm/aeron-go/archive/codecs"
	"github.com/lirm/aeron-go/e2e"
	"github.com/stretchr/testify/require"
)

const extendChannel = "aeron:udp?endpoint=localhost:27339"

// TestArchiveKillDuringActiveRecording SIGKILLs the archive while each publisher type is being recorded. The stale
// client must fail within its message timeout, and after a restart the recording must be listed with a stop
// position on a frame boundary that covers everything recorded before the kill, replay gap-free and byte-identical,
// and be extendable from its stop position.
func TestArchiveKillDuringActiveRecording(t *testing.T) {
	for i, k := range pubKinds {
		k, stream := k, int32(9300+i)
		t.Run(k.name, func(t *testing.T) { recordKillRecover(t, k, stream, archiveA) })
	}
}

// TestDriverKillDuringActiveRecording SIGKILLs the archiving media driver (taking the archive with it) during a
// recording: the Go client must report a driver timeout through its ErrorHandler and close, and the restarted
// archive must recover the recording as above.
func TestDriverKillDuringActiveRecording(t *testing.T) {
	for i, k := range []pubKind{pubKinds[0], pubKinds[2], pubKinds[6]} {
		k, stream := k, int32(9310+i)
		t.Run(k.name, func(t *testing.T) { recordKillRecover(t, k, stream, driverSvc) })
	}
}

func recordKillRecover(t *testing.T, k pubKind, stream int32, victim string) {
	k.skipIfUnsupported(t)
	freshStack(t)
	base := snapshot(t)
	sink := &errSink{}
	a := connect(t, sink)
	var closed atomic.Bool
	a.AddCloseHandler(func() { closed.Store(true) })
	arch := connectArchive(t, controlA, sink)

	_, err := arch.StartRecording(k.recChannel, stream, k.local, true)
	require.NoError(t, err)
	if k.liveSub != "" {
		sub, err := a.AddSubscription(k.liveSub, stream)
		require.NoError(t, err)
		go controlledDrain(sub)
	}

	var r *runner
	var jp *javaPub
	if k.java {
		jp = startJavaPub(t, k.pubChannel, stream, k.size)
	} else {
		r = runPublisher(k.open(t, a, stream), 0, -1, k.size, k.format(), k.claim, 100*time.Microsecond)
	}
	desc, err := e2e.WaitForRecording(arch, k.fragment, stream, 20*time.Second, nil)
	require.NoError(t, err)
	recID := desc.RecordingId
	require.NoError(t, e2e.WaitRecordingPosition(arch, recID, desc.StartPosition+64*1024, 20*time.Second))
	recordedBefore, err := arch.GetRecordingPosition(recID)
	require.NoError(t, err)

	kill(t, victim)
	if victim == archiveA {
		// The stale client must not hang: Java AeronArchive fails with a TimeoutException or an ArchiveException
		// for the disconnected response channel.
		err = requireBounded(t, "GetRecordingPosition on killed archive", msgTimeout+slack, func() error {
			_, err := arch.GetRecordingPosition(recID)
			return err
		})
		requireArchiveFailure(t, err)
	} else {
		awaitTrue(t, "driver timeout via ErrorHandler", 3*time.Second+5*time.Second, func() bool {
			return sink.any(func(err error) bool {
				var dte *aeron.DriverTimeoutError
				return errors.As(err, &dte)
			})
		})
		awaitTrue(t, "client close handler", 5*time.Second, closed.Load)
		require.True(t, a.IsClosed())
		err = requireBounded(t, "GetRecordingPosition after driver death", msgTimeout+slack, func() error {
			_, err := arch.GetRecordingPosition(recID)
			return err
		})
		require.Error(t, err)
	}
	requireBounded(t, "stale archive Close", msgTimeout+slack, func() error { return arch.Close() })

	var published map[int64]int64
	if r != nil {
		published = r.halt()
	} else {
		jp.kill()
	}
	if victim == driverSvc {
		start(t, driverSvc, archiveA, archiveB, archiveSml)
	} else {
		start(t, archiveA)
	}

	arch2 := connectArchive(t, controlA, nil)
	desc, err = arch2.ListRecording(recID)
	require.NoError(t, err)
	require.NotNil(t, desc)
	stop := desc.StopPosition
	t.Logf("recording %d recovered start=%d stop=%d (recorded before kill %d)", recID, desc.StartPosition, stop,
		recordedBefore)
	require.NotEqual(t, int64(aeron.NullValue), stop, "recovered recording has no stop position")
	require.GreaterOrEqual(t, stop, recordedBefore, "recovered stop position lost recorded data")
	require.Zero(t, stop%32, "stop position not frame aligned")

	lastFull := int64(-1)
	maxPub := int64(0)
	for seq, end := range published {
		if end <= stop && seq > lastFull {
			lastFull = seq
		}
		if end > maxPub {
			maxPub = end
		}
	}
	if published != nil {
		require.LessOrEqual(t, stop, maxPub+int64(k.size)+64, "stop beyond anything published")
		if k.size < 1376 {
			require.Equal(t, published[lastFull], stop, "stop position %d is not the end of a message", stop)
		}
	}

	v := &verifier{format: k.format(), size: k.size, endPos: published}
	replayVerify(t, arch2, recID, desc.StartPosition, stop, stream, v)
	if published != nil {
		require.Equal(t, lastFull+1, v.next, "replay did not deliver every message recorded before the kill")
	} else {
		lastFull = v.next - 1
		require.Positive(t, v.next)
	}

	// ExtendRecording resumes the recovered recording from its stop position.
	a2 := connect(t, nil)
	_, err = arch2.ExtendRecording(recID, stream, codecs.SourceLocation.REMOTE, true, extendChannel)
	require.NoError(t, err)
	uri, err := aeron.ParseChannelUri(extendChannel)
	require.NoError(t, err)
	require.NoError(t, uri.InitialPosition(stop, desc.InitialTermId, desc.TermBufferLength))
	uri.Set("mtu", fmt.Sprint(desc.MtuLength))
	pub2, err := a2.AddExclusivePublication(uri.String(), stream)
	require.NoError(t, err)
	require.NoError(t, e2e.WaitConnected(pub2, 10*time.Second))
	const extra = 300
	r2 := runPublisher(pub2, lastFull+1, extra, k.size, k.format(), false, 0)
	<-r2.done
	require.NoError(t, r2.err)
	target := pub2.Position()
	require.NoError(t, e2e.WaitRecordingPosition(arch2, recID, target, 20*time.Second))
	require.NoError(t, pub2.Close())
	newStop, err := e2e.WaitRecordingStopped(arch2, recID, 20*time.Second)
	require.NoError(t, err)
	require.Equal(t, target, newStop)

	all := r2.positions()
	for s, p := range published {
		if s <= lastFull {
			all[s] = p
		}
	}
	v2 := &verifier{format: k.format(), size: k.size, endPos: all}
	replayVerify(t, arch2, recID, desc.StartPosition, newStop, stream, v2)
	require.Equal(t, lastFull+1+extra, v2.next)

	require.NoError(t, arch2.Close())
	require.NoError(t, a2.Close())
	require.NoError(t, a.Close())
	requireNoStreamCounters(t, stream)
	requireNoLeaks(t, base)
}

// controlledDrain consumes a live subscription with ControlledPoll until it is closed.
func controlledDrain(sub *aeron.Subscription) {
	h := func(*aatomic.Buffer, int32, int32, *logbuffer.Header) term.ControlledPollAction {
		return term.ControlledPollActionContinue
	}
	for !sub.IsClosed() {
		if sub.ControlledPoll(h, 10) == 0 {
			time.Sleep(time.Millisecond)
		}
	}
}

// requireNoStreamCounters waits for the driver to free every publication/subscription counter of stream.
func requireNoStreamCounters(t *testing.T, stream int32) {
	t.Helper()
	obs := connect(t, nil)
	defer obs.Close()
	label := fmt.Sprintf(" %d ", stream)
	awaitTrue(t, fmt.Sprintf("driver counters for stream %d freed", stream), 15*time.Second, func() bool {
		n := 0
		for _, typeID := range []int32{1, 2, 3, 4, 5, 6, 7, 9, 12, 13} {
			n += countCounters(obs, typeID, label)
		}
		return n == 0
	})
}

// TestArchiveFrozenThenKilledDuringStartRecording SIGSTOPs the archive so StartRecording gets no response: it must
// fail with a TimeoutError after the message timeout (Java: TimeoutException). The archive is then SIGKILLed and
// restarted; the stale client must fail fast and a new client must be able to record the same channel.
func TestArchiveFrozenThenKilledDuringStartRecording(t *testing.T) {
	freshStack(t)
	base := snapshot(t)
	const stream = 9320
	channel := "aeron:udp?endpoint=localhost:27340"
	arch := connectArchive(t, controlA, nil)

	pause(t, archiveA)
	err := requireBounded(t, "StartRecording on frozen archive", msgTimeout+slack, func() error {
		_, err := arch.StartRecording(channel, stream, false, true)
		return err
	})
	var te *aeron.TimeoutError
	require.True(t, errors.As(err, &te), "want TimeoutError, got %T: %v", err, err)

	kill(t, archiveA)
	start(t, archiveA)
	err = requireBounded(t, "stale client after archive restart", msgTimeout+slack, func() error {
		_, err := arch.StartRecording(channel, stream, false, true)
		return err
	})
	requireArchiveFailure(t, err)
	requireBounded(t, "stale Close", msgTimeout+slack, func() error { return arch.Close() })

	arch2 := connectArchive(t, controlA, nil)
	subs, err := arch2.ListRecordingSubscriptions(0, 10, false, 0, "")
	require.NoError(t, err)
	require.Empty(t, subs, "recording subscriptions must not survive an archive restart")
	recordAndVerify(t, arch2, channel, stream, 100)
	require.NoError(t, arch2.Close())
	requireNoStreamCounters(t, stream)
	requireNoLeaks(t, base)
}

// TestArchiveKillRacingStartRecording SIGKILLs the archive at a random moment around StartRecording. Whatever the
// interleaving, the call must return (success or a timeout/disconnect error) within the message timeout, and the
// restarted archive must accept and record the same channel.
func TestArchiveKillRacingStartRecording(t *testing.T) {
	freshStack(t)
	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))
	for round := 0; round < 4; round++ {
		stream := int32(9321 + round)
		channel := fmt.Sprintf("aeron:udp?endpoint=localhost:%d", 27341+round)
		arch := connectArchive(t, controlA, nil)
		// docker kill takes tens of milliseconds to land, so fire it first and call StartRecording at a random
		// point inside that window.
		killed := make(chan struct{})
		go func() { defer close(killed); docker(t, "kill", "-s", "KILL", containers[archiveA]) }()
		time.Sleep(time.Duration(rnd.Intn(150)) * time.Millisecond)
		err := requireBounded(t, fmt.Sprintf("round %d StartRecording racing kill", round), msgTimeout+slack,
			func() error {
				_, err := arch.StartRecording(channel, stream, false, true)
				return err
			})
		t.Logf("round %d: StartRecording -> %v", round, err)
		if err != nil {
			requireArchiveFailure(t, err)
		}
		<-killed
		kill(t, archiveA)
		requireBounded(t, "Close", msgTimeout+slack, func() error { return arch.Close() })
		start(t, archiveA)
		arch2 := connectArchive(t, controlA, nil)
		recordAndVerify(t, arch2, channel, stream, 50)
		require.NoError(t, arch2.Close())
	}
}
