package archivecrash

import (
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lirm/aeron-go/aeron"
	aatomic "github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/logbuffer"
	"github.com/lirm/aeron-go/aeron/logbuffer/term"
	"github.com/lirm/aeron-go/archive"
	"github.com/lirm/aeron-go/archive/codecs"
	"github.com/lirm/aeron-go/e2e"
	"github.com/stretchr/testify/require"
)

// recordCompleted records count messages of size on channel/stream and stops the recording.
func recordCompleted(t *testing.T, arch *archive.Archive, channel, fragment string, stream int32, count int64,
	size int) (*codecs.RecordingDescriptor, int64, map[int64]int64) {
	t.Helper()
	subID, err := arch.StartRecording(channel, stream, false, true)
	require.NoError(t, err)
	a := connect(t, nil)
	pub, err := a.AddExclusivePublication(channel, stream)
	require.NoError(t, err)
	require.NoError(t, e2e.WaitConnected(pub, 10*time.Second))
	r := runPublisher(pub, 0, count, size, goFormat, false, 0)
	<-r.done
	require.NoError(t, r.err)
	desc, err := e2e.WaitForRecording(arch, fragment, stream, 10*time.Second,
		func(d *codecs.RecordingDescriptor) bool { return d.SessionId == pub.SessionID() })
	require.NoError(t, err)
	require.NoError(t, e2e.WaitRecordingPosition(arch, desc.RecordingId, pub.Position(), 20*time.Second))
	require.NoError(t, arch.StopRecordingBySubscriptionId(subID))
	stop, err := e2e.WaitRecordingStopped(arch, desc.RecordingId, 10*time.Second)
	require.NoError(t, err)
	require.NoError(t, pub.Close())
	require.NoError(t, a.Close())
	return desc, stop, r.positions()
}

// recordAndVerify records count messages on channel/stream and replays them back.
func recordAndVerify(t *testing.T, arch *archive.Archive, channel string, stream int32, count int64) {
	t.Helper()
	desc, stop, positions := recordCompleted(t, arch, channel, channel[len("aeron:udp?endpoint="):], stream, count, 100)
	v := &verifier{format: goFormat, size: 100, endPos: positions}
	replayVerify(t, arch, desc.RecordingId, desc.StartPosition, stop, stream, v)
	require.Equal(t, count, v.next)
}

// imageEvents counts image available/unavailable callbacks.
type imageEvents struct{ available, unavailable atomic.Int32 }

func (e *imageEvents) onAvailable(aeron.Image)   { e.available.Add(1) }
func (e *imageEvents) onUnavailable(aeron.Image) { e.unavailable.Add(1) }

// replayConsumer is a slow replay subscriber (so the replay is still running when the fault hits).
type replayConsumer struct {
	sub        *aeron.Subscription
	controlled bool
	v          *verifier
	h          term.FragmentHandler
	ch         term.ControlledFragmentHandler
}

func newReplayConsumer(t *testing.T, a *aeron.Aeron, channel, mdsDest string, stream int32, controlled bool,
	v *verifier, ev *imageEvents) *replayConsumer {
	t.Helper()
	sub, err := a.AddSubscriptionWithHandlers(channel, stream, ev.onAvailable, ev.onUnavailable)
	require.NoError(t, err)
	if mdsDest != "" {
		require.NoError(t, sub.AddDestination(mdsDest))
	}
	t.Cleanup(func() { _ = sub.Close() })
	c := &replayConsumer{sub: sub, controlled: controlled, v: v, h: v.handler()}
	c.ch = aeron.NewControlledFragmentAssembler(func(buf *aatomic.Buffer, off, length int32,
		hdr *logbuffer.Header) term.ControlledPollAction {
		v.onMessage(buf, off, length, hdr)
		return term.ControlledPollActionContinue
	}, 4096).OnFragment
	return c
}

func (c *replayConsumer) poll(limit int) int {
	if c.controlled {
		return c.sub.ControlledPoll(c.ch, limit)
	}
	return c.sub.Poll(c.h, limit)
}

// consumeUntil polls slowly until the verifier reaches position pos or the deadline passes.
func (c *replayConsumer) consumeUntil(t *testing.T, pos int64, perMs int) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for c.v.lastPos < pos {
		c.poll(perMs)
		require.NoError(t, c.v.err)
		require.True(t, time.Now().Before(deadline), "replay stalled at %d < %d", c.v.lastPos, pos)
		time.Sleep(time.Millisecond)
	}
}

type replayCase struct {
	name       string
	victim     string
	channel    string // replay channel requested from the archive
	subChannel string // subscriber channel (session id added)
	mdsDest    string
	controlled bool
}

// TestKillDuringReplay SIGKILLs the archive or the archiving driver while a slow consumer is in the middle of a
// replay. The replay image must go unavailable (callback fires), the stale archive client must fail within its
// timeout, and after the restart a new replay from the consumed position must complete the stream gap-free and
// byte-identically. Consumers: plain poll, controlled poll and a multi-destination (manual MDS) subscription.
func TestKillDuringReplay(t *testing.T) {
	cases := []replayCase{
		{name: "archive-kill-poll", victim: archiveA, channel: "aeron:udp?endpoint=localhost:27350",
			subChannel: "aeron:udp?endpoint=localhost:27350"},
		{name: "archive-kill-controlled-poll", victim: archiveA, channel: "aeron:udp?endpoint=localhost:27351",
			subChannel: "aeron:udp?endpoint=localhost:27351", controlled: true},
		{name: "archive-kill-mds", victim: archiveA, channel: "aeron:udp?endpoint=localhost:27352",
			subChannel: "aeron:udp?control-mode=manual", mdsDest: "aeron:udp?endpoint=localhost:27352"},
		{name: "driver-kill-controlled-poll", victim: driverSvc, channel: "aeron:udp?endpoint=localhost:27353",
			subChannel: "aeron:udp?endpoint=localhost:27353", controlled: true},
	}
	for i, c := range cases {
		c, stream := c, int32(9330+i)
		t.Run(c.name, func(t *testing.T) { killDuringReplay(t, c, stream, false) })
	}
}

// TestKillDuringBoundedReplay does the same for a replay bounded by a client-owned limit counter: the replay must
// never pass the limit, neither before nor after the fault.
func TestKillDuringBoundedReplay(t *testing.T) {
	cases := []replayCase{
		{name: "archive-kill", victim: archiveA, channel: "aeron:udp?endpoint=localhost:27354",
			subChannel: "aeron:udp?endpoint=localhost:27354"},
		{name: "driver-kill", victim: driverSvc, channel: "aeron:udp?endpoint=localhost:27355",
			subChannel: "aeron:udp?endpoint=localhost:27355", controlled: true},
	}
	for i, c := range cases {
		c, stream := c, int32(9335+i)
		t.Run(c.name, func(t *testing.T) { killDuringReplay(t, c, stream, true) })
	}
}

func killDuringReplay(t *testing.T, c replayCase, stream int32, bounded bool) {
	freshStack(t)
	base := snapshot(t)
	arch := connectArchive(t, controlA, nil)
	const count, size = 4000, 500
	// A small term keeps the replay flow controlled by the slow consumer, so it is still running at the fault.
	endpoint := fmt.Sprintf("localhost:%d", 27360+stream%10)
	desc, stop, positions := recordCompleted(t, arch, "aeron:udp?term-length=65536|endpoint="+endpoint, endpoint,
		stream, count, size)
	v := &verifier{format: goFormat, size: size, endPos: positions}

	startReplay := func(arch *archive.Archive, a *aeron.Aeron, from int64, limit *aeron.Counter,
		ev *imageEvents) (*replayConsumer, int64) {
		var replaySession int64
		var err error
		if bounded {
			replaySession, err = arch.BoundedReplay(desc.RecordingId, from, stop-from, limit.ID(), stream, c.channel)
		} else {
			replaySession, err = arch.StartReplay(desc.RecordingId, from, stop-from, c.channel, stream)
		}
		require.NoError(t, err)
		ch, err := e2e.WithSessionID(c.subChannel, int32(replaySession))
		require.NoError(t, err)
		return newReplayConsumer(t, a, ch, c.mdsDest, stream, c.controlled, v, ev), replaySession
	}
	newLimit := func(a *aeron.Aeron, value int64) *aeron.Counter {
		if !bounded {
			return nil
		}
		cnt, err := a.AddCounterByLabel(1001, "adv-archive-crash bounded replay limit")
		require.NoError(t, err)
		cnt.Counter().Set(value)
		return cnt
	}

	sink := &errSink{}
	a := connect(t, sink)
	ev := &imageEvents{}
	limitPos := positions[count/4-1]
	limit := newLimit(a, limitPos)
	consumer, _ := startReplay(arch, a, desc.StartPosition, limit, ev)
	if bounded {
		consumer.consumeUntil(t, limitPos, 50)
		// Nothing beyond the limit is replayed.
		time.Sleep(300 * time.Millisecond)
		consumer.poll(100)
		require.Equal(t, limitPos, v.lastPos, "bounded replay passed its limit")
		limit.Counter().Set(positions[count/2-1])
	}
	consumer.consumeUntil(t, positions[count/3], 2)
	awaitTrue(t, "replay image available", 5*time.Second, func() bool { return ev.available.Load() == 1 })

	kill(t, c.victim)
	// Drain what was already received; the image must then go away.
	awaitTrue(t, "replay image unavailable", 20*time.Second, func() bool {
		consumer.poll(100)
		return ev.unavailable.Load() == 1
	})
	require.NoError(t, v.err)
	resumeFrom := v.lastPos
	t.Logf("replay interrupted at %d of %d", resumeFrom, stop)
	require.Less(t, resumeFrom, stop, "replay finished before the fault")
	if bounded {
		require.LessOrEqual(t, resumeFrom, positions[count/2-1], "bounded replay passed its limit")
	}
	err := requireBounded(t, "StopReplay via stale client", msgTimeout+slack, func() error {
		return arch.StopAllReplays(desc.RecordingId)
	})
	require.Error(t, err)
	requireBounded(t, "stale archive Close", msgTimeout+slack, func() error { return arch.Close() })

	if c.victim == driverSvc {
		awaitTrue(t, "client closed after driver death", 10*time.Second, a.IsClosed)
		require.NoError(t, a.Close())
		start(t, driverSvc, archiveA, archiveB, archiveSml)
		a = connect(t, nil)
		limit = newLimit(a, stop)
	} else {
		require.NoError(t, consumer.sub.Close())
		start(t, archiveA)
		if bounded {
			limit.Counter().Set(stop)
		}
	}

	arch2 := connectArchive(t, controlA, nil)
	d2, err := arch2.ListRecording(desc.RecordingId)
	require.NoError(t, err)
	require.Equal(t, stop, d2.StopPosition, "completed recording changed by the restart")
	ev2 := &imageEvents{}
	consumer2, _ := startReplay(arch2, a, resumeFrom, limit, ev2)
	consumer2.consumeUntil(t, stop, 100)
	require.Equal(t, int64(count), v.next, "resumed replay is not gap-free")
	awaitTrue(t, "replay image unavailable at end of replay", 15*time.Second, func() bool {
		consumer2.poll(10)
		return ev2.unavailable.Load() == 1
	})

	require.NoError(t, consumer2.sub.Close())
	if limit != nil {
		require.NoError(t, limit.Close())
	}
	require.NoError(t, arch2.Close())
	require.NoError(t, a.Close())
	requireNoStreamCounters(t, stream)
	requireNoLeaks(t, base)
}
