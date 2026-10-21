package resourcelimits

import (
	"fmt"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/lirm/aeron-go/aeron"
	aeronatomic "github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/counters"
	"github.com/lirm/aeron-go/aeron/logbuffer"
	"github.com/lirm/aeron-go/aeron/logbuffer/term"
	"github.com/stretchr/testify/require"
)

// systemCounter returns the value of the driver system counter whose label starts with label.
func systemCounter(a *aeron.Aeron, label string) int64 {
	var v int64 = -1
	a.CounterReader().Scan(func(c counters.Counter) {
		if c.TypeId == 0 && strings.HasPrefix(c.Label, label) {
			v = c.Value
		}
	})
	return v
}

const largeMsg = 100_000 // fragmented over ~70 MTU-sized frames

func fanMsgLen(seq int64) int32 {
	if seq%50 == 0 {
		return largeMsg
	}
	return 200
}

// fanChecker verifies reassembled messages arrive in order, with the right length and content.
type fanChecker struct {
	t    *testing.T
	next int64
}

func (f *fanChecker) on(b *aeronatomic.Buffer, o, l int32) {
	seq := b.GetInt64(o)
	require.Equal(f.t, f.next, seq, "gap or duplicate")
	require.Equal(f.t, fanMsgLen(seq), l)
	for i := int32(8); i < l; i += 997 {
		require.Equal(f.t, byte(seq+int64(i)), b.GetUInt8(o+i), "corrupt byte %d of message %d", i, seq)
	}
	require.Equal(f.t, byte(seq+int64(l-1)), b.GetUInt8(o+l-1))
	f.next++
}

// TestFanOut129Subscribers publishes fragmented (100 KB) and small messages on one UDP stream to 100 subscriptions
// on rl-driver-b (reassembled with FragmentAssembler), 28 spy subscriptions on rl-driver (controlled poll with
// ControlledFragmentAssembler) and one subscriber in an rlhelper process that is SIGKILLed half way. Every
// remaining subscriber gets every message exactly once and intact, and every available-image handler fires.
func TestFanOut129Subscribers(t *testing.T) {
	const (
		channel = "aeron:udp?endpoint=localhost:27940|term-length=1m"
		stream  = 9940
		subs    = 100
		spies   = 28
		total   = 1000
	)
	requireStack(t, driverSvc, driverBSvc)
	base := baseline(t)
	eh := &errs{}
	a := connect(t, newContext(dir, eh))
	b := connect(t, newContext(dirB, eh))

	var available atomic.Int32
	onAvailable := func(aeron.Image) { available.Add(1) }
	var polls []func() int
	var checkers []*fanChecker
	for i := 0; i < subs; i++ {
		s, err := b.AddSubscriptionWithHandlers(channel, stream, onAvailable, func(aeron.Image) {})
		require.NoError(t, err)
		c := &fanChecker{t: t}
		fa := aeron.NewFragmentAssembler(func(b *aeronatomic.Buffer, o, l int32, _ *logbuffer.Header) {
			c.on(b, o, l)
		}, 0)
		checkers = append(checkers, c)
		polls = append(polls, func() int { return s.Poll(fa.OnFragment, 64) })
	}
	for i := 0; i < spies; i++ {
		s, err := a.AddSubscriptionWithHandlers("aeron-spy:"+channel, stream, onAvailable, func(aeron.Image) {})
		require.NoError(t, err)
		c := &fanChecker{t: t}
		n := 0
		fa := aeron.NewControlledFragmentAssembler(
			func(b *aeronatomic.Buffer, o, l int32, _ *logbuffer.Header) term.ControlledPollAction {
				c.on(b, o, l)
				if n++; n%10 == 0 {
					return term.ControlledPollActionCommit
				}
				return term.ControlledPollActionContinue
			}, 0)
		checkers = append(checkers, c)
		polls = append(polls, func() int { return s.ControlledPoll(fa.OnFragment, 64) })
	}
	victim := startHelper(t, "-mode=sub", "-dir="+dirB, "-channel="+channel, fmt.Sprint("-stream=", stream))
	victim.expect(t, "ready", 10*time.Second)

	pub, err := a.AddExclusivePublication(channel, stream)
	require.NoError(t, err)
	awaitTrue(t, "all images available", 20*time.Second, func() bool { return available.Load() == subs+spies })
	victim.expect(t, "available", 10*time.Second)

	buf := payload(largeMsg)
	var seq int64
	done := func() bool {
		for _, c := range checkers {
			if c.next < total {
				return false
			}
		}
		return true
	}
	deadline := time.Now().Add(120 * time.Second)
	for !done() {
		require.True(t, time.Now().Before(deadline), "stalled at seq=%d", seq)
		if seq < total {
			l := fanMsgLen(seq)
			buf.PutInt64(0, seq)
			for i := int32(8); i < l; i++ {
				buf.PutUInt8(i, byte(seq+int64(i)))
			}
			switch r := pub.Offer(buf, 0, l, nil); {
			case r > 0:
				seq++
				if seq == total/2 {
					victim.signal(t, syscall.SIGKILL)
				}
			case r == aeron.BackPressured || r == aeron.AdminAction:
			default:
				t.Fatalf("unexpected %s", aeron.PublicationErrorString(r))
			}
		}
		for _, p := range polls {
			p()
		}
	}
	require.Equal(t, int32(subs+spies), available.Load())
	require.NoError(t, pub.Close())
	require.NoError(t, a.Close())
	require.NoError(t, b.Close())
	require.Empty(t, eh.all())
	base.check(t, dir, dirB)
}

// TestPublisherKilledMidClaim SIGKILLs an rlhelper process that has committed one TryClaim and is holding a second
// claim uncommitted. For a concurrent publication shared with a live publisher in this process, the driver unblocks
// the log (aeron.publication.unblock.timeout=4s) as Java does: the "Unblocked Publications" counter increments and
// the live publisher's messages, which were appended behind the dead claim, are all delivered in order. For an
// exclusive publication the image goes unavailable and a new publisher process can take over the stream.
func TestPublisherKilledMidClaim(t *testing.T) {
	t.Run("concurrent", func(t *testing.T) {
		const stream = 9950
		requireStack(t, driverSvc)
		base := baseline(t)
		eh := &errs{}
		a := connect(t, newContext(dir, eh))
		sub, err := a.AddSubscription(ipcChannel, stream)
		require.NoError(t, err)
		h := startHelper(t, "-mode=claim", "-dir="+dir, "-channel="+ipcChannel, fmt.Sprint("-stream=", stream),
			fmt.Sprint("-size=", msgLength))
		ready := h.expect(t, "ready", 10*time.Second)
		h.expect(t, "claimed", 10*time.Second)
		unblockedBefore := systemCounter(a, "Unblocked Publications")
		require.GreaterOrEqual(t, unblockedBefore, int64(0))
		p := newPub(t, a, "concurrent", ipcChannel, stream)
		require.Equal(t, field(ready, "sessionId"), fmt.Sprint(p.SessionID()))

		var got []int64
		poll := func() {
			sub.Poll(func(b *aeronatomic.Buffer, o, l int32, _ *logbuffer.Header) {
				require.Equal(t, int32(msgLength), l)
				got = append(got, b.GetInt64(o))
			}, 10)
		}
		awaitTrue(t, "committed claim", 10*time.Second, func() bool { poll(); return len(got) == 1 })
		require.Equal(t, int64(0), got[0])
		// 25 frames stay inside the 32 KB publication window that starts at the dead claim.
		for seq := int64(1000); seq < 1025; {
			if r := p.offerSeq(seq); r > 0 {
				seq++
			} else {
				require.Equal(t, aeron.AdminAction, r)
			}
		}
		for i := 0; i < 100; i++ {
			poll()
			time.Sleep(5 * time.Millisecond)
		}
		require.Len(t, got, 1, "nothing may be delivered past the uncommitted claim")

		h.signal(t, syscall.SIGKILL)
		killed := time.Now()
		awaitTrue(t, "unblocked", 15*time.Second, func() bool { poll(); return len(got) == 26 })
		t.Logf("unblocked %v after SIGKILL", time.Since(killed).Round(time.Millisecond))
		for i, v := range got[1:] {
			require.Equal(t, int64(1000+i), v)
		}
		require.Equal(t, unblockedBefore+1, systemCounter(a, "Unblocked Publications"))
		for seq := int64(1025); seq < 1075; {
			if p.offerSeq(seq) > 0 {
				seq++
			}
			poll()
		}
		awaitTrue(t, "after unblock", 10*time.Second, func() bool { poll(); return len(got) == 76 })
		for i, v := range got[1:] {
			require.Equal(t, int64(1000+i), v)
		}
		require.NoError(t, a.Close())
		require.Empty(t, eh.all())
		base.check(t, dir)
	})

	t.Run("exclusive", func(t *testing.T) {
		const stream = 9951
		requireStack(t, driverSvc)
		base := baseline(t)
		eh := &errs{}
		a := connect(t, newContext(dir, eh))
		var available, unavailable atomic.Int32
		sub, err := a.AddSubscriptionWithHandlers(ipcChannel, stream,
			func(aeron.Image) { available.Add(1) }, func(aeron.Image) { unavailable.Add(1) })
		require.NoError(t, err)
		h := startHelper(t, "-mode=claim", "-exclusive", "-dir="+dir, "-channel="+ipcChannel,
			fmt.Sprint("-stream=", stream), fmt.Sprint("-size=", msgLength))
		h.expect(t, "claimed", 10*time.Second)
		var n int
		count := func(*aeronatomic.Buffer, int32, int32, *logbuffer.Header) { n++ }
		awaitTrue(t, "committed claim", 10*time.Second, func() bool { sub.Poll(count, 10); return n == 1 })

		h.signal(t, syscall.SIGKILL)
		awaitTrue(t, "unavailable image", 20*time.Second, func() bool { sub.Poll(count, 10); return unavailable.Load() == 1 })
		require.Equal(t, 1, n, "the uncommitted claim must never be delivered")

		h2 := startHelper(t, "-mode=pub", "-exclusive", "-dir="+dir, "-channel="+ipcChannel,
			fmt.Sprint("-stream=", stream), fmt.Sprint("-size=", msgLength))
		h2.expect(t, "ready", 10*time.Second)
		c := &seqChecker{t: t}
		awaitTrue(t, "new publisher delivers", 20*time.Second, func() bool { sub.Poll(c.handler, 10); return c.n >= 500 })
		require.Equal(t, int32(2), available.Load())
		h2.signal(t, syscall.SIGKILL)
		awaitTrue(t, "second unavailable", 20*time.Second, func() bool { sub.Poll(c.handler, 10); return unavailable.Load() == 2 })
		require.NoError(t, a.Close())
		require.Empty(t, eh.all())
		base.check(t, dir)
	})
}
