package resourcelimits

import (
	"errors"
	"fmt"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/lirm/aeron-go/aeron"
	aeronatomic "github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/logbuffer"
	"github.com/stretchr/testify/require"
)

const (
	ipcChannel = "aeron:ipc?term-length=64k"
	termLength = 64 * 1024
	termWindow = termLength / 2 // ipc/publication term window and receiver window with term-length=64k
	msgLength  = 1024
	frameLen   = msgLength + 32
)

// pubKinds are the publisher variants the back-pressure tests run with.
var pubKinds = []string{"concurrent", "exclusive", "exclusive_claim"}

// testPub offers sequence-numbered messages through one of the pubKinds.
type testPub struct {
	aeron.Publisher
	claim bool
	buf   *aeronatomic.Buffer
	bc    logbuffer.Claim
}

func newPub(t *testing.T, a *aeron.Aeron, kind, channel string, stream int32) *testPub {
	t.Helper()
	p := &testPub{claim: kind == "exclusive_claim", buf: payload(msgLength)}
	var err error
	if kind == "concurrent" {
		p.Publisher, err = a.AddPublication(channel, stream)
	} else {
		p.Publisher, err = a.AddExclusivePublication(channel, stream)
	}
	require.NoError(t, err)
	return p
}

// offerSeq offers one message carrying seq and returns the Java-equivalent result.
func (p *testPub) offerSeq(seq int64) int64 {
	if p.claim {
		r := p.TryClaim(msgLength, &p.bc)
		if r > 0 {
			p.bc.Buffer().PutInt64(p.bc.Offset(), seq)
			p.bc.Commit()
		}
		return r
	}
	p.buf.PutInt64(0, seq)
	return p.Offer(p.buf, 0, msgLength, nil)
}

// seqChecker asserts messages arrive in sequence with no gaps or duplicates.
type seqChecker struct {
	t    *testing.T
	next int64
	n    int
}

func (c *seqChecker) handler(b *aeronatomic.Buffer, o, l int32, _ *logbuffer.Header) {
	c.t.Helper()
	require.Equal(c.t, int32(msgLength), l)
	got := b.GetInt64(o)
	if c.n == 0 && c.next < 0 {
		c.next = got
	}
	require.Equal(c.t, c.next, got, "gap or duplicate after %d messages", c.n)
	c.next++
	c.n++
}

// offerUntilBlocked offers until the first rejection and returns it together with the next sequence number.
func offerUntilBlocked(t *testing.T, p *testPub, seq int64) (int64, int64) {
	t.Helper()
	for i := 0; i < 10*termLength/frameLen; i++ {
		r := p.offerSeq(seq)
		if r == aeron.AdminAction {
			continue
		}
		if r < 0 {
			return r, seq
		}
		seq++
	}
	t.Fatal("publication was never back pressured")
	return 0, 0
}

// TestIPCStuckConsumerThenConsumerKilled: a consumer in a separate rlhelper process stops polling, so the IPC
// publication may only run one term window (term-length/2) ahead of it and then returns BACK_PRESSURED for every
// further offer. The consumer process is then SIGKILLed while back pressured: once the driver times the client out
// (aeron.client.liveness.timeout=3s) the publication has no subscriber and returns NOT_CONNECTED, as Java's
// Publication.backPressureStatus does. A new subscriber joins at the last consumer position and gets every message,
// including the ones the killed consumer never read, exactly once.
func TestIPCStuckConsumerThenConsumerKilled(t *testing.T) {
	for i, kind := range pubKinds {
		t.Run(kind, func(t *testing.T) {
			stream := int32(9910 + i)
			requireStack(t, driverSvc)
			base := baseline(t)
			eh := &errs{}
			a := connect(t, newContext(dir, eh))
			stuck := startHelper(t, "-mode=sub", "-dir="+dir, "-channel="+ipcChannel,
				fmt.Sprint("-stream=", stream), "-stall-after=0")
			stuck.expect(t, "stalled", 10*time.Second)
			p := newPub(t, a, kind, ipcChannel, stream)
			awaitTrue(t, "connected", 10*time.Second, func() bool { return p.IsConnected() && p.PositionLimit() > 0 })

			r, seq := offerUntilBlocked(t, p, 0)
			require.Equal(t, aeron.PublicationErrorString(aeron.BackPressured), aeron.PublicationErrorString(r))
			require.Equal(t, int64(termWindow), p.PositionLimit())
			require.Equal(t, int64((termWindow+frameLen-1)/frameLen*frameLen), p.Position())
			for i := 0; i < 50; i++ {
				require.Equal(t, aeron.BackPressured, p.offerSeq(seq))
				time.Sleep(5 * time.Millisecond)
			}

			stuck.signal(t, syscall.SIGKILL)
			killed := time.Now()
			awaitTrue(t, "NOT_CONNECTED after the consumer is killed", 10*time.Second, func() bool {
				r := p.offerSeq(seq)
				require.Contains(t, []int64{aeron.BackPressured, aeron.NotConnected}, r)
				return r == aeron.NotConnected
			})
			t.Logf("NOT_CONNECTED %v after SIGKILL", time.Since(killed).Round(time.Millisecond))
			require.False(t, p.IsConnected())

			var available atomic.Int32
			sub, err := a.AddSubscriptionWithHandlers(ipcChannel, stream,
				func(aeron.Image) { available.Add(1) }, func(aeron.Image) {})
			require.NoError(t, err)
			awaitTrue(t, "resubscribed", 10*time.Second, func() bool { return sub.IsConnected() && p.IsConnected() })
			require.Equal(t, int32(1), available.Load())
			// As in Java (IpcPublication.joinPosition), the new subscriber joins at the last consumer position, so the
			// messages the killed consumer never read are delivered too: nothing is lost.
			c := &seqChecker{t: t}
			end := seq + 500
			deadline := time.Now().Add(20 * time.Second)
			for c.n < int(end) {
				require.True(t, time.Now().Before(deadline), "resumed delivery stalled at %d", c.n)
				if seq < end && p.offerSeq(seq) > 0 {
					seq++
				}
				sub.Poll(c.handler, 10)
			}
			require.NoError(t, sub.Close())
			require.NoError(t, p.Close())
			require.NoError(t, a.Close())
			require.Empty(t, eh.all())
			base.check(t, dir)
		})
	}
}

// TestDriverKilledWhileBackPressured SIGKILLs the driver container while the publisher is back pressured by a
// consumer that does not poll. Every offer keeps returning promptly (BACK_PRESSURED) until the client detects the
// driver timeout (3s here); then, as in Java, the client terminates with a DriverTimeoutError on the ErrorHandler,
// the subscription's unavailable-image handler fires, offers return CLOSED and Close returns. A new client on a
// restarted driver works.
func TestDriverKilledWhileBackPressured(t *testing.T) {
	for i, kind := range pubKinds {
		t.Run(kind, func(t *testing.T) {
			stream := int32(9915 + i)
			requireStack(t, driverSvc)
			base := baseline(t)
			eh := &errs{}
			a, err := aeron.Connect(newContext(dir, eh).MediaDriverTimeout(3 * time.Second))
			require.NoError(t, err)
			t.Cleanup(func() { _ = a.Close() })
			var unavailable atomic.Int32
			sub, err := a.AddSubscriptionWithHandlers(ipcChannel, stream, func(aeron.Image) {},
				func(aeron.Image) { unavailable.Add(1) })
			require.NoError(t, err)
			p := newPub(t, a, kind, ipcChannel, stream)
			awaitTrue(t, "connected", 10*time.Second, func() bool { return p.IsConnected() && p.PositionLimit() > 0 })
			r, seq := offerUntilBlocked(t, p, 0)
			require.Equal(t, aeron.BackPressured, r)

			signalService(t, driverSvc, "KILL")
			killed := time.Now()
			var slowest time.Duration
			awaitTrue(t, "CLOSED after driver timeout", 15*time.Second, func() bool {
				start := time.Now()
				r := p.offerSeq(seq)
				slowest = max(slowest, time.Since(start))
				require.Contains(t, []int64{aeron.BackPressured, aeron.PublicationClosed}, r)
				return r == aeron.PublicationClosed
			})
			detected := time.Since(killed)
			t.Logf("CLOSED %v after SIGKILL, slowest offer %v", detected.Round(time.Millisecond), slowest)
			require.Less(t, slowest, 100*time.Millisecond, "offer must not block on a dead driver")
			require.GreaterOrEqual(t, detected, 2*time.Second)
			require.True(t, a.IsClosed())
			require.True(t, p.IsClosed())
			awaitTrue(t, "unavailable image", 5*time.Second, func() bool { return unavailable.Load() == 1 })
			var timeout *aeron.DriverTimeoutError
			require.NotNil(t, eh.find(func(err error) bool { return errors.As(err, &timeout) }), "%v", eh.all())
			require.Equal(t, aeron.PublicationClosed, p.offerSeq(seq))
			within(t, "Close after driver timeout", 5*time.Second, a.Close)
			require.True(t, sub.IsClosed())
			base.check(t, dir)

			freshStack(t, driverSvc)
			transferIPC(t, kind, stream, 200)
			base.check(t, dir)
		})
	}
}

// TestDriverPausedWhileBackPressured SIGSTOPs the driver for less than the driver timeout while the publisher is
// back pressured, then SIGCONTs it: the client must survive, offers keep returning BACK_PRESSURED while it is
// paused, and once the consumer polls again every message is delivered exactly once.
func TestDriverPausedWhileBackPressured(t *testing.T) {
	for i, kind := range pubKinds {
		t.Run(kind, func(t *testing.T) {
			stream := int32(9918 + i)
			requireStack(t, driverSvc)
			base := baseline(t)
			eh := &errs{}
			a := connect(t, newContext(dir, eh))
			sub, err := a.AddSubscription(ipcChannel, stream)
			require.NoError(t, err)
			p := newPub(t, a, kind, ipcChannel, stream)
			awaitTrue(t, "connected", 10*time.Second, func() bool { return p.IsConnected() && p.PositionLimit() > 0 })
			r, seq := offerUntilBlocked(t, p, 0)
			require.Equal(t, aeron.BackPressured, r)

			signalService(t, driverSvc, "STOP")
			paused := time.Now()
			for time.Since(paused) < 2*time.Second {
				require.Equal(t, aeron.BackPressured, p.offerSeq(seq))
				time.Sleep(10 * time.Millisecond)
			}
			signalService(t, driverSvc, "CONT")

			c := &seqChecker{t: t}
			const total = 2000
			deadline := time.Now().Add(30 * time.Second)
			for c.n < total {
				require.True(t, time.Now().Before(deadline), "delivery stalled at %d", c.n)
				if seq < total && p.offerSeq(seq) > 0 {
					seq++
				}
				sub.Poll(c.handler, 10)
			}
			require.False(t, a.IsClosed())
			require.NoError(t, a.Close())
			require.Empty(t, eh.all())
			base.check(t, dir)
		})
	}
}

// transferIPC checks a fresh client can publish n messages to itself over IPC with the given publisher kind.
func transferIPC(t *testing.T, kind string, stream int32, n int) {
	t.Helper()
	eh := &errs{}
	a := connect(t, newContext(dir, eh))
	sub, err := a.AddSubscription(ipcChannel, stream)
	require.NoError(t, err)
	p := newPub(t, a, kind, ipcChannel, stream)
	awaitTrue(t, "connected", 10*time.Second, p.IsConnected)
	c := &seqChecker{t: t}
	var seq int64
	deadline := time.Now().Add(20 * time.Second)
	for c.n < n {
		require.True(t, time.Now().Before(deadline), "delivery stalled at %d", c.n)
		if seq < int64(n) && p.offerSeq(seq) > 0 {
			seq++
		}
		sub.Poll(c.handler, 10)
	}
	require.NoError(t, a.Close())
	require.Empty(t, eh.all())
}

// TestStuckReceiverMinMaxFlowControl runs a dynamic MDC publication on rl-driver with two receivers: a fast one on
// rl-driver-b and a stuck one (an rlhelper process on rl-driver that never polls). With fc=min the stuck receiver
// holds the publisher back (BACK_PRESSURED) until it is SIGKILLed, after which the driver drops it
// (client liveness 3s + aeron.min.flow.control.receiver.timeout 2s) and the publisher resumes. With fc=max the
// stuck receiver never holds the publisher back. In both cases the fast receiver gets every message exactly once.
func TestStuckReceiverMinMaxFlowControl(t *testing.T) {
	for i, fc := range []string{"min", "max"} {
		t.Run(fc, func(t *testing.T) {
			stream := int32(9921 + i)
			control := 27920 + 3*i
			pubChannel := fmt.Sprintf("aeron:udp?control=localhost:%d|control-mode=dynamic|fc=%s|term-length=64k",
				control, fc)
			subChannel := func(port int) string {
				return fmt.Sprintf("aeron:udp?endpoint=localhost:%d|control=localhost:%d|control-mode=dynamic",
					port, control)
			}
			requireStack(t, driverSvc, driverBSvc)
			base := baseline(t)
			eh := &errs{}
			a := connect(t, newContext(dir, eh))
			b := connect(t, newContext(dirB, eh))
			fast, err := b.AddSubscription(subChannel(control+1), stream)
			require.NoError(t, err)
			stuck := startHelper(t, "-mode=sub", "-dir="+dir, "-channel="+subChannel(control+2),
				fmt.Sprint("-stream=", stream), "-stall-after=0")
			stuck.expect(t, "stalled", 10*time.Second)
			p := newPub(t, a, "exclusive", pubChannel, stream)
			awaitTrue(t, "fast receiver connected", 10*time.Second, fast.IsConnected)
			stuck.expect(t, "available", 10*time.Second)

			c := &seqChecker{t: t}
			var seq int64
			const total = 3000 // ~48 terms
			blockedSince := time.Time{}
			killed := false
			deadline := time.Now().Add(60 * time.Second)
			for c.n < total {
				require.True(t, time.Now().Before(deadline), "stalled at sent=%d received=%d", seq, c.n)
				if seq < total {
					r := p.offerSeq(seq)
					switch {
					case r > 0:
						seq++
						blockedSince = time.Time{}
					case r == aeron.AdminAction:
					case r == aeron.BackPressured:
						if blockedSince.IsZero() {
							blockedSince = time.Now()
						}
						if time.Since(blockedSince) > time.Second && !killed {
							require.Equal(t, "min", fc, "fc=max must not be held back by the stuck receiver")
							require.Less(t, seq, int64(2*termLength/frameLen), "the stuck receiver held at %d", seq)
							stuck.signal(t, syscall.SIGKILL)
							killed = true
						}
					default:
						t.Fatalf("unexpected %s", aeron.PublicationErrorString(r))
					}
				}
				fast.Poll(c.handler, 10)
			}
			require.Equal(t, fc == "min", killed)
			if !killed {
				stuck.signal(t, syscall.SIGKILL)
			}
			require.NoError(t, p.Close())
			require.NoError(t, a.Close())
			require.NoError(t, b.Close())
			require.Empty(t, eh.all())
			base.check(t, dir, dirB)
		})
	}
}

// TestSlowConsumerBoundedByTermWindow: a consumer that polls one fragment per millisecond can never fall more than
// the term window behind the publisher (IPC: publication window; UDP: publication window + receiver window), the
// publisher sees BACK_PRESSURED instead, and no message is lost or duplicated.
func TestSlowConsumerBoundedByTermWindow(t *testing.T) {
	cases := []struct {
		name, channel string
		subDir        string
		maxLag        int64
	}{
		{"ipc", ipcChannel, dir, termWindow + frameLen},
		{"udp", "aeron:udp?endpoint=localhost:27930|term-length=64k", dirB, 2*termWindow + frameLen},
	}
	for i, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			stream := int32(9930 + i)
			requireStack(t, driverSvc, driverBSvc)
			base := baseline(t)
			eh := &errs{}
			a := connect(t, newContext(dir, eh))
			subClient := a
			if tc.subDir != dir {
				subClient = connect(t, newContext(tc.subDir, eh))
			}
			sub, err := subClient.AddSubscription(tc.channel, stream)
			require.NoError(t, err)
			p := newPub(t, a, "exclusive", tc.channel, stream)
			awaitTrue(t, "connected", 10*time.Second, func() bool { return p.IsConnected() && sub.IsConnected() })
			img := sub.ImageBySessionID(p.SessionID())
			require.NotNil(t, img)

			c := &seqChecker{t: t}
			var seq int64
			var backPressured, maxLag int64
			const total = 1500 // ~24 terms
			deadline := time.Now().Add(60 * time.Second)
			for c.n < total {
				require.True(t, time.Now().Before(deadline), "stalled at %d", c.n)
				for burst := 0; burst < 8 && seq < total; burst++ {
					switch r := p.offerSeq(seq); {
					case r > 0:
						seq++
						maxLag = max(maxLag, r-img.Position())
					case r == aeron.AdminAction:
					case r == aeron.BackPressured:
						backPressured++
					default:
						t.Fatalf("unexpected %s", aeron.PublicationErrorString(r))
					}
				}
				sub.Poll(c.handler, 1)
				time.Sleep(time.Millisecond)
			}
			t.Logf("max lag %d bytes, %d BACK_PRESSURED", maxLag, backPressured)
			require.LessOrEqual(t, maxLag, tc.maxLag)
			require.Greater(t, backPressured, int64(0))
			require.NoError(t, a.Close())
			require.NoError(t, subClient.Close())
			require.Empty(t, eh.all())
			base.check(t, dir, dirB)
		})
	}
}
