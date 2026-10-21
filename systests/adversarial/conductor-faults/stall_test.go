package conductorfaults

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/lirm/aeron-go/aeron"
	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/logbuffer"
	"github.com/stretchr/testify/require"
)

const stallTimeout = 1500 * time.Millisecond

// stalledOp is a blocking client command issued while the driver is SIGSTOPped. prepare runs with the driver alive.
type stalledOp struct {
	name    string
	prepare func(t *testing.T, c *client) func() error
}

func stalledOps() []stalledOp {
	return []stalledOp{
		{"AddPublication", func(t *testing.T, c *client) func() error {
			return func() error { _, err := c.AddPublication(udp(nextPort()), nextStream()); return err }
		}},
		{"AddExclusivePublication", func(t *testing.T, c *client) func() error {
			return func() error { _, err := c.AddExclusivePublication("aeron:ipc", nextStream()); return err }
		}},
		{"AddSubscription", func(t *testing.T, c *client) func() error {
			return func() error { _, err := c.AddSubscription(udp(nextPort()), nextStream()); return err }
		}},
		{"AddCounter", func(t *testing.T, c *client) func() error {
			return func() error { _, err := c.AddCounterByLabel(1001, "adv-conductor-faults stalled"); return err }
		}},
		{"AddStaticCounter", func(t *testing.T, c *client) func() error {
			return func() error {
				_, err := c.AddStaticCounterByLabel(1002, "adv-conductor-faults static", 98001)
				return err
			}
		}},
		{"AddDestination", func(t *testing.T, c *client) func() error {
			pub, err := c.AddExclusivePublication("aeron:udp?control-mode=manual", nextStream())
			require.NoError(t, err)
			return func() error { _, err := c.AddDestination(pub.RegistrationID(), udp(nextPort())); return err }
		}},
		{"RemoveDestination", func(t *testing.T, c *client) func() error {
			pub, err := c.AddPublication("aeron:udp?control-mode=manual", nextStream())
			require.NoError(t, err)
			dest := udp(nextPort())
			_, err = c.AddDestination(pub.RegistrationID(), dest)
			require.NoError(t, err)
			return func() error { return c.RemoveDestination(pub.RegistrationID(), dest) }
		}},
		{"AddRcvDestination", func(t *testing.T, c *client) func() error {
			sub, err := c.AddSubscription("aeron:udp?control-mode=manual", nextStream())
			require.NoError(t, err)
			return func() error { return c.AddRcvDestination(sub.RegistrationID(), udp(nextPort())) }
		}},
		{"RemoveRcvDestination", func(t *testing.T, c *client) func() error {
			sub, err := c.AddSubscription("aeron:udp?control-mode=manual", nextStream())
			require.NoError(t, err)
			dest := udp(nextPort())
			require.NoError(t, c.AddRcvDestination(sub.RegistrationID(), dest))
			return func() error { return c.RemoveRcvDestination(sub.RegistrationID(), dest) }
		}},
		{"NextSessionID", func(t *testing.T, c *client) func() error {
			return func() error { _, err := c.NextSessionID(nextStream()); return err }
		}},
	}
}

// TestStalledDriverBlockingCommandsTimeOut SIGSTOPs the driver while each blocking command is in flight, together
// with concurrent blocked callers. Java: awaitResponse throws DriverTimeoutException after driverTimeoutMs; the
// conductor then terminates (keepalive stale) and every other waiter gets a client-closed error; all resources close.
// After SIGCONT the driver times the dead client out and frees its counters; a new client works.
func TestStalledDriverBlockingCommandsTimeOut(t *testing.T) {
	for _, op := range stalledOps() {
		op := op
		t.Run(op.name, func(t *testing.T) {
			setup(t)
			observer := connect(t, 10*time.Second)
			before := snapshot(t)
			c := connect(t, stallTimeout)
			sub, err := c.AddSubscription("aeron:ipc", nextStream())
			require.NoError(t, err)
			cmd := op.prepare(t, c)
			clientID := c.ClientID()

			signalDriver(t, "STOP")
			var wg sync.WaitGroup
			otherErrs := make(chan error, 4)
			var opErr error
			elapsed := within(t, stallTimeout+5*time.Second, op.name, func() {
				// The command under test takes the conductor lock first; the concurrent callers queue behind it.
				// Add before spawning: an Add from zero racing with Wait is a WaitGroup misuse (flagged by -race).
				wg.Add(4)
				go func() {
					time.Sleep(50 * time.Millisecond)
					for i := 0; i < 4; i++ {
						go func() {
							defer wg.Done()
							_, err := c.AddPublication("aeron:ipc", nextStream())
							otherErrs <- err
						}()
					}
				}()
				opErr = cmd()
			})
			time.Sleep(100 * time.Millisecond)
			wg.Wait()
			close(otherErrs)

			var dte *aeron.DriverTimeoutError
			require.Truef(t, errors.As(opErr, &dte), "%s: want DriverTimeoutError, got %T %v", op.name, opErr, opErr)
			require.GreaterOrEqual(t, elapsed, stallTimeout*9/10, "%s returned before the driver timeout", op.name)
			for err := range otherErrs {
				require.Error(t, err)
				require.Truef(t, errors.As(err, &dte) || errors.Is(err, aeron.ErrClientClosed),
					"concurrent add: want DriverTimeoutError or ErrClientClosed, got %T %v", err, err)
			}

			awaitTrue(t, 3*stallTimeout, "client terminated", c.IsClosed)
			require.Truef(t, errors.As(c.TerminalError(), &dte), "terminal error %T %v", c.TerminalError(), c.TerminalError())
			// Java service() rethrows a liveness failure hit inside a client API call to the caller instead of the
			// ErrorHandler, so the handler is not required to see it here (see TestDriverKeepaliveLossIdleClient).
			require.True(t, sub.IsClosed())

			signalDriver(t, "CONT")
			require.NoError(t, c.Close())
			assertNoLeaks(t, before)
			awaitNoClientCounters(t, observer, clientID, 10*time.Second)
			requireIPCRoundTrip(t)
		})
	}
}

// TestShortDriverStallCommandCompletes stalls the driver for less than the driver timeout: the blocked commands
// complete after SIGCONT and the client stays healthy (Java: awaitResponse keeps waiting until the deadline).
func TestShortDriverStallCommandCompletes(t *testing.T) {
	setup(t)
	c := connect(t, 4*time.Second)
	signalDriver(t, "STOP")
	type result struct {
		pub *aeron.Publication
		sub *aeron.Subscription
		ctr *aeron.Counter
		err error
	}
	res := make(chan result, 3)
	stream := nextStream()
	go func() { p, err := c.AddPublication("aeron:ipc", stream); res <- result{pub: p, err: err} }()
	go func() { s, err := c.AddSubscription("aeron:ipc", stream); res <- result{sub: s, err: err} }()
	go func() { k, err := c.AddCounterByLabel(1003, "adv short stall"); res <- result{ctr: k, err: err} }()
	time.Sleep(time.Second)
	signalDriver(t, "CONT")
	var pub *aeron.Publication
	var sub *aeron.Subscription
	for i := 0; i < 3; i++ {
		select {
		case r := <-res:
			require.NoError(t, r.err)
			if r.pub != nil {
				pub = r.pub
			}
			if r.sub != nil {
				sub = r.sub
			}
		case <-time.After(10 * time.Second):
			t.Fatal("command did not complete after SIGCONT")
		}
	}
	require.False(t, c.IsClosed())
	roundTrip(t, pub, sub, 200)
	require.Empty(t, c.errs.all())
}

func requireIPCRoundTrip(t *testing.T) {
	t.Helper()
	c := connect(t, 5*time.Second)
	stream := nextStream()
	sub, err := c.AddSubscription("aeron:ipc", stream)
	require.NoError(t, err)
	pub, err := c.AddPublication("aeron:ipc", stream)
	require.NoError(t, err)
	roundTrip(t, pub, sub, 100)
	require.NoError(t, c.Close())
}

// roundTrip publishes n sequenced messages on pub and checks sub receives exactly them, in order, uncorrupted.
func roundTrip(t *testing.T, pub aeron.Publisher, sub *aeron.Subscription, n int64) {
	t.Helper()
	awaitTrue(t, 10*time.Second, "publication connected", pub.IsConnected)
	msg := make([]byte, 128)
	buf := atomic.MakeBuffer(msg)
	next := int64(0)
	handler := func(b *atomic.Buffer, offset, length int32, _ *logbuffer.Header) {
		seq, err := decode(b.GetBytesArray(offset, length))
		require.NoError(t, err)
		require.Equal(t, next, seq, "gap or duplicate")
		next++
	}
	deadline := time.Now().Add(15 * time.Second)
	for seq := int64(0); seq < n; {
		encode(msg, seq)
		if pub.Offer(buf, 0, int32(len(msg)), nil) > 0 {
			seq++
		}
		sub.Poll(handler, 64)
		require.True(t, time.Now().Before(deadline), "publish timed out at %d", seq)
	}
	for next < n {
		if sub.Poll(handler, 64) == 0 {
			time.Sleep(100 * time.Microsecond)
		}
		require.True(t, time.Now().Before(deadline), "consume timed out at %d", next)
	}
}
