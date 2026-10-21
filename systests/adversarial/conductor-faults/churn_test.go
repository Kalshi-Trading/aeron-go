package conductorfaults

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lirm/aeron-go/aeron"
	atomicbuf "github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/logbuffer"
	"github.com/lirm/aeron-go/aeron/logbuffer/term"
	"github.com/stretchr/testify/require"
)

// churn repeatedly adds, uses and closes publications (concurrent/exclusive, IPC/UDP, offer/TryClaim), subscriptions
// (normal/spy, poll/controlled poll), counters and cancelled async adds until stop is set or the client is closed.
// check classifies each returned error and reports whether the client is closed.
func churn(c *client, g int, stop *atomic.Bool, check func(error) bool, ops *atomic.Int64) {
	buf := atomicbuf.MakeBuffer(make([]byte, 256))
	var claim logbuffer.Claim
	noop := func(*atomicbuf.Buffer, int32, int32, *logbuffer.Header) {}
	stream := nextStream()
	for i := g; !stop.Load(); i++ {
		ops.Add(1)
		switch i % 5 {
		case 0:
			sub, err := c.AddSubscription("aeron:ipc", stream)
			if check(err) {
				return
			}
			pub, err := c.AddPublication("aeron:ipc", stream)
			if check(err) {
				return
			}
			for j := 0; j < 10 && pub != nil; j++ {
				pub.Offer(buf, 0, buf.Capacity(), nil)
				if sub != nil {
					sub.Poll(noop, 10)
				}
			}
			if pub != nil {
				check(pub.Close())
			}
			if sub != nil {
				check(sub.Close())
			}
		case 1:
			ch := udp(nextPort())
			sub, err := c.AddSubscription(ch, stream)
			if check(err) {
				return
			}
			spy, err := c.AddSubscription("aeron-spy:"+ch, stream)
			if check(err) {
				return
			}
			pub, err := c.AddExclusivePublication(ch, stream)
			if check(err) {
				return
			}
			for j := 0; j < 10 && pub != nil; j++ {
				if pub.TryClaim(64, &claim) > 0 {
					claim.Commit()
				}
				for _, s := range []*aeron.Subscription{sub, spy} {
					if s != nil {
						s.ControlledPoll(func(*atomicbuf.Buffer, int32, int32, *logbuffer.Header) term.ControlledPollAction {
							return term.ControlledPollActionContinue
						}, 10)
					}
				}
			}
			if pub != nil {
				check(pub.Close())
			}
			if spy != nil {
				check(spy.Close())
			}
			if sub != nil {
				check(sub.Close())
			}
		case 2:
			ctr, err := c.AddCounterByLabel(advCounterType, fmt.Sprintf("adv-cf-churn-%d", g))
			if check(err) {
				return
			}
			if ctr != nil {
				ctr.Counter().Increment()
				check(ctr.Close())
			}
		case 3:
			id, err := c.AsyncAddPublication("aeron:ipc", stream)
			if check(err) {
				return
			}
			check(c.AsyncRemovePublication(id))
			id, err = c.AsyncAddSubscription(udp(nextPort()), stream)
			if check(err) {
				return
			}
			check(c.AsyncRemoveSubscription(id))
		case 4:
			id, err := c.AsyncAddCounterByLabel(advCounterType, "adv-cf-churn-async")
			if check(err) {
				return
			}
			for k := 0; k < 100; k++ {
				ctr, err := c.GetCounter(id)
				if check(err) {
					return
				}
				if ctr != nil || err != nil {
					if ctr != nil {
						check(ctr.Close())
					}
					break
				}
				time.Sleep(time.Millisecond)
			}
		}
	}
}

// runChurn starts n churn goroutines and returns a function that stops them and fails the test if they do not all
// exit within limit (a deadlock), plus the set of unexpected (untyped) errors and panics observed.
func runChurn(t *testing.T, c *client, n int, strict bool) (stopAndWait func(limit time.Duration), ops *atomic.Int64) {
	var stop atomic.Bool
	ops = &atomic.Int64{}
	var mu sync.Mutex
	var bad []string
	check := func(err error) bool {
		if err == nil {
			return false
		}
		if errors.Is(err, aeron.ErrClientClosed) && !strict {
			return true
		}
		if _, ok := aeron.CategoryOf(err); ok && !strict {
			return false
		}
		mu.Lock()
		bad = append(bad, fmt.Sprintf("%T: %v", err, err))
		mu.Unlock()
		return errors.Is(err, aeron.ErrClientClosed)
	}
	var wg sync.WaitGroup
	for g := 0; g < n; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					mu.Lock()
					bad = append(bad, fmt.Sprintf("panic: %v", r))
					mu.Unlock()
				}
			}()
			churn(c, g, &stop, check, ops)
		}(g)
	}
	return func(limit time.Duration) {
		t.Helper()
		stop.Store(true)
		done := make(chan struct{})
		go func() { wg.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(limit):
			t.Fatalf("churn goroutines did not exit within %v (deadlock)", limit)
		}
		mu.Lock()
		defer mu.Unlock()
		require.Empty(t, bad, "unexpected errors")
	}, ops
}

// TestConcurrentChurnAcrossDriverRestart runs many goroutines adding, using and closing every resource type while
// the driver is SIGKILLed and replaced. Every error must be a typed Aeron error (RegistrationException,
// DriverTimeoutException, ...) or client closed, nothing may panic or deadlock, the client terminates within the
// driver timeout, and a fresh client then runs the same churn error free and without leaks.
func TestConcurrentChurnAcrossDriverRestart(t *testing.T) {
	setup(t)
	before := snapshot(t)
	const driverTimeout = 3 * time.Second
	c := connect(t, driverTimeout)
	stopAndWait, ops := runChurn(t, c, 12, false)
	time.Sleep(1500 * time.Millisecond)
	require.False(t, c.IsClosed(), "client errors before restart: %v", c.errs.all())
	beforeKill := ops.Load()
	restartDriver(t)
	awaitTrue(t, driverTimeout+10*time.Second, "client terminated", c.IsClosed)
	stopAndWait(15 * time.Second)
	t.Logf("%d churn ops before the kill, %d in total", beforeKill, ops.Load())
	require.Greater(t, beforeKill, int64(100))
	// The liveness failure is usually detected inside one of the churning blocking calls, which (like Java's
	// ClientConductor.service during a client API call) returns it to that caller instead of the ErrorHandler.
	var dte *aeron.DriverTimeoutError
	require.Truef(t, errors.As(c.TerminalError(), &dte), "terminal error %T %v", c.TerminalError(), c.TerminalError())
	require.NoError(t, c.Close())

	observer := connect(t, 5*time.Second)
	c2 := connect(t, 5*time.Second)
	stop2, ops2 := runChurn(t, c2, 12, true)
	time.Sleep(1500 * time.Millisecond)
	stop2(15 * time.Second)
	require.Greater(t, ops2.Load(), int64(100))
	require.Empty(t, c2.errs.all())
	requireIPCRoundTrip(t)
	c2ID := c2.ClientID()
	require.NoError(t, c2.Close())
	awaitNoClientCounters(t, observer, c2ID, 10*time.Second)
	require.NoError(t, observer.Close())
	assertNoLeaks(t, before)
}
