package clientcrash

import (
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lirm/aeron-go/aeron"
	ab "github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/logbuffer"
	"github.com/lirm/aeron-go/aeron/logbuffer/term"
	"github.com/lirm/aeron-go/e2e"
	"github.com/stretchr/testify/require"
)

func ipcPair(t *testing.T, a *aeron.Aeron, channel string, stream int32) (*aeron.ExclusivePublication, *aeron.Subscription) {
	t.Helper()
	sub, err := a.AddSubscription(channel, stream)
	require.NoError(t, err)
	pub, err := a.AddExclusivePublication(channel, stream)
	require.NoError(t, err)
	require.NoError(t, e2e.WaitConnected(pub, 10*time.Second))
	return pub, sub
}

func decode(buf *ab.Buffer, offset, length int32) int64 {
	seq, err := e2e.DecodeMessage(buf.GetBytesArray(offset, length))
	if err != nil {
		panic(err)
	}
	return seq
}

// recovered runs f and returns the value it panicked with, if any.
func recovered(f func()) (r any) {
	defer func() { r = recover() }()
	f()
	return nil
}

// TestPollHandlerPanic panics from Poll and ControlledPoll fragment handlers. As in Java (Image.poll advances the
// subscriber position in a finally block past the fragment whose handler threw), the panic must reach the caller and
// the failing fragment must be consumed: the next poll continues with the following message, nothing is redelivered
// or skipped, and the client stays usable.
func TestPollHandlerPanic(t *testing.T) {
	requireAdv(t)
	leaks := baseline()
	a := connect(t, newContext())
	for _, controlled := range []bool{false, true} {
		t.Run(fmt.Sprintf("controlled=%v", controlled), func(t *testing.T) {
			stream := int32(9140)
			if controlled {
				stream = 9141
			}
			pub, sub := ipcPair(t, a, "aeron:ipc", stream)
			require.NoError(t, e2e.Publish(pub, e2e.PublishOptions{Count: 20, Size: 64, Timeout: 5 * time.Second}))
			var got []int64
			onMessage := func(buf *ab.Buffer, offset, length int32) {
				seq := decode(buf, offset, length)
				got = append(got, seq)
				if seq == 5 || seq == 12 {
					panic(fmt.Sprintf("handler panic at %d", seq))
				}
			}
			var panics []any
			deadline := time.Now().Add(5 * time.Second)
			for len(got) < 20 && time.Now().Before(deadline) {
				if r := recovered(func() {
					if controlled {
						sub.ControlledPoll(func(buf *ab.Buffer, o, l int32, _ *logbuffer.Header) term.ControlledPollAction {
							onMessage(buf, o, l)
							return term.ControlledPollActionContinue
						}, 3)
					} else {
						sub.Poll(func(buf *ab.Buffer, o, l int32, _ *logbuffer.Header) { onMessage(buf, o, l) }, 3)
					}
				}); r != nil {
					panics = append(panics, r)
				}
			}
			require.Len(t, panics, 2)
			want := make([]int64, 20)
			for i := range want {
				want[i] = int64(i)
			}
			require.Equal(t, want, got, "each message exactly once, the panicking ones included")
			require.False(t, a.IsClosed())
			require.NoError(t, e2e.Publish(pub, e2e.PublishOptions{From: 20, Count: 10, Size: 64, Timeout: 5 * time.Second}))
			checker := e2e.NewSequenceChecker(20)
			require.NoError(t, e2e.Consume(e2e.ReassemblingPoller(sub), checker, 10, 5*time.Second))
			require.NoError(t, pub.Close())
			require.NoError(t, sub.Close())
		})
	}
	require.NoError(t, a.Close())
	leaks.requireNoLeaks(t)
}

// TestCloseFromCallbacks closes resources and the client from inside callbacks. From a poll handler (application
// goroutine) Subscription.Close and Aeron.Close must complete without deadlock and the poll must return; from an
// image handler (conductor goroutine) Publication.Close must work and Aeron.Close must return ErrCloseFromCallback
// and complete asynchronously (Java defers the close in the same way via its re-entrant conductor), firing the close
// handler once. The driver must reclaim everything and nothing may leak.
func TestCloseFromCallbacks(t *testing.T) {
	requireAdv(t)
	leaks := baseline()
	observer := connect(t, newContext())

	t.Run("subscription-in-poll", func(t *testing.T) {
		a := connect(t, newContext())
		pub, sub := ipcPair(t, a, "aeron:ipc?alias=advk-cb-subclose", 9142)
		require.NoError(t, e2e.Publish(pub, e2e.PublishOptions{Count: 10, Size: 64, Timeout: 5 * time.Second}))
		var calls int
		withTimeout(t, "poll closing its subscription", 5*time.Second, func() {
			for calls == 0 {
				sub.Poll(func(*ab.Buffer, int32, int32, *logbuffer.Header) {
					calls++
					require.NoError(t, sub.Close())
				}, 10)
			}
		})
		require.True(t, sub.IsClosed())
		require.Zero(t, sub.Poll(func(*ab.Buffer, int32, int32, *logbuffer.Header) { calls++ }, 10))
		awaitTrue(t, "driver to drop the subscription", reclaimTO, func() bool {
			return len(counterLabels(observer, "sub-pos")) == 0
		})
		require.NoError(t, a.Close())
	})

	t.Run("client-in-poll", func(t *testing.T) {
		a := connect(t, newContext())
		pub, sub := ipcPair(t, a, "aeron:ipc?alias=advk-cb-cliclose", 9143)
		require.NoError(t, e2e.Publish(pub, e2e.PublishOptions{Count: 10, Size: 64, Timeout: 5 * time.Second}))
		id := a.ClientID()
		var closeErr error
		var calls int
		withTimeout(t, "poll closing the client", 10*time.Second, func() {
			for calls == 0 {
				sub.Poll(func(*ab.Buffer, int32, int32, *logbuffer.Header) {
					calls++
					closeErr = a.Close()
				}, 10)
			}
		})
		require.NoError(t, closeErr)
		require.True(t, a.IsClosed())
		require.True(t, pub.IsClosed())
		require.True(t, sub.IsClosed())
		awaitReclaimed(t, observer, id, "advk-cb-cliclose")
	})

	t.Run("publication-and-client-in-image-handlers", func(t *testing.T) {
		closed := make(chan struct{})
		var closeCalls atomic.Int32
		a := connect(t, newContext().CloseHandler(func() {
			if closeCalls.Add(1) == 1 {
				close(closed)
			}
		}))
		id := a.ClientID()
		const channel = "aeron:ipc?alias=advk-cb-imgclose"
		pubs := make(chan *aeron.Publication, 1)
		pubCloseErr := make(chan error, 1)
		clientCloseErr := make(chan error, 1)
		_, err := a.AddSubscriptionWithHandlers(channel, 9144,
			func(aeron.Image) { pubCloseErr <- (<-pubs).Close() },
			func(aeron.Image) { clientCloseErr <- a.Close() })
		require.NoError(t, err)
		pub, err := a.AddPublication(channel, 9144)
		require.NoError(t, err)
		pubs <- pub
		select {
		case err := <-pubCloseErr:
			require.NoError(t, err)
		case <-time.After(10 * time.Second):
			t.Fatal("Publication.Close from the available image handler did not return")
		}
		// Closing the only publication makes the image unavailable, whose handler closes the client.
		select {
		case err := <-clientCloseErr:
			require.True(t, errors.Is(err, aeron.ErrCloseFromCallback), "%v", err)
		case <-time.After(reclaimTO):
			t.Fatal("unavailable image handler did not run")
		}
		select {
		case <-closed:
		case <-time.After(5 * time.Second):
			t.Fatal("client close requested from a callback did not complete")
		}
		require.True(t, a.IsClosed())
		require.NoError(t, a.Close())
		require.EqualValues(t, 1, closeCalls.Load())
		awaitReclaimed(t, observer, id, "advk-cb-imgclose")
	})

	require.NoError(t, observer.Close())
	leaks.requireNoLeaks(t)
}

// TestDoubleClose closes every resource twice, before and after the client, which must all succeed like Java's
// idempotent close.
func TestDoubleClose(t *testing.T) {
	requireAdv(t)
	leaks := baseline()
	a := connect(t, newContext())
	pub, err := a.AddPublication("aeron:udp?endpoint=localhost:27140", 9145)
	require.NoError(t, err)
	ex, err := a.AddExclusivePublication("aeron:ipc", 9145)
	require.NoError(t, err)
	sub, err := a.AddSubscription("aeron:udp?endpoint=localhost:27140", 9145)
	require.NoError(t, err)
	sub2, err := a.AddSubscription("aeron:ipc", 9145)
	require.NoError(t, err)
	counter, err := a.AddCounterByLabel(1001, "advk-double-close")
	require.NoError(t, err)
	for i := 0; i < 2; i++ {
		require.NoError(t, pub.Close())
		require.NoError(t, sub.Close())
		require.NoError(t, counter.Close())
	}
	for i := 0; i < 2; i++ {
		require.NoError(t, a.Close())
	}
	require.NoError(t, ex.Close())
	require.NoError(t, sub2.Close())
	require.True(t, ex.IsClosed() && sub2.IsClosed() && counter.IsClosed())
	leaks.requireNoLeaks(t)
}

// TestCloseRacesOfferPoll closes the client while other goroutines Offer, TryClaim, Poll, ControlledPoll, add
// resources and read positions, many times over (run under -race). Every call must return a defined result (a
// position, a Java back-pressure/closed code or ErrClientClosed), never panic or hang, and nothing may leak.
//
// As in Java, the client unmaps its log buffers once Close has lingered for aeron.client.close.linger.duration
// (default 0): Java documents that without a linger, threads still using a publication or image can fault on the
// unmapped file. The test therefore sets the linger, as a Java application closing under concurrent use must.
func TestCloseRacesOfferPoll(t *testing.T) {
	requireAdv(t)
	leaks := baseline()
	for i := 0; i < 20; i++ {
		a := connect(t, newContext().CloseLingerDuration(100*time.Millisecond))
		pub, err := a.AddPublication("aeron:ipc", 9146)
		require.NoError(t, err)
		ex, sub := ipcPair(t, a, "aeron:udp?endpoint=localhost:27141", 9147)
		ipcSub, err := a.AddSubscription("aeron:ipc", 9146)
		require.NoError(t, err)

		var wg sync.WaitGroup
		var unexpected atomic.Value
		check := func(what string, r int64) bool {
			switch {
			case r > 0, r == aeron.NotConnected, r == aeron.BackPressured, r == aeron.AdminAction:
				return true
			case r == aeron.PublicationClosed:
				return false
			}
			unexpected.Store(fmt.Sprintf("%s returned %d", what, r))
			return false
		}
		run := func(f func() bool) {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for f() {
				}
			}()
		}
		buf := ab.MakeBuffer(make([]byte, 256))
		run(func() bool { return check("Offer", pub.Offer(buf, 0, 256, nil)) })
		var claim logbuffer.Claim
		run(func() bool {
			r := ex.TryClaim(128, &claim)
			if r > 0 {
				claim.Commit()
			}
			return check("TryClaim", r)
		})
		noop := func(*ab.Buffer, int32, int32, *logbuffer.Header) {}
		run(func() bool { sub.Poll(noop, 10); return !sub.IsClosed() })
		run(func() bool {
			ipcSub.ControlledPoll(func(*ab.Buffer, int32, int32, *logbuffer.Header) term.ControlledPollAction {
				return term.ControlledPollActionCommit
			}, 10)
			return !ipcSub.IsClosed()
		})
		run(func() bool {
			p, err := a.AddPublication("aeron:ipc", 9148)
			if err != nil {
				if !errors.Is(err, aeron.ErrClientClosed) {
					unexpected.Store(fmt.Sprintf("AddPublication: %v", err))
				}
				return false
			}
			_ = p.Close()
			return true
		})
		run(func() bool { _ = pub.Position(); _ = ex.PositionLimit(); return !ex.IsClosed() })

		time.Sleep(time.Duration(rand.Intn(20)) * time.Millisecond)
		require.NoError(t, a.Close())
		waitGroup(t, &wg, 5*time.Second)
		require.Nil(t, unexpected.Load())
	}
	leaks.requireNoLeaks(t)
}

func withTimeout(t *testing.T, what string, timeout time.Duration, f func()) {
	t.Helper()
	done := make(chan struct{})
	go func() { defer close(done); f() }()
	select {
	case <-done:
	case <-time.After(timeout):
		t.Fatalf("%s: no return within %v", what, timeout)
	}
}
