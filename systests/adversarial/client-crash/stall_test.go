package clientcrash

import (
	"errors"
	osexec "os/exec"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/lirm/aeron-go/aeron"
	ab "github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/logbuffer"
	"github.com/lirm/aeron-go/e2e"
	"github.com/stretchr/testify/require"
)

// TestStallClientProcess SIGSTOPs a Go client process that is hot-looping Offer and Poll, then SIGCONTs it. While it
// is stopped past the client liveness timeout the driver must reclaim it. On resume, as in Java 1.53.3 (whose
// inter-service timeout is the driver's client liveness timeout from the CnC file and whose conductor checks the
// service interval before reading driver events), the conductor must terminate with a ConductorServiceTimeout
// error reported once through the error handler, the close handler must run once, every resource must report
// closed, Offer must return PublicationClosed, AddPublication ErrClientClosed, both Closes must succeed, no mapping
// or goroutine may be left, and a new client in the same process must work. A stop shorter than the timeout must
// be survived without any error or gap.
func TestStallClientProcess(t *testing.T) {
	for _, c := range []struct{ name, channel, subChan string }{
		{"ipc", "aeron:ipc?alias=advk-stall-ipc", "aeron:ipc"},
		{"udp", "aeron:udp?endpoint=localhost:27133|alias=advk-stall-udp", "aeron:udp?endpoint=localhost:27133"},
	} {
		t.Run(c.name, func(t *testing.T) {
			requireAdv(t)
			a := connect(t, newContext())
			watcher := newLiveSub(t, a, c.subChan, 9131)
			p := startPeer(t, "stalled", "-mode", "stall", "-channel", c.channel, "-stream", "9131",
				"-kind", "exclusive", "-interval", "100us")
			ready := p.await("ready", 30*time.Second)
			session, clientID := int32(ready.int("sessionId")), ready.int("clientId")

			// A pause shorter than the liveness timeout is survived.
			p.signal(syscall.SIGSTOP)
			time.Sleep(livenessTO / 2)
			p.signal(syscall.SIGCONT)
			before := watcher.checker(session).Next
			watcher.awaitReceived(session, before+500, 10*time.Second)
			require.Nil(t, p.last("terminated"))
			require.NotEmpty(t, clientCounters(a, clientID))

			p.signal(syscall.SIGSTOP)
			awaitReclaimed(t, a, clientID, alias(c.channel))
			watcher.awaitUnavailable(session, reclaimTO)
			// Stay stopped past the inter-service timeout too: a resume just after the driver's reclaim can see the
			// freed heartbeat counter before the service interval is exceeded (the same race exists in Java).
			time.Sleep(time.Second)
			p.signal(syscall.SIGCONT)
			term := p.await("terminated", 5*time.Second)
			t.Logf("terminated: %v", term)
			require.Equal(t, "ConductorServiceTimeoutError", term.str("terminalType"))
			handled, _ := term["handled"].([]any)
			require.Len(t, handled, 1, "errors reported to the handler: %v", handled)
			require.Contains(t, handled[0], "ConductorServiceTimeoutError")
			require.EqualValues(t, 1, term.int("closeHandlerCalls"))
			require.Equal(t, true, term["addErrIsClientClosed"])
			require.EqualValues(t, aeron.PublicationClosed, term.int("offerAfter"))
			require.Equal(t, true, term["pubClosed"])
			require.Equal(t, true, term["subClosed"])
			require.Equal(t, "<nil>", term.str("closeErr"))
			require.Equal(t, "<nil>", term.str("secondCloseErr"))
			require.Zero(t, term.int("mapped"))
			p.await("recovered", 30*time.Second)
			require.NoError(t, p.waitExit(10*time.Second))
			require.NoError(t, ptr(watcher.checker(session)).Err())
		})
	}
}

// TestConductorCallbackPause blocks the conductor goroutine (a GC-like pause inside an available-image handler)
// past the client liveness timeout while other goroutines keep offering and polling. As in Java, the driver must
// reclaim the client, and once the callback returns the client must terminate with a ConductorServiceTimeout error
// through the error handler, without panicking; the blocked application goroutines must observe the close and a new
// client must work. Nothing may leak.
func TestConductorCallbackPause(t *testing.T) {
	requireAdv(t)
	leaks := baseline()
	observer := connect(t, newContext())

	var mu sync.Mutex
	var handled []error
	closed := make(chan struct{})
	ctx := newContext().ErrorHandler(func(err error) {
		mu.Lock()
		handled = append(handled, err)
		mu.Unlock()
	}).CloseHandler(func() { close(closed) })
	a := connect(t, ctx)
	const channel = "aeron:ipc?alias=advk-cb-pause"
	var pauseOnce sync.Once
	paused := make(chan struct{})
	sub, err := a.AddSubscriptionWithHandlers(channel, 9132, func(aeron.Image) {
		pauseOnce.Do(func() {
			close(paused)
			time.Sleep(livenessTO + 2*time.Second)
		})
	}, nil)
	require.NoError(t, err)
	pub, err := a.AddExclusivePublication(channel, 9132)
	require.NoError(t, err)
	<-paused

	var wg sync.WaitGroup
	wg.Add(2)
	var lastOffer int64
	go func() {
		defer wg.Done()
		buf := ab.MakeBuffer(make([]byte, 64))
		for !pub.IsClosed() {
			lastOffer = pub.Offer(buf, 0, 64, nil)
			time.Sleep(10 * time.Microsecond)
		}
		lastOffer = pub.Offer(buf, 0, 64, nil)
	}()
	go func() {
		defer wg.Done()
		for !sub.IsClosed() {
			sub.Poll(func(*ab.Buffer, int32, int32, *logbuffer.Header) {}, 10)
		}
	}()

	awaitReclaimed(t, observer, a.ClientID(), "advk-cb-pause")
	select {
	case <-closed:
	case <-time.After(livenessTO + 5*time.Second):
		t.Fatal("client did not close after the conductor pause")
	}
	require.True(t, a.IsClosed())
	var cst *aeron.ConductorServiceTimeoutError
	require.True(t, errors.As(a.TerminalError(), &cst), "terminal error %T %v", a.TerminalError(), a.TerminalError())
	waitGroup(t, &wg, 5*time.Second)
	require.EqualValues(t, aeron.PublicationClosed, lastOffer)
	mu.Lock()
	require.Len(t, handled, 1, "%v", handled)
	require.True(t, errors.As(handled[0], &cst))
	mu.Unlock()
	require.NoError(t, a.Close())
	require.NoError(t, a.Close())

	b := connect(t, newContext())
	roundTrip(t, b, "aeron:ipc", 9133)
	require.NoError(t, b.Close())
	require.NoError(t, observer.Close())
	leaks.requireNoLeaks(t)
}

// TestDriverStopped SIGSTOPs the Java driver process (docker kill -s STOP). Past the client's driver timeout, as in
// Java, the client must detect the dead driver itself (DriverTimeout error through the handler), close, and unblock
// Offer/Poll callers, while the driver is still stopped. Stopped for longer than the client liveness timeout but less
// than the driver timeout, the client must survive: Java clients heartbeat by writing their heartbeat counter
// directly (not through the stopped driver's command queue), so the resumed driver (AeronClient.onTimeEvent) sees a
// fresh heartbeat and no one times out; the existing publication and subscription must keep working. After SIGCONT
// a new client must work and nothing may leak.
func TestDriverStopped(t *testing.T) {
	for _, c := range []struct {
		name     string
		stop     time.Duration
		expected string
	}{
		{"past-client-liveness", livenessTO + time.Second, ""},
		{"past-driver-timeout", 8 * time.Second, "DriverTimeoutError"},
	} {
		t.Run(c.name, func(t *testing.T) {
			requireAdv(t)
			leaks := baseline()
			var mu sync.Mutex
			var handled []error
			closed := make(chan struct{})
			ctx := newContext().ErrorHandler(func(err error) {
				mu.Lock()
				handled = append(handled, err)
				mu.Unlock()
			}).CloseHandler(func() { close(closed) })
			a := connect(t, ctx)
			sub, err := a.AddSubscription("aeron:udp?endpoint=localhost:27131", 9134)
			require.NoError(t, err)
			pub, err := a.AddPublication("aeron:udp?endpoint=localhost:27131", 9134)
			require.NoError(t, err)
			require.NoError(t, e2e.WaitConnected(pub, 10*time.Second))

			t.Cleanup(func() { _ = osexec.Command("docker", "kill", "-s", "CONT", advContainer).Run() })
			docker(t, "kill", "-s", "STOP", advContainer)
			stopped := time.Now()
			if c.expected == "DriverTimeoutError" {
				select {
				case <-closed:
				case <-time.After(c.stop):
					t.Fatal("client did not detect the stopped driver")
				}
				t.Logf("closed %v after the driver stopped", time.Since(stopped))
				buf := ab.MakeBuffer(make([]byte, 64))
				require.EqualValues(t, aeron.PublicationClosed, pub.Offer(buf, 0, 64, nil))
				require.Zero(t, sub.Poll(func(*ab.Buffer, int32, int32, *logbuffer.Header) {}, 10))
				time.Sleep(time.Until(stopped.Add(c.stop)))
				docker(t, "kill", "-s", "CONT", advContainer)
			} else {
				time.Sleep(c.stop)
				docker(t, "kill", "-s", "CONT", advContainer)
				require.NoError(t, e2e.Publish(pub, e2e.PublishOptions{Count: 500, Size: 64, Timeout: 10 * time.Second}))
				checker := e2e.NewSequenceChecker(0)
				require.NoError(t, e2e.Consume(e2e.ReassemblingPoller(sub), checker, 500, 10*time.Second), checker.String())
				time.Sleep(livenessTO)
				require.False(t, a.IsClosed())
				mu.Lock()
				require.Empty(t, handled)
				mu.Unlock()
				roundTrip(t, a, "aeron:ipc", 9136)
				require.NoError(t, a.Close())
				leaks.requireNoLeaks(t)
				return
			}
			mu.Lock()
			require.NotEmpty(t, handled)
			require.Equal(t, c.expected, errName(handled[0]), "%v", handled)
			mu.Unlock()
			require.Equal(t, c.expected, errName(a.TerminalError()))
			require.True(t, pub.IsClosed())
			require.True(t, sub.IsClosed())
			require.NoError(t, a.Close())

			b := connect(t, newContext())
			roundTrip(t, b, "aeron:udp?endpoint=localhost:27132", 9135)
			require.NoError(t, b.Close())
			leaks.requireNoLeaks(t)
		})
	}
}

func errName(err error) string {
	var cst *aeron.ConductorServiceTimeoutError
	var ct *aeron.ClientTimeoutError
	var dt *aeron.DriverTimeoutError
	switch {
	case errors.As(err, &cst):
		return "ConductorServiceTimeoutError"
	case errors.As(err, &ct):
		return "ClientTimeoutError"
	case errors.As(err, &dt):
		return "DriverTimeoutError"
	case err == nil:
		return "<nil>"
	}
	return err.Error()
}

func waitGroup(t *testing.T, wg *sync.WaitGroup, timeout time.Duration) {
	t.Helper()
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(timeout):
		t.Fatal("application goroutines did not observe the closed client")
	}
}

// roundTrip publishes and consumes 500 sequenced messages on a fresh publication/subscription of a.
func roundTrip(t *testing.T, a *aeron.Aeron, channel string, stream int32) {
	t.Helper()
	sub, err := a.AddSubscription(channel, stream)
	require.NoError(t, err)
	pub, err := a.AddExclusivePublication(channel, stream)
	require.NoError(t, err)
	require.NoError(t, e2e.WaitConnected(pub, 10*time.Second))
	errs := make(chan error, 1)
	go func() { errs <- e2e.Publish(pub, e2e.PublishOptions{Count: 500, Size: 64, Timeout: 10 * time.Second}) }()
	checker := e2e.NewSequenceChecker(0)
	require.NoError(t, e2e.Consume(e2e.ReassemblingPoller(sub), checker, 500, 10*time.Second), checker.String())
	require.NoError(t, <-errs)
	require.NoError(t, pub.Close())
	require.NoError(t, sub.Close())
}
