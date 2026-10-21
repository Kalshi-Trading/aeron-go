package chaos

import (
	"errors"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lirm/aeron-go/aeron"
	ab "github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/logbuffer"
	"github.com/lirm/aeron-go/e2e"
	"github.com/stretchr/testify/require"
)

// TestChaosDriverKillMidStream kills the media driver while publishers, subscribers and blocking adds are hammering
// the client. The client must terminate with a DriverTimeoutError, every goroutine must observe closed resources and
// return (no hang, panic or SIGSEGV from unmapped log buffers), and after a restart a new client works.
func TestChaosDriverKillMidStream(t *testing.T) {
	requireChaos(t)
	restartStack(t)
	baseGoroutines := runtime.NumGoroutine()

	var handlerErrs atomic.Int32
	ctx := newContext(chaosDir).ErrorHandler(func(err error) { handlerErrs.Add(1) })
	a, err := aeron.Connect(ctx)
	require.NoError(t, err)

	channels := []string{"aeron:ipc", "aeron:udp?endpoint=localhost:24501", "aeron:udp?endpoint=localhost:24502|mtu=1408"}
	var subs []*aeron.Subscription
	var pubs []aeron.Publisher
	for i, ch := range channels {
		sub, err := a.AddSubscription(ch, int32(5001+i))
		require.NoError(t, err)
		pub, err := a.AddExclusivePublication(ch, int32(5001+i))
		require.NoError(t, err)
		require.NoError(t, e2e.WaitConnected(pub, 5*time.Second))
		subs = append(subs, sub)
		pubs = append(pubs, pub)
	}

	stop := make(chan struct{})
	var wg sync.WaitGroup
	var received, sent atomic.Int64
	for i := range channels {
		pub, sub := pubs[i], subs[i]
		size := 200
		if i == 2 {
			size = 6000 // fragmented
		}
		wg.Add(2)
		go func() {
			defer wg.Done()
			buf := ab.MakeBuffer(make([]byte, size))
			for {
				select {
				case <-stop:
					return
				default:
				}
				if r := pub.Offer(buf, 0, int32(size), nil); r > 0 {
					sent.Add(1)
				} else if r == aeron.PublicationClosed {
					return
				}
			}
		}()
		go func() {
			defer wg.Done()
			assembler := aeron.NewFragmentAssembler(func(*ab.Buffer, int32, int32, *logbuffer.Header) {
				received.Add(1)
			}, aeron.DefaultFragmentAssemblyBufferLength)
			for !sub.IsClosed() {
				sub.Poll(assembler.OnFragment, 64)
				select {
				case <-stop:
					return
				default:
				}
			}
		}()
	}
	// A goroutine issuing blocking driver commands throughout.
	var adds, addErrs atomic.Int32
	var lastAddErr atomic.Value
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; ; i++ {
			pub, err := a.AddPublication("aeron:ipc", 5100)
			if err != nil {
				addErrs.Add(1)
				lastAddErr.Store(errBox{err})
				if !a.IsClosed() {
					t.Logf("add error before close: %v", err)
				}
				if errors.Is(err, aeron.ErrClientClosed) {
					return
				}
				continue
			}
			adds.Add(1)
			_ = pub.Close()
		}
	}()

	awaitTrue(t, "traffic", 10*time.Second, func() bool { return received.Load() > 1000 && adds.Load() > 10 })
	docker(t, "kill", chaosDriver)
	killedAt := time.Now()
	t.Cleanup(func() { restartStack(t) })

	awaitTrue(t, "client terminated", 15*time.Second, a.IsClosed)
	t.Logf("client terminated %v after kill; sent=%d received=%d adds=%d", time.Since(killedAt), sent.Load(),
		received.Load(), adds.Load())
	var timeoutErr *aeron.DriverTimeoutError
	require.True(t, errors.As(a.TerminalError(), &timeoutErr), "terminal error: %v", a.TerminalError())

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		close(stop)
		t.Fatalf("workers did not observe the closed client")
	}
	for _, p := range pubs {
		require.True(t, p.IsClosed())
		require.Equal(t, aeron.PublicationClosed, p.Offer(ab.MakeBuffer(make([]byte, 8)), 0, 8, nil))
	}
	for _, s := range subs {
		require.True(t, s.IsClosed())
		require.Zero(t, s.Poll(func(*ab.Buffer, int32, int32, *logbuffer.Header) {}, 10))
	}
	require.ErrorIs(t, lastAddErr.Load().(errBox).err, aeron.ErrClientClosed)
	require.NoError(t, a.Close())
	// Like Java, the timeout reaches the error handler only when the conductor duty cycle (not a client API call
	// such as the adding goroutine) detects it.
	t.Logf("error handler calls: %d", handlerErrs.Load())
	require.Zero(t, mappedFiles(t, chaosDir), "log buffers / cnc still mapped after close")
	awaitGoroutines(t, baseGoroutines, 5*time.Second)

	// Restart and check a fresh client works on the new driver instance.
	restartStack(t)
	b := connect(t, chaosDir)
	sub, err := b.AddSubscription("aeron:ipc", 5001)
	require.NoError(t, err)
	pub, err := b.AddPublication("aeron:ipc", 5001)
	require.NoError(t, err)
	require.NoError(t, e2e.WaitConnected(pub, 5*time.Second))
	go func() { _ = e2e.Publish(pub, e2e.PublishOptions{Count: 100}) }()
	require.NoError(t, e2e.Consume(e2e.SubscriptionPoller(sub), e2e.NewSequenceChecker(0), 100, 10*time.Second))
}

type errBox struct{ err error }

// TestChaosDriverFastRestart restarts the driver faster than the client's driver timeout. The new driver deletes and
// recreates the aeron directory, so the client's mapped CnC no longer receives heartbeats and the client must still
// time out rather than hang, and blocking commands sent to the stale ring buffer must time out.
func TestChaosDriverFastRestart(t *testing.T) {
	requireChaos(t)
	restartStack(t)
	a, err := aeron.Connect(newContext(chaosDir))
	require.NoError(t, err)
	defer a.Close()
	_, err = a.AddSubscription("aeron:ipc", 5200)
	require.NoError(t, err)

	docker(t, "restart", "-t", "0", chaosDriver)
	t.Cleanup(func() { restartStack(t) })

	start := time.Now()
	_, err = a.AddPublication("aeron:ipc", 5200)
	require.Error(t, err, "command to a restarted driver must fail")
	require.Less(t, time.Since(start), 10*time.Second)
	t.Logf("add after restart: %v", err)
	awaitTrue(t, "client terminated", 15*time.Second, a.IsClosed)
	var timeoutErr *aeron.DriverTimeoutError
	require.True(t, errors.As(a.TerminalError(), &timeoutErr), "terminal error: %v", a.TerminalError())
}

// TestChaosInvokerStall uses the ConductorAgentInvoker and stops invoking it for longer than the driver's client
// liveness timeout (3s on chaos-driver). The next invocation must terminate the client with a timeout error.
func TestChaosInvokerStall(t *testing.T) {
	requireChaos(t)
	restartStack(t)
	var errs []error
	var mu sync.Mutex
	ctx := newContext(chaosDir).UseConductorAgentInvoker(true).ErrorHandler(func(err error) {
		mu.Lock()
		errs = append(errs, err)
		mu.Unlock()
	})
	a, err := aeron.Connect(ctx)
	require.NoError(t, err)
	defer a.Close()
	sub, err := a.AddSubscription("aeron:ipc", 5300)
	require.NoError(t, err)
	invoker := a.ConductorAgentInvoker()
	require.NotNil(t, invoker)
	for i := 0; i < 50; i++ {
		invoker.Invoke()
		time.Sleep(10 * time.Millisecond)
	}
	require.False(t, a.IsClosed())

	time.Sleep(5 * time.Second)
	deadline := time.Now().Add(5 * time.Second)
	for !a.IsClosed() && time.Now().Before(deadline) {
		invoker.Invoke()
		time.Sleep(10 * time.Millisecond)
	}
	require.True(t, a.IsClosed(), "stalled invoker client must terminate")
	require.Error(t, a.TerminalError())
	t.Logf("terminal error: %v", a.TerminalError())
	require.True(t, sub.IsClosed())
	_, err = a.AddPublication("aeron:ipc", 5300)
	require.ErrorIs(t, err, aeron.ErrClientClosed)
	mu.Lock()
	require.NotEmpty(t, errs, fmt.Sprint("error handler not called"))
	mu.Unlock()
}
