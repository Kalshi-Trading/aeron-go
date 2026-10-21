package drivercrash

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/lirm/aeron-go/aeron"
	"github.com/stretchr/testify/require"
)

// TestDriverDeathDuringAdds kills or freezes the driver while several goroutines continuously add and close
// publications, exclusive publications, subscriptions and counters. Like Java, a command waiting for a driver response
// fails with DriverTimeoutException (DriverTimeoutError) no later than the driver timeout, the client then terminates,
// every later call fails at once, nothing panics or hangs, and a fresh client on the restarted (or resumed) driver can
// add resources again.
func TestDriverDeathDuringAdds(t *testing.T) {
	requireAdv(t)
	for _, fault := range []string{"KILL", "STOP"} {
		fault := fault
		t.Run(fault, func(t *testing.T) {
			startDriver(t, plain)
			base := snapshot(t)
			a, o := connect(t)

			var calls, failures, timeoutsReturned atomic.Int64
			var mu sync.Mutex
			var unexpected []error
			var slowest time.Duration
			stop := make(chan struct{})
			var wg sync.WaitGroup
			adders := func(c *aeron.Aeron) []func(i int) error {
				return []func(i int) error{
					func(i int) error {
						p, err := c.AddPublication("aeron:udp?endpoint="+udp(20+i%4), int32(9020+i%4))
						if err == nil {
							err = p.Close()
						}
						return err
					},
					func(i int) error {
						p, err := c.AddExclusivePublication("aeron:ipc", int32(9024+i%4))
						if err == nil {
							err = p.Close()
						}
						return err
					},
					func(i int) error {
						s, err := c.AddSubscription("aeron:udp?endpoint="+udp(24+i%4), int32(9028+i%4))
						if err == nil {
							err = s.Close()
						}
						return err
					},
					func(i int) error {
						c, err := c.AddCounterByLabel(1001, fmt.Sprint("adv-", i))
						if err == nil {
							err = c.Close()
						}
						return err
					},
				}
			}
			for g, add := range adders(a) {
				g, add := g, add
				wg.Add(1)
				go func() {
					defer wg.Done()
					for i := 0; ; i++ {
						select {
						case <-stop:
							return
						default:
						}
						start := time.Now()
						err := add(i)
						d := time.Since(start)
						calls.Add(1)
						mu.Lock()
						if d > slowest {
							slowest = d
						}
						if err != nil {
							failures.Add(1)
							var dt *aeron.DriverTimeoutError
							if errors.As(err, &dt) {
								timeoutsReturned.Add(1)
							} else if !errors.Is(err, aeron.ErrClientClosed) {
								unexpected = append(unexpected, fmt.Errorf("adder %d: %w", g, err))
							}
						}
						mu.Unlock()
						if a.IsClosed() {
							return
						}
					}
				}()
			}
			awaitTrue(t, "adds in flight", 10*time.Second, func() bool { return calls.Load() > 200 })
			signalDriver(t, plain, fault)
			awaitTrue(t, "client terminated", driverTimeout+slack, a.IsClosed)
			done := make(chan struct{})
			go func() { wg.Wait(); close(done) }()
			select {
			case <-done:
			case <-time.After(driverTimeout + slack):
				close(stop)
				t.Fatal("add calls still blocked after the client terminated")
			}
			mu.Lock()
			t.Logf("%d adds, %d failed, slowest %v", calls.Load(), failures.Load(), slowest)
			require.Empty(t, unexpected)
			require.Less(t, slowest, driverTimeout+slack, "an add blocked longer than the driver timeout")
			mu.Unlock()
			require.Positive(t, failures.Load())
			var dt *aeron.DriverTimeoutError
			require.True(t, errors.As(a.TerminalError(), &dt), "%v", a.TerminalError())
			// Java ClientConductor.service rethrows a timeout detected inside a client API call to that caller only;
			// the error handler sees it only when the conductor thread detects it first. Either way it surfaces once.
			for _, err := range o.errors() {
				require.Equal(t, "DriverTimeout", classify(err), "%v", err)
			}
			require.Positive(t, int64(len(o.errors()))+timeoutsReturned.Load(), "the driver timeout was never reported")
			t.Logf("timeout returned to %d callers, error handler calls %d", timeoutsReturned.Load(), len(o.errors()))
			requireClientCallsFailFast(t, a)
			within(t, "Close", 5*time.Second, func() { require.NoError(t, a.Close()) })
			require.Zero(t, mappedFiles(t))

			if fault == "STOP" {
				// The frozen driver resumes with its old CnC; a fresh client must work on it.
				signalDriver(t, plain, "CONT")
			} else {
				restartDriver(t, plain)
			}
			b, o2 := connect(t)
			for i, add := range adders(b) {
				require.NoError(t, add(100+i), "adder %d after %s", i, fault)
			}
			f := startFlow(t, b, specs(5)[0])
			f.awaitProgress(t, 500, 10*time.Second)
			f.stop()
			f.requireIntact(t)
			require.NoError(t, b.Close())
			require.Empty(t, o2.errors())
			requireNoLeaks(t, base)
		})
	}
}

// TestDriverDeathDuringBackPressure SIGKILLs the driver while a concurrent and an exclusive publication are back
// pressured by a subscriber that stopped polling (Offer, TryClaim and OfferBlock spinning on BACK_PRESSURED). Every
// spinning call must turn into CLOSED within the driver timeout, the stalled subscriber's image must be closed with a
// callback, and after a restart the same streams flow again without gaps.
func TestDriverDeathDuringBackPressure(t *testing.T) {
	requireAdv(t)
	startDriver(t, plain)
	base := snapshot(t)
	a, o := connect(t)
	var flows []*flow
	for _, s := range []flowSpec{
		{name: "concurrent-udp", pubChannel: "aeron:udp?endpoint=" + udp(40) + "|term-length=64k", stream: 9050,
			mode: offer, size: 1024},
		{name: "exclusive-ipc-offerblock", pubChannel: "aeron:ipc?term-length=64k", stream: 9051, exclusive: true,
			mode: offerBlock, size: 992},
		{name: "exclusive-udp-tryclaim", pubChannel: "aeron:udp?endpoint=" + udp(41) + "|term-length=64k",
			stream: 9052, exclusive: true, mode: tryClaim, size: 1024},
	} {
		f := startFlow(t, a, s)
		f.awaitProgress(t, 50, 10*time.Second)
		// Stop the subscribers only: the publisher keeps offering into a full window.
		f.stopSubscribers()
		flows = append(flows, f)
	}
	for _, f := range flows {
		f := f
		awaitTrue(t, f.spec.name+" back pressured", 10*time.Second, func() bool {
			return f.lastRes.Load() == aeron.BackPressured
		})
	}
	sent := make([]int64, len(flows))
	for i, f := range flows {
		sent[i] = f.sent.Load()
	}
	time.Sleep(200 * time.Millisecond)
	for i, f := range flows {
		require.Equal(t, sent[i], f.sent.Load(), "%s should be fully back pressured", f.spec.name)
	}

	signalDriver(t, plain, "KILL")
	killed := time.Now()
	for _, f := range flows {
		f.awaitPublisherExit(t, driverTimeout+slack)
		require.Equal(t, aeron.PublicationClosed, f.lastRes.Load(), f.spec.name)
		requireClosedPublication(t, f.pub)
	}
	t.Logf("back-pressured offers returned CLOSED %v after the kill", time.Since(killed))
	require.Equal(t, o.available.Load(), o.unavailable.Load())
	require.NoError(t, a.Close())
	require.Zero(t, mappedFiles(t))

	restartDriver(t, plain)
	b, _ := connect(t)
	for _, f := range flows {
		f2 := startFlow(t, b, f.spec)
		f2.awaitProgress(t, 2000, 20*time.Second)
		f2.stop()
		f2.requireIntact(t)
	}
	require.NoError(t, b.Close())
	requireNoLeaks(t, base)
}

// TestGoClientSIGKILLDuringAdds SIGKILLs a Go client process while it is adding and removing publications and
// subscriptions in a loop: the driver must release everything the dead client owned (its heartbeat, publisher limit
// and subscriber position counters) after the client liveness timeout.
func TestGoClientSIGKILLDuringAdds(t *testing.T) {
	requireAdv(t)
	startDriver(t, plain)
	a, o := connect(t)
	reader := a.CounterReader()
	for round := 0; round < 3; round++ {
		p := startGoPeer(t, goArgs(map[string]string{"role": "adds", "channel": "aeron:udp?endpoint=" + udp(45),
			"stream": "9055", "interval": "0s"})...)
		client := p.await("ready", 10*time.Second).int("clientId")
		awaitTrue(t, "peer adding", 10*time.Second, func() bool {
			e := p.last("progress")
			return e != nil && e.int("sent") > 50
		})
		p.signal(syscall.SIGKILL)
		require.True(t, p.exited(5*time.Second))
		// Java driver: the client goes after the liveness timeout, its network publications after the
		// publication linger timeout (default 5s) on top of that.
		awaitTrue(t, "driver releases the dead client's heartbeat", clientLiveness+slack, func() bool {
			return heartbeatCount(reader, client) == 0
		})
		awaitTrue(t, "driver releases the dead client's publications and subscriptions",
			publicationLinger+slack, func() bool {
				return countCounters(reader, pubLimitTypeID, "9055") == 0 &&
					countCounters(reader, subPosTypeID, "9055") == 0
			})
	}
	require.Empty(t, o.errors())
	// The same stream is usable afterwards.
	f := startFlow(t, a, flowSpec{name: "after", pubChannel: "aeron:udp?endpoint=" + udp(45), stream: 9055,
		mode: offer, size: 64})
	f.awaitProgress(t, 500, 10*time.Second)
	f.stop()
	f.requireIntact(t)
}
