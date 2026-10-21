package drivercrash

import (
	"errors"
	"testing"
	"time"

	"github.com/lirm/aeron-go/aeron"
	aatomic "github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/logbuffer"
	"github.com/stretchr/testify/require"
)

// slack is the extra time allowed on top of a configured timeout before a call or detection counts as a hang.
const slack = 3 * time.Second

// TestDriverSIGKILLEveryClientKind SIGKILLs a plain and an archiving Java driver while one Go client runs every kind
// of publisher and subscriber at once, then checks everything Java 1.53.3 guarantees: the client detects the driver
// death within its driver timeout (DriverTimeoutException -> DriverTimeoutError), reports it exactly once through the
// error handler, runs the close handlers, closes every image with an unavailable-image callback, fails every later
// call promptly (Offer/TryClaim/OfferBlock return CLOSED, adds return an error), Close is safe and unmaps the CnC and
// log buffers, received data has no corruption, duplicate or gap, and a fresh client on the restarted driver (new
// CnC) resumes traffic with no leaked goroutine, fd or mapping.
func TestDriverSIGKILLEveryClientKind(t *testing.T) {
	requireAdv(t)
	for i, kind := range []driverKind{plain, archiving} {
		kind, base := kind, i
		t.Run(kind.service, func(t *testing.T) {
			startDriver(t, kind)
			base0 := snapshot(t)

			a, o := connect(t)
			closeHandlerCalls := 0
			a.AddCloseHandler(func() { closeHandlerCalls++ })
			var flows []*flow
			for _, s := range specs(base) {
				flows = append(flows, startFlow(t, a, s))
			}
			for _, f := range flows {
				f.awaitProgress(t, 200, 20*time.Second)
			}
			images := o.available.Load()
			require.Greater(t, mappedFiles(t), 1, "CnC and log buffers should be mapped")

			signalDriver(t, kind, "KILL")
			killed := time.Now()
			awaitTrue(t, "client to detect the driver death", driverTimeout+slack, a.IsClosed)
			t.Logf("driver death detected after %v", time.Since(killed))

			var dt *aeron.DriverTimeoutError
			require.True(t, errors.As(a.TerminalError(), &dt), "terminal error %v", a.TerminalError())
			errs := o.errors()
			require.Len(t, errs, 1, "the error handler must be called exactly once: %v", errs)
			require.Equal(t, "DriverTimeout", classify(errs[0]))
			require.EqualValues(t, 1, o.closes.Load(), "context close handler")
			require.Equal(t, 1, closeHandlerCalls, "AddCloseHandler handler")
			awaitTrue(t, "unavailable-image callbacks", time.Second, func() bool { return o.unavailable.Load() == images })

			for _, f := range flows {
				f.awaitExit(t, 2*time.Second)
				f.requireIntact(t)
				requireClosedPublication(t, f.pub)
				for _, s := range f.subs {
					require.True(t, s.IsClosed(), f.spec.name)
					require.Zero(t, s.ImageCount(), f.spec.name)
					require.Zero(t, s.Poll(func(*aatomic.Buffer, int32, int32, *logbuffer.Header) {}, 10), f.spec.name)
				}
			}
			requireClientCallsFailFast(t, a)

			within(t, "Close after driver death", 5*time.Second, func() { require.NoError(t, a.Close()) })
			within(t, "second Close", time.Second, func() { require.NoError(t, a.Close()) })
			require.Zero(t, mappedFiles(t), "CnC/log buffers still mapped after Close")
			require.Len(t, o.errors(), 1, "Close must not report more errors")

			restartDriver(t, kind)
			b, o2 := connect(t)
			var flows2 []*flow
			for _, s := range specs(base + 2) {
				flows2 = append(flows2, startFlow(t, b, s))
			}
			for _, f := range flows2 {
				f.awaitProgress(t, 500, 20*time.Second)
				f.stop()
				f.requireIntact(t)
			}
			require.NoError(t, b.Close())
			require.Empty(t, o2.errors())
			requireNoLeaks(t, base0)
		})
	}
}

// requireClosedPublication asserts every publish operation on a publication of a terminated client returns CLOSED.
func requireClosedPublication(t *testing.T, pub aeron.Publisher) {
	t.Helper()
	require.True(t, pub.IsClosed())
	require.False(t, pub.IsConnected())
	buf := aatomic.MakeBuffer(make([]byte, 64))
	require.Equal(t, aeron.PublicationClosed, pub.Offer(buf, 0, 64, nil))
	var claim logbuffer.Claim
	require.Equal(t, aeron.PublicationClosed, pub.TryClaim(64, &claim))
	if ex, ok := pub.(*aeron.ExclusivePublication); ok {
		require.Equal(t, aeron.PublicationClosed, ex.OfferBlock(buf, 0, 64))
	}
}

// requireClientCallsFailFast asserts that every client API call on a terminated client returns an error at once,
// like Java's ensureOpen ("Aeron client is closed"), instead of waiting for the dead driver.
func requireClientCallsFailFast(t *testing.T, a *aeron.Aeron) {
	t.Helper()
	calls := map[string]func() error{
		"AddPublication": func() error { _, err := a.AddPublication("aeron:ipc", 9099); return err },
		"AddExclusivePublication": func() error {
			_, err := a.AddExclusivePublication("aeron:ipc", 9099)
			return err
		},
		"AddSubscription": func() error { _, err := a.AddSubscription("aeron:ipc", 9099); return err },
		"AddCounter":      func() error { _, err := a.AddCounterByLabel(1001, "adv"); return err },
		"AsyncAddPublication": func() error {
			_, err := a.AsyncAddPublication("aeron:ipc", 9099)
			return err
		},
	}
	for name, call := range calls {
		var err error
		d := within(t, name+" after driver death", 2*time.Second, func() { err = call() })
		require.Error(t, err, name)
		require.Less(t, d, 100*time.Millisecond, "%s took %v", name, d)
	}
}
