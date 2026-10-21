package conductorfaults

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/lirm/aeron-go/aeron"
	"github.com/lirm/aeron-go/aeron/counters"
	"github.com/stretchr/testify/require"
)

// counterLog records available/unavailable counter notifications (registrationId -> counterId).
type counterLog struct {
	mu  sync.Mutex
	ids map[int64]int32
}

func newCounterLog() *counterLog { return &counterLog{ids: map[int64]int32{}} }

func (l *counterLog) Handle(_ *counters.Reader, registrationID int64, counterID int32) {
	l.mu.Lock()
	l.ids[registrationID] = counterID
	l.mu.Unlock()
}

func (l *counterLog) has(registrationID int64) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	_, ok := l.ids[registrationID]
	return ok
}

const advCounterType = 1098

// TestCountersAndHeartbeatAcrossDriverRestart checks the client heartbeat counter is allocated and advanced,
// counters added by one client are announced to the available/unavailable counter handlers of every client, and that
// a SIGKILLed driver (replaced by a fresh one) makes clients terminate with DriverTimeoutError within the driver
// timeout, close their counters, notify unavailable counter handlers and the close handler (Java
// ClientConductor.forceCloseResources), after which fresh clients get working counters and heartbeats again.
func TestCountersAndHeartbeatAcrossDriverRestart(t *testing.T) {
	setup(t)
	before := snapshot(t)
	const driverTimeout = 3 * time.Second

	obsAvail, obsUnavail := newCounterLog(), newCounterLog()
	observer := connect(t, driverTimeout, func(ctx *aeron.Context) {
		ctx.AvailableCounterHandler(obsAvail).UnavailableCounterHandler(obsUnavail)
	})
	ownUnavail := newCounterLog()
	var closes sync.WaitGroup
	closes.Add(1)
	var closeOnce sync.Once
	owner := connect(t, driverTimeout, func(ctx *aeron.Context) {
		ctx.UnavailableCounterHandler(ownUnavail).CloseHandler(func() { closeOnce.Do(closes.Done) })
	})

	reader := observer.CounterReader()
	require.Negative(t, heartbeatCounter(reader, owner.ClientID()), "no heartbeat counter before the first command")
	kept, err := owner.AddCounterByLabel(advCounterType, "adv-cf-kept")
	require.NoError(t, err)
	var hb int32 = -1
	awaitTrue(t, 5*time.Second, "heartbeat counter", func() bool {
		hb = heartbeatCounter(reader, owner.ClientID())
		return hb >= 0
	})
	first := reader.GetCounterValue(hb)
	awaitTrue(t, 5*time.Second, "heartbeat advances", func() bool { return reader.GetCounterValue(hb) > first })

	closed, err := owner.AddCounterByLabel(advCounterType, "adv-cf-closed")
	require.NoError(t, err)
	asyncID, err := owner.AsyncAddCounterByLabel(advCounterType, "adv-cf-async")
	require.NoError(t, err)
	var async *aeron.Counter
	awaitTrue(t, 5*time.Second, "async counter", func() bool {
		async, err = owner.GetCounter(asyncID)
		require.NoError(t, err)
		return async != nil
	})
	for _, c := range []*aeron.Counter{kept, closed, async} {
		c.Counter().Set(42)
		reg := c.RegistrationID()
		awaitTrue(t, 5*time.Second, "observer sees available counter", func() bool { return obsAvail.has(reg) })
		require.Equal(t, int64(42), reader.GetCounterValue(c.ID()))
	}
	require.NoError(t, closed.Close())
	awaitTrue(t, 5*time.Second, "observer sees unavailable counter", func() bool {
		return obsUnavail.has(closed.RegistrationID())
	})

	restartDriver(t)
	start := time.Now()
	awaitTrue(t, driverTimeout+5*time.Second, "owner terminated", owner.IsClosed)
	t.Logf("owner terminated %v after driver restart", time.Since(start))
	awaitTrue(t, 5*time.Second, "driver timeout reported", func() bool {
		var dte *aeron.DriverTimeoutError
		return owner.errs.find(&dte)
	})
	closes.Wait()
	require.True(t, kept.IsClosed())
	require.True(t, async.IsClosed())
	awaitTrue(t, 5*time.Second, "unavailable counter handlers on force close", func() bool {
		return ownUnavail.has(kept.RegistrationID()) && ownUnavail.has(async.RegistrationID())
	})
	_, err = owner.AddCounterByLabel(advCounterType, "adv-cf-after")
	require.True(t, errors.Is(err, aeron.ErrClientClosed), "%v", err)
	require.NoError(t, owner.Close())
	awaitTrue(t, driverTimeout+5*time.Second, "observer terminated", observer.IsClosed)
	require.NoError(t, observer.Close())

	avail2 := newCounterLog()
	observer2 := connect(t, 5*time.Second, func(ctx *aeron.Context) { ctx.AvailableCounterHandler(avail2) })
	owner2 := connect(t, 5*time.Second)
	reader2 := observer2.CounterReader()
	c2, err := owner2.AddCounterByLabel(advCounterType, "adv-cf-resumed")
	require.NoError(t, err)
	awaitTrue(t, 5*time.Second, "new heartbeat counter", func() bool {
		return heartbeatCounter(reader2, owner2.ClientID()) >= 0
	})
	awaitTrue(t, 5*time.Second, "new observer sees counter", func() bool { return avail2.has(c2.RegistrationID()) })
	owner2ID := owner2.ClientID()
	require.NoError(t, owner2.Close())
	awaitNoClientCounters(t, observer2, owner2ID, 10*time.Second)
	require.NoError(t, observer2.Close())
	assertNoLeaks(t, before)
}
