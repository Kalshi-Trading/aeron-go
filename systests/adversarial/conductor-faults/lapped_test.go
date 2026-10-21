package conductorfaults

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/lirm/aeron-go/aeron"
	"github.com/lirm/aeron-go/aeron/broadcast"
	"github.com/lirm/aeron-go/aeron/idlestrategy"
	"github.com/stretchr/testify/require"
)

// TestToClientBroadcastLapped fills the to-driver ring buffer with publication adds (whose ON_PUBLICATION_READY
// responses are about twice the size of the commands) while the driver is SIGSTOPped and the client conductor
// sleeps between duty cycles, so after SIGCONT the driver laps the client on the to-client broadcast buffer.
// Java ClientConductor.service then terminates the conductor and reports AeronException("Driver events adapter is
// invalid"); every resource is force closed, further API calls fail with client closed, the driver reclaims the
// client's counters and a fresh client works.
func TestToClientBroadcastLapped(t *testing.T) {
	setup(t)
	observer := connect(t, 10*time.Second)
	before := snapshot(t)
	c := connect(t, 20*time.Second, func(ctx *aeron.Context) {
		ctx.IdleStrategy(idlestrategy.Sleeping{SleepFor: time.Second})
	})
	stream := nextStream()
	sub, err := c.AddSubscription("aeron:ipc", stream)
	require.NoError(t, err)
	clientID := c.ClientID()

	signalDriver(t, "STOP")
	n := 0
	within(t, 10*time.Second, "fill ring buffer", func() {
		for ; n < 200_000; n++ {
			if _, err := c.AsyncAddPublication("aeron:ipc", stream); err != nil {
				return
			}
		}
	})
	signalDriver(t, "CONT")
	t.Logf("%d publication adds outstanding", n)

	awaitTrue(t, 20*time.Second, "client terminated", c.IsClosed)
	// The conductor marks the client closed before it releases its lock and runs the error handler, as Java's
	// terminateConductor precedes the AgentRunner error handler, so the report is awaited rather than assumed.
	var lapped error
	awaitTrue(t, 5*time.Second, "lapped error reported", func() bool {
		for _, e := range c.errs.all() {
			var aerr *aeron.AeronError
			if errors.As(e, &aerr) && strings.Contains(e.Error(), "Driver events adapter is invalid") {
				lapped = e
				return true
			}
		}
		return false
	})
	require.ErrorIs(t, lapped, broadcast.ErrUnableToKeepUp)
	require.True(t, sub.IsClosed())
	_, err = c.AddPublication("aeron:ipc", stream)
	require.ErrorIs(t, err, aeron.ErrClientClosed)
	require.NoError(t, c.Close())
	assertNoLeaks(t, before)
	awaitNoClientCounters(t, observer, clientID, 20*time.Second)
	requireIPCRoundTrip(t)
}
