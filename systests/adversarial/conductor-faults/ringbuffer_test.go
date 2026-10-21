package conductorfaults

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/lirm/aeron-go/aeron"
	"github.com/stretchr/testify/require"
)

// TestToDriverRingBufferFull SIGSTOPs the driver and issues async subscription adds until the to-driver ring
// buffer is full. Java DriverProxy throws AeronException("failed to write ...") (category ERROR) without
// terminating the client. Half of the outstanding adds are cancelled while the driver is still stopped; after
// SIGCONT every remaining add completes, every cancelled one is released by the driver, and all driver counters of
// the client are freed. Subscriptions are used because their responses are smaller than their commands, so the
// burst of responses fits the to-client broadcast buffer (see TestToClientBroadcastLapped for the opposite).
func TestToDriverRingBufferFull(t *testing.T) {
	setup(t)
	observer := connect(t, 10*time.Second)
	before := snapshot(t)
	c := connect(t, 20*time.Second)
	stream, ringStream := nextStream(), nextStream()
	sub, err := c.AddSubscription("aeron:ipc", stream)
	require.NoError(t, err)

	signalDriver(t, "STOP")
	var ids []int64
	var fullErr error
	within(t, 10*time.Second, "fill ring buffer", func() {
		for i := 0; i < 200_000; i++ {
			// distinct channels so every add creates its own driver publication
			id, err := c.AsyncAddSubscription(fmt.Sprintf("aeron:ipc?alias=adv-ring-%d", i), ringStream)
			if err != nil {
				fullErr = err
				return
			}
			ids = append(ids, id)
		}
	})
	t.Logf("ring buffer full after %d commands: %T %v", len(ids), fullErr, fullErr)
	require.Error(t, fullErr, "to-driver ring buffer never filled")
	require.Greater(t, len(ids), 1000)
	var aerr *aeron.AeronError
	require.Truef(t, errors.As(fullErr, &aerr), "want AeronError, got %T %v", fullErr, fullErr)
	require.Equal(t, aeron.CategoryError, aerr.Category)
	require.Contains(t, fullErr.Error(), "failed to write")
	require.False(t, c.IsClosed(), "a full ring buffer must not terminate the client")
	_, err = c.AsyncAddSubscription(fmt.Sprintf("aeron:ipc?alias=adv-ring-%d", len(ids)), ringStream)
	require.True(t, errors.As(err, &aerr), "still full: %T %v", err, err)

	cancelled := ids[:len(ids)/2]
	pending := ids[len(ids)/2:]
	cancelWrites := 0
	for _, id := range cancelled {
		// The buffer is full, so some removes cannot be written; those are retried after SIGCONT.
		if c.AsyncRemoveSubscription(id) == nil {
			cancelWrites++
		}
	}
	signalDriver(t, "CONT")

	subs := make([]*aeron.Subscription, 0, len(pending))
	var pollErr error
	within(t, 60*time.Second, "pending adds complete", func() {
		for _, id := range pending {
			for {
				s, err := c.GetSubscription(id)
				if err != nil {
					pollErr = err
					return
				}
				if s != nil {
					subs = append(subs, s)
					break
				}
				time.Sleep(time.Millisecond)
			}
		}
	})
	require.NoError(t, pollErr)
	t.Logf("%d adds completed, %d cancelled (%d removes written while full)", len(subs), len(cancelled), cancelWrites)
	for _, id := range cancelled {
		s, _ := c.GetSubscription(id)
		require.Nil(t, s, "cancelled add %d resolved to a subscription", id)
	}
	awaitTrue(t, 30*time.Second, "no active commands", func() bool { return !c.HasActiveCommands() })
	require.False(t, c.IsClosed())

	pub, err := c.AddPublication("aeron:ipc", stream)
	require.NoError(t, err)
	roundTrip(t, pub, sub, 100)
	for _, s := range subs {
		require.NoError(t, s.Close())
	}
	clientID := c.ClientID()
	require.NoError(t, c.Close())
	assertNoLeaks(t, before)
	awaitNoClientCounters(t, observer, clientID, 20*time.Second)
}
