package chaos

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/lirm/aeron-go/aeron"
	ab "github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/e2e"
	"github.com/stretchr/testify/require"
)

// publishAll offers count sequenced messages of size bytes, retrying back pressure, and returns how many offers were
// back pressured.
func publishAll(t *testing.T, pub aeron.Publisher, count int64, size func(seq int64) int, timeout time.Duration) int64 {
	t.Helper()
	deadline := time.Now().Add(timeout)
	buf := make([]byte, 4<<20)
	abuf := ab.MakeBuffer(buf)
	var backPressured int64
	for seq := int64(0); seq < count; {
		n := size(seq)
		e2e.EncodeMessage(buf[:n], seq)
		r := pub.Offer(abuf, 0, int32(n), nil)
		switch {
		case r > 0:
			seq++
		case r == aeron.BackPressured || r == aeron.AdminAction || r == aeron.NotConnected:
			if r == aeron.BackPressured {
				backPressured++
			}
			if time.Now().After(deadline) {
				t.Fatalf("publisher stuck at seq %d: last result %d", seq, r)
			}
		default:
			t.Fatalf("offer seq %d: %d", seq, r)
		}
	}
	return backPressured
}

func fixedSize(n int) func(int64) int { return func(int64) int { return n } }

// TestChaosSlowConsumerBackPressure: a consumer that starts slowly must back pressure the publisher without losing,
// duplicating or corrupting any message, on IPC and UDP.
func TestChaosSlowConsumerBackPressure(t *testing.T) {
	requireChaos(t)
	restartStack(t)
	a := connect(t, chaosDir)
	for i, channel := range []string{"aeron:ipc?term-length=65536", "aeron:udp?endpoint=localhost:24530|term-length=65536"} {
		t.Run(channel, func(t *testing.T) {
			const count = 30000
			sub, err := a.AddSubscription(channel, int32(5500+i))
			require.NoError(t, err)
			defer sub.Close()
			pub, err := a.AddExclusivePublication(channel, int32(5500+i))
			require.NoError(t, err)
			defer pub.Close()
			require.NoError(t, e2e.WaitConnected(pub, 5*time.Second))

			checker := e2e.NewSequenceChecker(0)
			done := make(chan error, 1)
			go func() {
				slowUntil := time.Now().Add(2 * time.Second)
				handler := checker.Handler()
				for time.Now().Before(slowUntil) {
					sub.Poll(handler, 1)
					time.Sleep(time.Millisecond)
				}
				done <- e2e.Consume(e2e.SubscriptionPoller(sub), checker, count, 30*time.Second)
			}()
			bp := publishAll(t, pub, count, fixedSize(1000), 60*time.Second)
			require.NoError(t, <-done)
			t.Logf("back pressured %d offers; %s", bp, checker)
			require.Positive(t, bp, "slow consumer should back pressure the publisher")
			require.Equal(t, int64(count), checker.Received)
		})
	}
}

// TestChaosStalledUntetheredSubscriber: an untethered IPC subscriber that stops polling is repeatedly moved to resting
// (unavailable image) and rejoined (a new image with the same correlation id) by the driver. The client must track
// every cycle without leaking mappings, the tethered subscriber must see every published message, and the stalled
// subscriber may skip data but must never see corrupt, duplicate or out-of-order messages.
func TestChaosStalledUntetheredSubscriber(t *testing.T) {
	requireChaos(t)
	restartStack(t)
	a, err := aeron.Connect(newContext(chaosDir).ResourceLingerTimeout(200 * time.Millisecond))
	require.NoError(t, err)
	defer a.Close()
	const streamID = 5510
	fast, err := a.AddSubscription("aeron:ipc", streamID)
	require.NoError(t, err)
	baseMapped := mappedFiles(t, chaosDir)
	var available, unavailable atomic.Int32
	stalled, err := a.AddSubscriptionWithHandlers("aeron:ipc?tether=false", streamID,
		func(aeron.Image) { available.Add(1) }, func(aeron.Image) { unavailable.Add(1) })
	require.NoError(t, err)
	pub, err := a.AddExclusivePublication("aeron:ipc?term-length=65536", streamID)
	require.NoError(t, err)
	require.NoError(t, e2e.WaitConnected(pub, 5*time.Second))
	awaitTrue(t, "stalled image", 5*time.Second, func() bool { return stalled.ImageCount() == 1 })

	checker := e2e.NewSequenceChecker(0)
	var published atomic.Int64
	stop := make(chan struct{})
	pubDone := make(chan struct{})
	go func() {
		defer close(pubDone)
		buf := make([]byte, 512)
		abuf := ab.MakeBuffer(buf)
		for seq := int64(0); ; {
			select {
			case <-stop:
				return
			default:
			}
			e2e.EncodeMessage(buf, seq)
			if pub.Offer(abuf, 0, 512, nil) > 0 {
				seq++
				published.Store(seq)
			}
			time.Sleep(20 * time.Microsecond)
		}
	}()
	stalledChecker := e2e.NewSequenceChecker(0)
	handler := checker.Handler()
	awaitTrue(t, "stalled subscriber rested, rejoined and received data", 30*time.Second, func() bool {
		fast.Poll(handler, 100)
		if unavailable.Load() >= 3 {
			stalled.Poll(stalledChecker.Handler(), 100)
		}
		return available.Load() >= 4 && stalledChecker.Received > 100
	})
	close(stop)
	<-pubDone
	require.NoError(t, e2e.Consume(e2e.SubscriptionPoller(fast), checker, published.Load(), 10*time.Second))
	t.Logf("published %d; fast %s; stalled %s; available=%d unavailable=%d", published.Load(), checker,
		stalledChecker, available.Load(), unavailable.Load())
	require.Equal(t, published.Load(), checker.Received)
	require.Zero(t, stalledChecker.Corrupt)
	require.Zero(t, stalledChecker.Duplicates)

	require.NoError(t, stalled.Close())
	// cnc.dat plus the log mapped once by the publication and once by the tethered subscriber's image: every rejoined
	// image of the stalled subscriber must have been unmapped.
	awaitTrue(t, "rejoined image mappings released", 10*time.Second, func() bool {
		return mappedFiles(t, chaosDir) == baseMapped+2
	})
}
