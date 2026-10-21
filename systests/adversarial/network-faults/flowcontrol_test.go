package networkfaults

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// fcCase: a dynamic MDC publication on driver a with two receivers, r1 on driver b and r2 on driver a, and a full
// partition of r2's path for 6s (> flow control receiver timeout 2s and image liveness 3s).
type fcCase struct {
	fc, tag string
	// Java 1.53.3 expectations while r2 is partitioned:
	minStall, maxStall time.Duration // longest gap in r1's delivery (the sender limit, not offers: the publication
	// window is far larger than what is sent during the partition)
	notConnected bool // tagged group below its min size: publication not connected
}

func TestFlowControlPartitionedReceiver(t *testing.T) {
	cases := map[string]fcCase{
		// max: the fastest receiver paces the sender, a partitioned receiver never stalls it.
		"max": {fc: "max", maxStall: time.Second},
		// min: the slowest receiver paces the sender until it is dropped after the receiver timeout (2s).
		"min": {fc: "min", minStall: time.Second, maxStall: fcRcvTimeout + 2*time.Second},
		// tagged with group min size 2: dropping one of the two tagged receivers leaves the group below its
		// required size, so hasRequiredReceivers() is false and the publication reports not connected until the
		// receiver comes back.
		"tagged-g2": {fc: "tagged,g:95/2", tag: "gtag=95", notConnected: true},
	}
	for name, fc := range cases {
		name, fc := name, fc
		t.Run(name, func(t *testing.T) {
			requireStack(t)
			base := snapshot(t)
			a, b := connect(t, dirA), connect(t, dirB)
			c, d1, d2, stream := port(), port(), port(), streamID()
			pub, err := a.AddPublication(fmt.Sprintf("aeron:udp?control=localhost:%d|control-mode=dynamic|fc=%s", c, fc.fc), stream)
			require.NoError(t, err)
			subCh := func(d int) string {
				ch := fmt.Sprintf("aeron:udp?endpoint=localhost:%d|control=localhost:%d", d, c)
				if fc.tag != "" {
					ch += "|" + fc.tag
				}
				return ch
			}
			sub1, err := b.AddSubscription(subCh(d1), stream)
			require.NoError(t, err)
			sub2, err := a.AddSubscription(subCh(d2), stream)
			require.NoError(t, err)
			img1 := awaitImage(t, b.available, "r1 available", 10*time.Second)
			img2 := awaitImage(t, a.available, "r2 available", 10*time.Second)
			awaitTrue(t, "publication connected", 10*time.Second, pub.IsConnected)
			r1, r2 := newImageReader(img1), newImageReader(img2)
			pb := newPublisher(pub, modeOffer, smallSize)
			done := pb.publishAsync(1<<40, 200*time.Microsecond, 2*time.Minute)
			pollBoth := func() { r1.poll(); r2.poll() }
			r1.pollUntil(t, "pre-partition traffic", 10*time.Second, func() bool { r2.poll(); return r1.received > 1000 })
			require.NoError(t, r2.err)

			pb.resetStall()
			netem(t, toBoth("loss 100%", d2))
			start := time.Now()
			sawNotConnected := false
			var stall time.Duration
			lastNext, lastProgress := r1.next, time.Now()
			for time.Since(start) < 6*time.Second {
				pollBoth()
				if !pub.IsConnected() {
					sawNotConnected = true
				}
				if now := time.Now(); r1.next != lastNext {
					lastNext, lastProgress = r1.next, now
				} else if gap := now.Sub(lastProgress); gap > stall {
					stall = gap
				}
			}
			t.Logf("%s: max stall %v during partition, notConnected seen %v (offers %d), r1 %d..%d", name, stall,
				sawNotConnected, pb.notConn.Load(), r1.first, r1.next-1)
			require.NoError(t, r1.err)
			if fc.notConnected {
				require.True(t, sawNotConnected, "tagged publication stayed connected below group min size")
				require.Positive(t, pb.notConn.Load())
			} else {
				require.False(t, sawNotConnected)
				require.GreaterOrEqual(t, stall, fc.minStall)
				require.Less(t, stall, fc.maxStall)
			}
			gone := awaitImage(t, a.unavailable, "r2 unavailable", 5*time.Second)
			require.Equal(t, img2.CorrelationID(), gone.CorrelationID())
			noImageEvent(t, b.unavailable, "r1 unavailable")

			clearNetem(t)
			img2b := awaitImage(t, a.available, "r2 available after heal", 20*time.Second)
			require.Greater(t, img2b.JoinPosition(), img2.JoinPosition())
			awaitTrue(t, "publication connected after heal", 10*time.Second, func() bool { pollBoth(); return pub.IsConnected() })
			r2b := newImageReader(img2b)
			pb.resetStall()
			r1.pollUntil(t, "post-heal traffic", 15*time.Second, func() bool { r2b.poll(); return r2b.received > 1000 })
			require.NoError(t, r2b.err)
			require.Less(t, time.Duration(pb.maxStall.Load()), 2*time.Second, "publisher stuck after heal")
			pb.stop.Store(true)
			require.NoError(t, <-done)
			require.NoError(t, sub1.Close())
			require.NoError(t, sub2.Close())
			require.NoError(t, pub.Close())
			require.Empty(t, a.errors.all())
			require.Empty(t, b.errors.all())
			requireNoLeaks(t, base, a, b)
		})
	}
}

// TestDynamicMDCControlLoss: 30% loss on the dynamic MDC control endpoint only (status messages, NAKs and setup
// elicitation all travel to it). Delivery stays reliable and ordered.
func TestDynamicMDCControlLoss(t *testing.T) {
	requireStack(t)
	c, d := port(), port()
	topo := topology{pub: fmt.Sprintf("aeron:udp?control=localhost:%d|control-mode=dynamic", c),
		sub: fmt.Sprintf("aeron:udp?endpoint=localhost:%d|control=localhost:%d", d, c)}
	_, b, pub, sub, _ := goPair(t, topo, false)
	netem(t, toBoth("loss 30%", c), toPorts("loss 5%", d))
	pb := newPublisher(pub, modeOffer, mixedSize)
	done := pb.publishAsync(10000, 0, 2*time.Minute)
	cons := newConsumer(sub, 0, false)
	require.NoError(t, cons.consume(10000, 2*time.Minute), netemStats(t))
	require.NoError(t, <-done)
	noImageEvent(t, b.unavailable, "unavailable")
	require.Empty(t, b.errors.all())
}

// TestDynamicMDCControlPartition: only the status path to the dynamic MDC control endpoint is cut, for 10s. Java: the
// publication goes not connected (no status messages, 3s), the sender drops the destination after its 5s
// DESTINATION_TIMEOUT and the receiver's image then times out; when the control path heals, the receiver's periodic
// setup elicitation re-adds the destination and a new image joins.
func TestDynamicMDCControlPartition(t *testing.T) {
	requireStack(t)
	c, d := port(), port()
	topo := topology{pub: fmt.Sprintf("aeron:udp?control=localhost:%d|control-mode=dynamic", c),
		sub: fmt.Sprintf("aeron:udp?endpoint=localhost:%d|control=localhost:%d", d, c)}
	a, b, pub, _, _ := goPair(t, topo, false)
	img1 := awaitImage(t, b.available, "available", 10*time.Second)
	r1 := newImageReader(img1)
	pb := newPublisher(pub, modeOffer, smallSize)
	done := pb.publishAsync(1<<40, 200*time.Microsecond, 2*time.Minute)
	r1.pollUntil(t, "pre-partition traffic", 10*time.Second, func() bool { return r1.received > 1000 })

	netem(t, toPorts("loss 100%", c))
	start := time.Now()
	awaitTrue(t, "publication not connected", 8*time.Second, func() bool { r1.poll(); return !pub.IsConnected() })
	r1.pollUntil(t, "image unavailable", 15*time.Second, func() bool {
		select {
		case <-b.unavailable:
			return true
		default:
			return false
		}
	})
	t.Logf("image unavailable after %v", time.Since(start))
	clearNetem(t)
	img2 := awaitImage(t, b.available, "available after heal", 20*time.Second)
	require.GreaterOrEqual(t, img2.JoinPosition(), img1.Position())
	awaitTrue(t, "publication reconnected", 10*time.Second, pub.IsConnected)
	r2 := newImageReader(img2)
	pb.resetStall()
	r2.pollUntil(t, "post-heal traffic", 15*time.Second, func() bool { return r2.received > 1000 })
	require.Greater(t, r2.first, r1.next-1)
	require.Less(t, time.Duration(pb.maxStall.Load()), 3*time.Second)
	pb.stop.Store(true)
	require.NoError(t, <-done)
	require.Empty(t, a.errors.all())
	require.Empty(t, b.errors.all())
}
