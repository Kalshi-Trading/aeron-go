package networkfaults

import (
	"fmt"
	"testing"
	"time"

	"github.com/lirm/aeron-go/aeron"
	ab "github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/logbuffer"
	"github.com/lirm/aeron-go/e2e"
	"github.com/stretchr/testify/require"
)

// goPair connects a Go publisher on driver a and a Go subscriber on driver b over topo.
func goPair(t *testing.T, topo topology, exclusive bool) (*client, *client, aeron.Publisher, *aeron.Subscription, int32) {
	t.Helper()
	a, b := connect(t, dirA), connect(t, dirB)
	stream := streamID()
	sub, err := b.AddSubscription(topo.sub, stream)
	require.NoError(t, err)
	for _, d := range topo.subDests {
		require.NoError(t, sub.AddDestination(d))
	}
	var pub aeron.Publisher
	if exclusive {
		pub, err = a.AddExclusivePublication(topo.pub, stream)
	} else {
		pub, err = a.AddPublication(topo.pub, stream)
	}
	require.NoError(t, err)
	for _, d := range topo.pubDests {
		require.NoError(t, pub.AddDestination(d))
	}
	awaitTrue(t, "publication connected", 15*time.Second, pub.IsConnected)
	return a, b, pub, sub, stream
}

// imageReader polls one image through its own assembler, checking every message decodes and that sequences are
// contiguous from the first one it sees.
type imageReader struct {
	img       aeron.Image
	first     int64
	next      int64
	received  int64
	err       error
	assembler *aeron.ImageFragmentAssembler
}

func newImageReader(img aeron.Image) *imageReader {
	r := &imageReader{img: img, first: -1}
	r.assembler = aeron.NewImageFragmentAssembler(func(buf *ab.Buffer, off, length int32, _ *logbuffer.Header) {
		seq, err := e2e.DecodeMessage(buf.GetBytesArray(off, length))
		switch {
		case r.err != nil:
		case err != nil:
			r.err = err
		case r.first < 0:
			r.first, r.next = seq, seq+1
		case seq != r.next:
			r.err = fmt.Errorf("image %d: seq %d after %d", img.CorrelationID(), seq, r.next-1)
		default:
			r.next++
		}
		r.received++
	}, 0)
	return r
}

func (r *imageReader) poll() int { return r.img.Poll(r.assembler.OnFragment, 64) }

func (r *imageReader) pollUntil(t *testing.T, what string, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		require.NoError(t, r.err)
		if r.poll() == 0 {
			require.True(t, time.Now().Before(deadline), "timed out after %v waiting for %s (received %d next %d)",
				timeout, what, r.received, r.next)
			time.Sleep(100 * time.Microsecond)
		}
	}
	require.NoError(t, r.err)
}

func partitionTopologies() []string {
	return []string{"unicast", "multicast", "mdc-dynamic", "mdc-manual"}
}

func topologyByName(name string) topology {
	for _, tp := range topologies() {
		if tp.name == name {
			return tp.make()
		}
	}
	panic(name)
}

// TestShortPartition: a 1.5s full partition (both directions), shorter than the 3s image liveness and publication
// connection timeouts. Java keeps the image and the publication connected; the data sent into the partition is
// recovered by NAKs on heal, so the same image delivers every message exactly once, and the publisher (back
// pressured once the receiver window fills) resumes.
func TestShortPartition(t *testing.T) {
	for _, name := range partitionTopologies() {
		name := name
		t.Run(name, func(t *testing.T) {
			requireStack(t)
			base := snapshot(t)
			topo := topologyByName(name)
			a, b, pub, _, _ := goPair(t, topo, name == "mdc-manual")
			img := awaitImage(t, b.available, "available", 10*time.Second)
			r := newImageReader(img)
			pb := newPublisher(pub, modeOffer, mixedSize)
			done := pb.publishAsync(1<<40, 100*time.Microsecond, 2*time.Minute)
			r.pollUntil(t, "pre-partition traffic", 10*time.Second, func() bool { return r.received > 2000 })

			netem(t, toBoth("loss 100%", topo.ports...))
			for end := time.Now().Add(1500 * time.Millisecond); time.Now().Before(end); {
				r.poll()
			}
			clearNetem(t)
			pb.resetStall()
			healed := pb.next.Load()
			r.pollUntil(t, "post-heal traffic", 15*time.Second, func() bool { return r.next > healed+2000 })
			require.Zero(t, r.first, "image did not start at the first message")
			require.True(t, pub.IsConnected())
			noImageEvent(t, b.unavailable, "unavailable")
			require.False(t, img.IsClosed())
			require.Less(t, pb.maxStall.Load(), int64(3*time.Second), "publisher stalled after heal")
			t.Logf("%s: received %d, backPressured %d notConnected %d", name, r.received, pb.backPres.Load(), pb.notConn.Load())
			pb.stop.Store(true)
			require.NoError(t, <-done)
			require.Empty(t, a.errors.all())
			require.Empty(t, b.errors.all())
			requireNoLeaks(t, base, a, b)
		})
	}
}

// TestLongPartition: a 6s full partition, longer than image liveness (3s) and publication connection timeout (3s).
// Java: the subscriber's image goes unavailable, the publication reports not connected (offers return
// NOT_CONNECTED, not endless BACK_PRESSURED), and on heal the publication reconnects and the subscriber gets a new
// image (new correlation id) whose join position is at or beyond the old image's position; messages on the new image
// are contiguous and never repeat ones already delivered.
func TestLongPartition(t *testing.T) {
	for _, name := range partitionTopologies() {
		for _, exclusive := range []bool{false, true} {
			name, exclusive := name, exclusive
			t.Run(fmt.Sprintf("%s/exclusive=%v", name, exclusive), func(t *testing.T) {
				requireStack(t)
				base := snapshot(t)
				topo := topologyByName(name)
				a, b, pub, sub, _ := goPair(t, topo, exclusive)
				img1 := awaitImage(t, b.available, "available", 10*time.Second)
				r1 := newImageReader(img1)
				pb := newPublisher(pub, modeOffer, mixedSize)
				done := pb.publishAsync(1<<40, 200*time.Microsecond, 3*time.Minute)
				r1.pollUntil(t, "pre-partition traffic", 10*time.Second, func() bool { return r1.received > 1000 })

				netem(t, toBoth("loss 100%", topo.ports...))
				start := time.Now()
				r1.pollUntil(t, "image unavailable", imageLiveness+5*time.Second, func() bool {
					select {
					case img := <-b.unavailable:
						require.Equal(t, img1.CorrelationID(), img.CorrelationID())
						return true
					default:
						return false
					}
				})
				t.Logf("image unavailable after %v", time.Since(start))
				require.GreaterOrEqual(t, time.Since(start), imageLiveness-500*time.Millisecond)
				require.True(t, img1.IsClosed())
				require.Zero(t, img1.Poll(r1.assembler.OnFragment, 10))
				awaitTrue(t, "publication not connected", 5*time.Second, func() bool { return !pub.IsConnected() })
				awaitTrue(t, "offer NOT_CONNECTED", 5*time.Second, func() bool { return pb.notConn.Load() > 0 })
				require.Zero(t, sub.ImageCount())
				for time.Since(start) < 6*time.Second {
					time.Sleep(10 * time.Millisecond)
				}

				clearNetem(t)
				img2 := awaitImage(t, b.available, "available after heal", 20*time.Second)
				require.NotEqual(t, img1.CorrelationID(), img2.CorrelationID())
				require.Equal(t, img1.SessionID(), img2.SessionID())
				require.GreaterOrEqual(t, img2.JoinPosition(), img1.Position())
				require.Greater(t, img2.JoinPosition(), img1.JoinPosition())
				awaitTrue(t, "publication reconnected", 10*time.Second, pub.IsConnected)
				pb.resetStall()
				r2 := newImageReader(img2)
				r2.pollUntil(t, "post-heal traffic", 15*time.Second, func() bool { return r2.received > 2000 })
				require.Greater(t, r2.first, r1.next-1, "new image repeated delivered messages")
				require.Less(t, pb.maxStall.Load(), int64(3*time.Second), "publisher stuck after heal")
				require.Equal(t, 1, sub.ImageCount())
				t.Logf("old image %d..%d pos %d, new image join %d first seq %d", r1.first, r1.next-1, img1.Position(),
					img2.JoinPosition(), r2.first)
				pb.stop.Store(true)
				require.NoError(t, <-done)
				require.Empty(t, a.errors.all())
				require.Empty(t, b.errors.all())
				requireNoLeaks(t, base, a, b)
			})
		}
	}
}

// TestStatusPathPartition drops only what the receiver sends (status messages, NAKs) for longer than the
// publication connection timeout. Java: the sender keeps heartbeating, so the image stays alive, but with no status
// messages the publication goes not connected; when status messages return it reconnects and the same image
// continues without a gap.
func TestStatusPathPartition(t *testing.T) {
	requireStack(t)
	p := port()
	topo := topology{pub: udp("localhost", p, ""), sub: udp("localhost", p, ""), ports: []int{p}}
	a, b, pub, _, _ := goPair(t, topo, false)
	img := awaitImage(t, b.available, "available", 10*time.Second)
	r := newImageReader(img)
	pb := newPublisher(pub, modeOffer, smallSize)
	done := pb.publishAsync(1<<40, 200*time.Microsecond, 2*time.Minute)
	r.pollUntil(t, "pre-partition traffic", 10*time.Second, func() bool { return r.received > 1000 })

	netem(t, fromPorts("loss 100%", p))
	start := time.Now()
	awaitTrue(t, "publication not connected", 8*time.Second, func() bool { r.poll(); return !pub.IsConnected() })
	t.Logf("not connected after %v", time.Since(start))
	for time.Since(start) < 5*time.Second {
		r.poll()
	}
	noImageEvent(t, b.unavailable, "unavailable")
	clearNetem(t)
	awaitTrue(t, "publication reconnected", 10*time.Second, func() bool { r.poll(); return pub.IsConnected() })
	pb.resetStall()
	healed := pb.next.Load()
	r.pollUntil(t, "post-heal traffic", 15*time.Second, func() bool { return r.next > healed+1000 })
	require.Zero(t, r.first)
	require.False(t, img.IsClosed())
	pb.stop.Store(true)
	require.NoError(t, <-done)
	require.Empty(t, a.errors.all())
	require.Empty(t, b.errors.all())
}
