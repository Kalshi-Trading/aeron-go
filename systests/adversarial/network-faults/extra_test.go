package networkfaults

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestMultiDestinationSubscriptionPathPartition: a manual MDC publication feeds both destinations of a
// multi-destination subscription; one path is partitioned for 6s. Java merges the transports into one image, so
// delivery continues gap free over the surviving path, the image stays available and its active transport count
// drops to 1, returning to 2 after the path heals.
func TestMultiDestinationSubscriptionPathPartition(t *testing.T) {
	requireStack(t)
	topo := topologyByName("multi-destination-sub")
	a, b, pub, _, _ := goPair(t, topo, true)
	img := awaitImage(t, b.available, "available", 10*time.Second)
	r := newImageReader(img)
	pb := newPublisher(pub, modeOffer, mixedSize)
	done := pb.publishAsync(1<<40, 200*time.Microsecond, 2*time.Minute)
	r.pollUntil(t, "both transports active", 10*time.Second, func() bool { return img.ActiveTransportCount() == 2 && r.received > 500 })

	netem(t, toBoth("loss 100%", topo.ports[1]))
	r.pollUntil(t, "transport count 1", imageLiveness+5*time.Second, func() bool { return img.ActiveTransportCount() == 1 })
	before := r.next
	r.pollUntil(t, "delivery over the surviving path", 10*time.Second, func() bool { return r.next > before+3000 })
	noImageEvent(t, b.unavailable, "unavailable")
	clearNetem(t)
	r.pollUntil(t, "transport count 2 after heal", 15*time.Second, func() bool { return img.ActiveTransportCount() == 2 })
	healed := r.next
	r.pollUntil(t, "post-heal traffic", 10*time.Second, func() bool { return r.next > healed+1000 })
	require.Zero(t, r.first)
	require.False(t, img.IsClosed())
	pb.stop.Store(true)
	require.NoError(t, <-done)
	require.Empty(t, a.errors.all())
	require.Empty(t, b.errors.all())
}

// TestSpyUnderPartition: a spy subscription on the publishing driver reads the publication locally, so it is
// unaffected by a full network partition while the remote subscriber loses its image. As in Java 1.53.3, with the
// default ssc=false the publication is not connected once the only network receiver is gone (offers stop and the
// spy holds everything published), while ssc=true (spies simulate connection) keeps it connected and the spy keeps
// receiving through the partition.
func TestSpyUnderPartition(t *testing.T) {
	for _, ssc := range []bool{false, true} {
		t.Run(fmt.Sprintf("ssc=%v", ssc), func(t *testing.T) {
			requireStack(t)
			p := port()
			sub := udp("localhost", p, "")
			pubCh := sub
			if ssc {
				pubCh = udp("localhost", p, "ssc=true")
			}
			a, b, pub, _, stream := goPair(t, topology{pub: pubCh, sub: sub, ports: []int{p}}, false)
			spy, err := a.AddSubscription("aeron-spy:"+sub, stream)
			require.NoError(t, err)
			remote := newImageReader(awaitImage(t, b.available, "remote available", 10*time.Second))
			spyImg := awaitImage(t, a.available, "spy available", 10*time.Second)
			require.Equal(t, spy.RegistrationID(), spyImg.SubscriptionRegistrationID())
			r := newImageReader(spyImg)
			pb := newPublisher(pub, modeOffer, mixedSize)
			done := pb.publishAsync(1<<40, 200*time.Microsecond, 2*time.Minute)
			netem(t, toBoth("loss 100%", p))
			// As in Java, an inactive image only goes unavailable once its subscribers have drained it.
			awaitImageWhilePolling(t, b.unavailable, func() int { return remote.poll() + r.poll() },
				imageLiveness+5*time.Second)
			require.NoError(t, remote.err)
			if ssc {
				before := r.next
				r.pollUntil(t, "spy progress during partition", 10*time.Second, func() bool { return r.next > before+3000 })
				require.True(t, pub.IsConnected())
			} else {
				awaitTrue(t, "publication not connected", 5*time.Second, func() bool { return !pub.IsConnected() })
				r.pollUntil(t, "spy caught up", 10*time.Second, func() bool { return r.next >= pb.next.Load() })
				require.Greater(t, pb.notConn.Load(), int64(0))
			}
			clearNetem(t)
			awaitTrue(t, "remote image after heal", 15*time.Second, func() bool {
				r.poll()
				select {
				case img := <-b.available:
					remote = newImageReader(img)
					return true
				default:
					return false
				}
			})
			awaitTrue(t, "connected", 15*time.Second, pub.IsConnected)
			healed := pb.next.Load()
			r.pollUntil(t, "spy after heal", 10*time.Second, func() bool {
				remote.poll()
				return r.next > healed+1000 && remote.received > 500
			})
			require.Zero(t, r.first)
			require.NoError(t, r.err)
			require.NoError(t, remote.err)
			pb.stop.Store(true)
			require.NoError(t, <-done)
			require.Empty(t, a.errors.all())
			require.Empty(t, b.errors.all())
		})
	}
}

// TestJavaPeersLongPartition: long (6s) partitions between a Java 1.53.3 peer and a Go peer. Go subscriber: the
// image from the Java publisher goes unavailable and a new one with a later join position follows the heal. Java
// subscriber: it reports imageGone and then a new image for the Go publication with a join position at or beyond
// where the old one stopped.
func TestJavaPeersLongPartition(t *testing.T) {
	t.Run("java-pub-go-sub", func(t *testing.T) {
		requireStack(t)
		b := connect(t, dirB)
		p, stream := port(), streamID()
		ch := udp("localhost", p, "")
		sub, err := b.AddSubscription(ch, stream)
		require.NoError(t, err)
		java := startJava(t, dirA, "pub", "channel="+ch, fmt.Sprintf("stream=%d", stream), "count=200000",
			"sizes=100,3000", "delayUs=200", "timeoutMs=120000")
		java.await("connected", 30*time.Second)
		img1 := awaitImage(t, b.available, "available", 10*time.Second)
		cons := newJavaReader(img1)
		cons.pollUntil(t, 1000, 10*time.Second)
		netem(t, toBoth("loss 100%", p))
		gone := awaitImageWhilePolling(t, b.unavailable, cons.poll, imageLiveness+5*time.Second)
		require.Equal(t, img1.CorrelationID(), gone.CorrelationID())
		time.Sleep(3 * time.Second)
		clearNetem(t)
		img2 := awaitImage(t, b.available, "available after heal", 20*time.Second)
		require.GreaterOrEqual(t, img2.JoinPosition(), img1.Position())
		require.Equal(t, img1.SessionID(), img2.SessionID())
		c2 := newJavaReader(img2)
		c2.pollUntil(t, 1000, 15*time.Second)
		require.Greater(t, c2.first, cons.next-1)
		require.Equal(t, 1, sub.ImageCount())
		require.Empty(t, b.errors.all())
	})
	t.Run("go-pub-java-sub", func(t *testing.T) {
		requireStack(t)
		a := connect(t, dirA)
		p, stream := port(), streamID()
		ch := udp("localhost", p, "")
		java := startJava(t, dirB, "sub", "channel="+ch, fmt.Sprintf("stream=%d", stream), "count=100000000",
			"sizes=100", "timeoutMs=120000")
		java.await("ready", 30*time.Second)
		pub, err := a.AddPublication(ch, stream)
		require.NoError(t, err)
		awaitTrue(t, "connected", 15*time.Second, pub.IsConnected)
		img1 := java.await("image", 10*time.Second)
		pb := newPublisher(pub, modeOffer, smallSize)
		done := pb.publishAsync(1<<40, 200*time.Microsecond, 2*time.Minute)
		time.Sleep(500 * time.Millisecond)
		netem(t, toBoth("loss 100%", p))
		gone := java.await("imageGone", imageLiveness+5*time.Second)
		awaitTrue(t, "go publication not connected", 5*time.Second, func() bool { return !pub.IsConnected() })
		time.Sleep(3 * time.Second)
		clearNetem(t)
		img2 := java.await("image", 20*time.Second)
		t.Logf("java images: %v gone %v new %v", img1, gone, img2)
		require.Equal(t, img1.int("sessionId"), img2.int("sessionId"))
		require.NotEqual(t, img1.int("correlationId"), img2.int("correlationId"))
		require.GreaterOrEqual(t, img2.int("joinPosition"), gone.int("position"))
		awaitTrue(t, "go publication reconnected", 10*time.Second, pub.IsConnected)
		pb.resetStall()
		time.Sleep(time.Second)
		require.Less(t, time.Duration(pb.maxStall.Load()), 2*time.Second)
		pb.stop.Store(true)
		require.NoError(t, <-done)
		require.Empty(t, a.errors.all())
	})
}
