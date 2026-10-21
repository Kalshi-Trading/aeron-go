package publishercrash

import (
	"fmt"
	"os/exec"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestSubscriberJoinsAfterPublisherDied SIGKILLs a publisher process and then subscribes, both while the driver
// still has the dead client's publication and after it has removed it, with the in-process Go subscriber and a Java
// 1.53.3 subscriber side by side as the oracle. Java: what a late joiner sees depends only on the driver (an image
// of the dead publication that reaches end of stream and goes unavailable while it is still being drained, nothing
// once it is gone); the Go subscriber must see exactly what the Java one sees, never data from before its join
// position and never corrupt data, and both receive a publisher restarted afterwards.
func TestSubscriberJoinsAfterPublisherDied(t *testing.T) {
	cases := []struct {
		tr      transport
		pubLang lang
		wait    bool // wait until the driver forgot the dead publication before subscribing
	}{
		{ipc, goLang, false}, {ipc, javaLang, true}, {udp, goLang, true}, {udp, javaLang, false},
		{mdcDynamic, goLang, false}, {spy, javaLang, true},
	}
	for i, tc := range cases {
		tc := tc
		stream := int32(9282 + i)
		port := 27300 - 2*(i+2) // 27296, 27294, ... 27286 (27298/27299 belong to the driver kill test)
		t.Run(fmt.Sprintf("%s/%s/afterCleanup=%t", tc.tr, tc.pubLang, tc.wait), func(t *testing.T) {
			requireAdv(t)
			checkLeaks(t)
			a := connect(t)
			ch := channelsFor(tc.tr, port)
			first := newConsumer(t, a, pollFA, ch.sub, stream)
			pub := startPeer(t, tc.pubLang, "pub", pubArgs(ch, stream, "size=3000", "count=300")...)
			ready := pub.await("ready", 30*time.Second, nil)
			session := int32(ready.int("sessionId"))
			pub.await("done", 30*time.Second, nil)
			first.pollUntil("all messages", 30*time.Second, func() bool { return first.session(session).next == 300 })
			pub.killAndWait()
			if tc.wait {
				first.pollUntil("unavailable image", 20*time.Second, func() bool { return first.isGone(session) })
				awaitDriverForgets(t, a, session, stream, ready.int("clientId"), 20*time.Second)
			}

			late := newConsumer(t, a, imageIFA, ch.sub, stream)
			oracle := startPeer(t, javaLang, "sub", "channel="+ch.sub, fmt.Sprintf("stream=%d", stream))
			oracle.await("ready", 30*time.Second, nil)
			// Run past the dead client's liveness timeout, drain and linger.
			deadline := time.Now().Add(3*clientLiveness + 3*time.Second)
			for time.Now().Before(deadline) {
				first.poll()
				late.poll()
				time.Sleep(200 * time.Microsecond)
			}
			require.True(t, first.isGone(session))
			awaitDriverForgets(t, a, session, stream, ready.int("clientId"), 20*time.Second)
			_, javaImages := oracle.report()
			require.Zero(t, javaImages)
			javaSaw := false
			for _, e := range oracle.snapshot() {
				if e["event"] == "image" && int32(e.int("sessionId")) == session {
					javaSaw = true
				}
			}
			goAvailable, goGone := late.imagesFor(session)
			t.Logf("late joiner images of the dead session: java=%t go=%d available/%d gone", javaSaw, goAvailable,
				goGone)
			require.Equal(t, goAvailable, goGone, "late image never went unavailable")
			if javaSaw != (goAvailable > 0) {
				// The two subscriptions are added a few ms apart, so only flag a difference when the driver would
				// have made it deterministic (it no longer had the publication).
				require.False(t, tc.wait, "Go and Java late joiners differ: java=%t go=%d", javaSaw, goAvailable)
			}
			if img := late.lastImageFor(session); img != nil {
				st := late.session(session)
				if st.received > 0 {
					require.Equal(t, img.JoinPosition() > 0, st.first > 0)
				}
			}
			require.False(t, late.sub.IsConnected())

			pub2 := startPeer(t, tc.pubLang, "pub", pubArgs(ch, stream, "size=3000", "count=200", "from=300")...)
			session2 := int32(pub2.await("ready", 30*time.Second, nil).int("sessionId"))
			late.pollUntil("restarted publisher", 30*time.Second, func() bool { return late.session(session2).next == 500 })
			require.EqualValues(t, 300, late.session(session2).first)
			oracle.await("progress", 30*time.Second, func(e event) bool { return e.int("received") >= 200 })
			late.requireClean()
			first.requireClean()
		})
	}
}

// netem runs tc in the NET_ADMIN sidecar on the host network.
func netem(t *testing.T, args ...string) error {
	out, err := exec.Command("docker", append([]string{"run", "--rm", "--network", "host", "--cap-add", "NET_ADMIN",
		netemImage}, args...)...).CombinedOutput()
	if err != nil {
		return fmt.Errorf("tc %v: %v: %s", args, err, out)
	}
	return nil
}

// TestPublisherKilledUnderLoss SIGKILLs a Go or Java UDP/MDC publisher while lo drops 5% of packets. Java 1.53.3:
// the driver's NetworkPublication keeps serving NAK retransmits from the dead publisher's log while it drains and
// lingers, so every message the publisher offered is still delivered exactly once, in order, before the image goes
// end of stream and unavailable.
func TestPublisherKilledUnderLoss(t *testing.T) {
	cases := []struct {
		lang lang
		tr   transport
		pub  pubKind
	}{
		{goLang, udp, concurrentOffer}, {javaLang, udp, exclusiveOffer}, {goLang, mdcManual, exclusiveClaim},
	}
	for i, tc := range cases {
		tc := tc
		stream := int32(9290 + i)
		port := 27270 + 2*i
		t.Run(fmt.Sprintf("%s/%s/%s", tc.lang, tc.tr, tc.pub), func(t *testing.T) {
			requireAdv(t)
			checkLeaks(t)
			a := connect(t)
			ch := channelsFor(tc.tr, port)
			c := newConsumer(t, a, pollFA, ch.sub, stream, ch.subDests...)
			p := startPeer(t, tc.lang, "pub", pubArgs(ch, stream, append(tc.pub.args(), "size=3000", "intervalUs=100")...)...)
			ready := p.await("ready", 30*time.Second, nil)
			session := int32(ready.int("sessionId"))
			p.await("connected", 30*time.Second, nil)
			require.NoError(t, netem(t, "qdisc", "add", "dev", "lo", "root", "netem", "loss", "5%"))
			t.Cleanup(func() { _ = netem(t, "qdisc", "del", "dev", "lo", "root") })
			c.pollUntil("messages under loss", 60*time.Second, func() bool { return c.session(session).received >= 1000 })
			p.killAndWait()
			published := p.lastPublished()
			c.pollUntil("unavailable image", 60*time.Second, func() bool { return c.isGone(session) })
			require.NoError(t, netem(t, "qdisc", "del", "dev", "lo", "root"))
			s := c.session(session)
			require.GreaterOrEqual(t, s.next, published, "messages offered before the kill were lost")
			require.Equal(t, s.next, s.received)
			c.requireClean()
			awaitDriverForgets(t, a, session, stream, ready.int("clientId"), 20*time.Second)
		})
	}
}
