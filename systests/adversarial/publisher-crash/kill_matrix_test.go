package publishercrash

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type pubKind struct {
	exclusive bool
	mode      string // offer, claim, offerblock
}

func (k pubKind) String() string {
	if k.exclusive {
		return "exclusive-" + k.mode
	}
	return "concurrent-" + k.mode
}

func (k pubKind) args() []string {
	return []string{fmt.Sprintf("exclusive=%t", k.exclusive), "mode=" + k.mode}
}

var (
	concurrentOffer = pubKind{mode: "offer"}
	concurrentClaim = pubKind{mode: "claim"}
	exclusiveOffer  = pubKind{exclusive: true, mode: "offer"}
	exclusiveClaim  = pubKind{exclusive: true, mode: "claim"}
	offerBlock      = pubKind{exclusive: true, mode: "offerblock"}
)

// TestPublisherKillMatrix SIGKILLs a Go or Java publisher process mid-stream for a cross-section of publication types,
// transports and subscriber styles (every pair of them is covered at least once). Java 1.53.3: the driver keeps
// draining what the dead publisher committed (every message it reported as offered is delivered, in order, exactly
// once), the client liveness timeout removes the publication, the image goes end of stream and unavailable (callback
// on the subscriber, all driver counters of the session and client freed), and a restarted publisher on the same
// channel and stream gets a new session id and is received from its first message.
func TestPublisherKillMatrix(t *testing.T) {
	cases := []struct {
		lang     lang
		pub      pubKind
		tr       transport
		consumer consumerKind
		size     int
	}{
		{goLang, concurrentOffer, ipc, pollFA, 200},
		{goLang, exclusiveOffer, ipc, controlledCFA, 5000},
		{goLang, offerBlock, ipc, boundedCtlICF, 200},
		{goLang, concurrentClaim, udp, imageIFA, 200},
		{goLang, offerBlock, udp, boundedIFA, 1000},
		{goLang, exclusiveClaim, multicast, boundedCtlICF, 200},
		{goLang, concurrentOffer, mdcManual, pollFA, 5000},
		{goLang, exclusiveOffer, mdcDynamic, imageIFA, 3000},
		{goLang, concurrentOffer, mds, controlledCFA, 200},
		{goLang, exclusiveOffer, spy, pollFA, 5000},
		{javaLang, concurrentOffer, ipc, imageIFA, 5000},
		{javaLang, exclusiveClaim, ipc, boundedIFA, 200},
		{javaLang, concurrentOffer, udp, pollFA, 5000},
		{javaLang, exclusiveOffer, multicast, controlledCFA, 3000},
		{javaLang, concurrentClaim, mdcDynamic, boundedCtlICF, 200},
		{javaLang, exclusiveOffer, mdcManual, imageIFA, 200},
		{javaLang, exclusiveOffer, mds, boundedIFA, 5000},
		{javaLang, concurrentOffer, spy, controlledCFA, 200},
	}
	for i, tc := range cases {
		tc := tc
		stream := int32(9200 + i)
		port := 27200 + 2*i
		t.Run(fmt.Sprintf("%s/%s/%s/%s/%dB", tc.lang, tc.pub, tc.tr, tc.consumer, tc.size), func(t *testing.T) {
			requireAdv(t)
			checkLeaks(t)
			a := connect(t)
			ch := channelsFor(tc.tr, port)
			c := newConsumer(t, a, tc.consumer, ch.sub, stream, ch.subDests...)
			args := pubArgs(ch, stream, append(tc.pub.args(), fmt.Sprintf("size=%d", tc.size), "intervalUs=200")...)

			p1 := startPeer(t, tc.lang, "pub", args...)
			ready := p1.await("ready", 30*time.Second, nil)
			session := int32(ready.int("sessionId"))
			p1.await("connected", 30*time.Second, nil)
			c.pollUntil("300 messages", 30*time.Second, func() bool { return c.session(session).received >= 300 })

			p1.killAndWait()
			published := p1.lastPublished()
			c.pollUntil("unavailable image", 20*time.Second, func() bool { return c.isGone(session) })
			c.pollFor(100 * time.Millisecond)
			s := c.session(session)
			require.EqualValues(t, 0, s.first)
			require.GreaterOrEqual(t, s.next, published, "lost messages the dead publisher had offered")
			require.Equal(t, s.next, s.received)
			c.mu.Lock()
			require.True(t, c.goneEOS[session], "image not end of stream when it went unavailable")
			c.mu.Unlock()
			awaitDriverForgets(t, a, session, stream, int64(ready.int("clientId")), 20*time.Second)
			require.False(t, c.sub.IsConnected())

			p2 := startPeer(t, tc.lang, "pub", append(args, fmt.Sprintf("from=%d", s.next))...)
			ready2 := p2.await("ready", 30*time.Second, nil)
			session2 := int32(ready2.int("sessionId"))
			require.NotEqual(t, session, session2, "restarted publisher reused the session id")
			p2.await("connected", 30*time.Second, nil)
			c.pollUntil("300 messages from the restarted publisher", 30*time.Second, func() bool {
				return c.session(session2).received >= 300
			})
			require.Equal(t, s.next, c.session(session2).first)
			require.Equal(t, s, c.session(session), "dead session delivered more after the unavailable image")
			p2.send("exit")
			c.pollUntil("restarted publisher's image unavailable after close", 20*time.Second, func() bool {
				return c.isGone(session2)
			})
			done := p2.await("done", 10*time.Second, nil)
			s2 := c.session(session2)
			require.Equal(t, done.int("next"), s2.next, "graceful close lost messages (linger)")
			c.requireClean()
		})
	}
}
