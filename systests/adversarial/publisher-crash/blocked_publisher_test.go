package publishercrash

import (
	"fmt"
	"testing"
	"time"

	"github.com/lirm/aeron-go/aeron"
	ab "github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/e2e"
	"github.com/stretchr/testify/require"
)

// offerAndPoll offers messages [from, from+count) of size bytes on pub while polling c, until c has received them.
func offerAndPoll(t *testing.T, pub aeron.Publisher, c *consumer, session int32, from, count int64, size int,
	timeout time.Duration) {
	t.Helper()
	msg := make([]byte, size)
	buf := ab.MakeBuffer(msg)
	seq := from
	deadline := time.Now().Add(timeout)
	for c.session(session).next < from+count {
		if time.Now().After(deadline) {
			t.Fatalf("%s: timed out after %v offering %d..%d (offered up to %d): %s", c.kind, timeout, from,
				from+count, seq, c.statsString())
		}
		if seq < from+count {
			e2e.EncodeMessage(msg, seq)
			r := pub.Offer(buf, 0, int32(size), nil)
			if r > 0 {
				seq++
			} else {
				require.Contains(t, []int64{aeron.BackPressured, aeron.AdminAction, aeron.NotConnected}, r,
					"offer failed with %d", r)
			}
		}
		if c.poll() == 0 && seq >= from+count {
			time.Sleep(100 * time.Microsecond)
		}
	}
}

// TestDeadPublisherHoldingSharedPublication kills a Go or Java publisher process while it holds an uncommitted
// TryClaim (holdclaim) or is halfway through writing a fragmented message (holdfrag: the first fragments are
// committed, the rest never written), while another client keeps using the same concurrent publication. Java 1.53.3
// (IpcPublication/NetworkPublication.checkForBlockedPublisher): after aeron.publication.unblock.timeout the driver
// pads the blocked region (LogBufferUnblocker), increments the Unblocked Publications counter and the other
// publisher's messages flow; subscribers skip the padding, drop the incomplete fragmented message and receive every
// other message exactly once and uncorrupted, without the image ever going away.
func TestDeadPublisherHoldingSharedPublication(t *testing.T) {
	cases := []struct {
		lang     lang
		mode     string
		size     int
		tr       transport
		consumer consumerKind
	}{
		{goLang, "holdclaim", 200, ipc, pollFA},
		{goLang, "holdclaim", 200, udp, imageIFA},
		{goLang, "holdfrag", 20000, ipc, controlledCFA},
		{goLang, "holdfrag", 20000, udp, boundedIFA},
		{goLang, "holdfrag", 20000, mdcDynamic, boundedCtlICF},
		{javaLang, "holdclaim", 200, ipc, boundedCtlICF},
		{javaLang, "holdclaim", 200, multicast, controlledCFA},
		{javaLang, "holdfrag", 20000, ipc, imageIFA},
		{javaLang, "holdfrag", 20000, udp, pollFA},
	}
	for i, tc := range cases {
		tc := tc
		stream := int32(9230 + i)
		port := 27240 + 2*i
		t.Run(fmt.Sprintf("%s/%s/%s/%s", tc.lang, tc.mode, tc.tr, tc.consumer), func(t *testing.T) {
			requireAdv(t)
			checkLeaks(t)
			a := connect(t)
			ch := channelsFor(tc.tr, port)
			c := newConsumer(t, a, tc.consumer, ch.sub, stream, ch.subDests...)
			unblockedBefore := systemCounter(a, "Unblocked Publications")
			require.GreaterOrEqual(t, unblockedBefore, int64(0))

			const holdAfter = 100
			holder := startPeer(t, tc.lang, "pub", pubArgs(ch, stream, "mode="+tc.mode, fmt.Sprintf("size=%d", tc.size),
				fmt.Sprintf("holdAfter=%d", holdAfter), "holdFragment=3", "intervalUs=200")...)
			session := int32(holder.await("ready", 30*time.Second, nil).int("sessionId"))
			c.pollUntil("publisher holding", 30*time.Second, func() bool { return holder.last("holding") != nil })

			// The test's publication shares the holder's driver publication (same channel and stream).
			pub, err := a.AddPublication(ch.pub, stream)
			require.NoError(t, err)
			defer pub.Close()
			require.Equal(t, session, pub.SessionID(), "not the shared publication")
			for _, d := range ch.pubDests {
				require.NoError(t, pub.AddDestination(d))
			}
			c.pollUntil("messages before the held one", 20*time.Second, func() bool {
				return c.session(session).next == holdAfter
			})
			holder.killAndWait()
			killedAt := time.Now()

			// The held message is never delivered, so the shared publication's messages continue the sequence.
			offerAndPoll(t, pub, c, session, holdAfter, 400, tc.size, unblockTimeout+clientLiveness+30*time.Second)
			t.Logf("unblocked %v after the kill", time.Since(killedAt))
			require.Greater(t, systemCounter(a, "Unblocked Publications"), unblockedBefore)
			require.False(t, c.isGone(session), "image of the still used publication went away")
			require.True(t, c.sub.IsConnected())
			require.Equal(t, int64(holdAfter+400), c.session(session).received, "partial message delivered")
			c.requireClean()
			require.NoError(t, pub.Close())
			c.pollUntil("unavailable image after the last publisher closed", 20*time.Second, func() bool {
				return c.isGone(session)
			})
			awaitDriverForgets(t, a, session, stream, int64(holder.await("ready", 0, nil).int("clientId")),
				20*time.Second)
		})
	}
}

// offerSeq offers message seq (e2e format, len(msg) bytes) once.
func offerSeq(pub aeron.Publisher, msg []byte, seq int64) int64 {
	e2e.EncodeMessage(msg, seq)
	return pub.Offer(ab.MakeBuffer(msg), 0, int32(len(msg)), nil)
}
