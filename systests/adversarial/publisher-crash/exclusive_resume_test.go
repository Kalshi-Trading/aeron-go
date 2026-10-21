package publishercrash

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestDeadExclusivePublisherResumes kills an exclusive publisher process holding an uncommitted claim or halfway
// through a fragmented message, then resumes the stream from the dead publication's final position with a new
// exclusive publication that sets session-id, init-term-id, term-id and term-offset explicitly. Java 1.53.3: the
// draining publication is unblocked (padding), so the image reaches end of stream right after the last committed
// message and goes unavailable without delivering the partial message; the resumed publication's image joins
// exactly at that position under the same session id and continues the sequence with no duplicates or corruption.
func TestDeadExclusivePublisherResumes(t *testing.T) {
	cases := []struct {
		lang      lang
		mode      string
		size      int
		tr        transport
		consumer  consumerKind
		resumeIn  lang
		holdAfter int
	}{
		{goLang, "holdclaim", 200, ipc, pollFA, goLang, 100},
		{goLang, "holdfrag", 20000, ipc, imageIFA, javaLang, 100},
		{goLang, "holdfrag", 20000, udp, pollFA, goLang, 100},
		{goLang, "holdclaim", 200, mdcManual, controlledCFA, javaLang, 100},
		// 60 x 20000 B crosses the first 1 MiB term boundary: the resumed position is in a later term.
		{goLang, "holdfrag", 20000, udp, boundedCtlICF, goLang, 60},
		{javaLang, "holdclaim", 200, udp, boundedIFA, goLang, 100},
		{javaLang, "holdfrag", 20000, ipc, controlledCFA, javaLang, 60},
		{javaLang, "holdfrag", 20000, mds, imageIFA, goLang, 100},
	}
	for i, tc := range cases {
		tc := tc
		stream := int32(9250 + i)
		port := 27260 + 2*i
		t.Run(fmt.Sprintf("%s/%s/%s/%s/resume-%s", tc.lang, tc.mode, tc.tr, tc.consumer, tc.resumeIn), func(t *testing.T) {
			requireAdv(t)
			checkLeaks(t)
			a := connect(t)
			ch := channelsFor(tc.tr, port)
			c := newConsumer(t, a, tc.consumer, ch.sub, stream, ch.subDests...)

			// The Java subscriber is the oracle; a multi-destination subscription can't share its endpoint.
			var oracle *peer
			if tc.tr != mds {
				oracle = startPeer(t, javaLang, "sub", subPeerArgs(ch, stream)...)
				oracle.await("ready", 30*time.Second, nil)
			}
			holder := startPeer(t, tc.lang, "pub", pubArgs(ch, stream, "exclusive=true", "mode="+tc.mode,
				fmt.Sprintf("size=%d", tc.size), fmt.Sprintf("holdAfter=%d", tc.holdAfter), "holdFragment=3")...)
			ready := holder.await("ready", 30*time.Second, nil)
			session := int32(ready.int("sessionId"))
			initialTermID, termLength := int32(ready.int("initialTermId")), ready.int("termLength")
			c.pollUntil("publisher holding", 30*time.Second, func() bool { return holder.last("holding") != nil })
			c.pollUntil("messages before the held one", 20*time.Second, func() bool {
				return c.session(session).next == int64(tc.holdAfter)
			})
			holder.killAndWait()
			var finalPosition int64
			cur := c
			if tc.mode == "holdclaim" {
				c.pollUntil("unavailable image", 20*time.Second, func() bool { return c.isGone(session) })
				if oracle != nil {
					oracle.await("imageGone", 20*time.Second, nil)
				}
				c.pollFor(100 * time.Millisecond)
				c.mu.Lock()
				var eos bool
				finalPosition, eos = c.gonePositions[session], c.goneEOS[session]
				c.mu.Unlock()
				require.True(t, eos, "image not end of stream when it went unavailable")
				if tc.consumer == pollFA {
					c.poll()
					require.False(t, c.fa.IsSessionAssembling(session), "assembler kept the dead image's partial message")
				}
			} else {
				// Java 1.53.3: the exclusive publication reserved the whole message in the tail before writing it,
				// and LogBufferUnblocker only pads a zeroed frame when a later frame is written or the term ends, so
				// the draining publication is never unblocked and the image stays, stuck at the unwritten fragment,
				// until its subscribers close. The Java oracle subscriber must see the same.
				c.pollFor(clientLiveness + unblockTimeout + 3*time.Second)
				require.False(t, c.isGone(session), "Go image went unavailable, Java's does not")
				img := c.imageFor(session)
				// The resume point is the end of the reserved message: the dead publication's final position.
				finalPosition = holder.last("holding").int("position") +
					int64(computeFragmentedLength(tc.size, int(ready.int("maxPayloadLength"))))
				require.Less(t, img.Position(), finalPosition)
				if tc.tr == ipc {
					require.Equal(t, finalPosition, img.EndOfStreamPosition())
				}
				require.NoError(t, c.sub.Close())
				if oracle != nil {
					require.Nil(t, oracle.last("imageGone"), "Java oracle image went unavailable")
					oracle.send("exit")
					select {
					case <-oracle.exited:
					case <-time.After(20 * time.Second):
						t.Fatal("oracle did not exit")
					}
				}
				cur = newConsumer(t, a, tc.consumer, ch.sub, stream, ch.subDests...)
			}
			require.Equal(t, int64(tc.holdAfter), c.session(session).received, "partial message delivered")
			c.requireClean()
			awaitDriverForgets(t, a, session, stream, ready.int("clientId"), 20*time.Second)

			termID := initialTermID + int32(finalPosition/termLength)
			resume := ch
			resume.pub += fmt.Sprintf("|session-id=%d|init-term-id=%d|term-id=%d|term-offset=%d", session,
				initialTermID, termID, finalPosition%termLength)
			p2 := startPeer(t, tc.resumeIn, "pub", pubArgs(resume, stream, "exclusive=true", "mode=offer",
				fmt.Sprintf("size=%d", tc.size), fmt.Sprintf("from=%d", tc.holdAfter), "count=200")...)
			ready2 := p2.await("ready", 30*time.Second, nil)
			require.Equal(t, session, int32(ready2.int("sessionId")))
			require.Equal(t, finalPosition, ready2.int("position"), "resumed publication position")
			p2.await("connected", 30*time.Second, nil)
			cur.pollUntil("resumed messages", 30*time.Second, func() bool {
				return cur.session(session).next == int64(tc.holdAfter+200)
			})
			img := cur.lastImageFor(session)
			require.NotNil(t, img)
			require.Equal(t, finalPosition, img.JoinPosition(), "resumed image join position")
			expected := tc.holdAfter + 200
			if cur != c {
				expected = 200
			}
			require.Equal(t, int64(expected), cur.session(session).received)
			available, _ := cur.imagesFor(session)
			require.Equal(t, 1+boolInt(cur == c), available)
			cur.requireClean()
			p2.send("exit")
			cur.pollUntil("resumed image unavailable", 20*time.Second, func() bool {
				_, gone := cur.imagesFor(session)
				return gone == 1+boolInt(cur == c)
			})
		})
	}
}

func boolInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

// computeFragmentedLength is the term space of a length byte message fragmented at maxPayload.
func computeFragmentedLength(length, maxPayload int) int {
	full := length / maxPayload
	frame := func(n int) int { return (n + 32 + 31) &^ 31 }
	total := full * frame(maxPayload)
	if rem := length % maxPayload; rem > 0 {
		total += frame(rem)
	}
	return total
}
