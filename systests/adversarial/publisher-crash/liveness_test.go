package publishercrash

import (
	"errors"
	"fmt"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/lirm/aeron-go/aeron"
	"github.com/stretchr/testify/require"
)

// TestStoppedPublisherIsTimedOut SIGSTOPs a Go or Java publisher process for longer than the driver's client
// liveness timeout and then SIGCONTs it. Java 1.53.3: the driver times the client out (ON_CLIENT_TIMEOUT), its
// publication drains and the subscriber's image goes unavailable while the publisher is stopped; on resume the
// client terminates with ClientTimeoutException (Go: *aeron.ClientTimeoutError) through the error handler, its
// publication is closed and nothing it offers after the timeout reaches the subscriber.
func TestStoppedPublisherIsTimedOut(t *testing.T) {
	for i, l := range []lang{goLang, javaLang} {
		l := l
		stream := int32(9270 + i)
		port := 27280 + 2*i
		t.Run(string(l), func(t *testing.T) {
			requireAdv(t)
			checkLeaks(t)
			a := connect(t)
			ch := channelsFor(udp, port)
			c := newConsumer(t, a, pollFA, ch.sub, stream)
			p := startPeer(t, l, "pub", pubArgs(ch, stream, "size=3000", "intervalUs=200")...)
			ready := p.await("ready", 30*time.Second, nil)
			session := int32(ready.int("sessionId"))
			c.pollUntil("messages", 30*time.Second, func() bool { return c.session(session).received >= 200 })

			p.signal(syscall.SIGSTOP)
			c.pollUntil("unavailable image of the stopped publisher", 3*clientLiveness+10*time.Second, func() bool {
				return c.isGone(session)
			})
			received := c.session(session)
			awaitDriverForgets(t, a, session, stream, ready.int("clientId"), 20*time.Second)
			p.signal(syscall.SIGCONT)

			errEvent := p.await("error", 20*time.Second, func(e event) bool {
				return strings.HasPrefix(e.str("type"), "ClientTimeout")
			})
			t.Logf("%s publisher error: %v", l, errEvent)
			p.await("fatal", 20*time.Second, nil)
			select {
			case <-p.exited:
			case <-time.After(20 * time.Second):
				t.Fatal("timed out publisher did not exit")
			}
			c.pollFor(500 * time.Millisecond)
			require.Equal(t, received, c.session(session), "zombie publisher delivered data after its timeout")
			require.GreaterOrEqual(t, received.next, int64(200))
			c.requireClean()
		})
	}
}

// TestSubscriberProcessKilled SIGKILLs a Go or Java subscriber process while the test publishes to it. Java
// 1.53.3: the driver times the subscriber's client out, the publication becomes not connected (offers return
// NOT_CONNECTED, never block), and a new subscriber process receives a gap free, uncorrupted stream from its join
// point while the publication carries on.
func TestSubscriberProcessKilled(t *testing.T) {
	cases := []struct {
		lang lang
		tr   transport
		excl bool
	}{
		{goLang, ipc, false}, {goLang, udp, true}, {goLang, mdcDynamic, false},
		{javaLang, ipc, true}, {javaLang, udp, false}, {javaLang, multicast, false},
	}
	for i, tc := range cases {
		tc := tc
		stream := int32(9272 + i)
		port := 27284 + 2*i
		t.Run(fmt.Sprintf("%s/%s/exclusive=%t", tc.lang, tc.tr, tc.excl), func(t *testing.T) {
			requireAdv(t)
			checkLeaks(t)
			a := connect(t)
			ch := channelsFor(tc.tr, port)
			var pub aeron.Publisher
			var err error
			if tc.excl {
				pub, err = a.AddExclusivePublication(ch.pub, stream)
			} else {
				pub, err = a.AddPublication(ch.pub, stream)
			}
			require.NoError(t, err)
			defer pub.Close()
			subArgs := []string{"channel=" + ch.sub, fmt.Sprintf("stream=%d", stream)}
			pump := newPump(t, pub, 3000)

			s1 := startPeer(t, tc.lang, "sub", subArgs...)
			s1.await("image", 30*time.Second, nil)
			pump.until(t, "first subscriber receives", 30*time.Second, func() bool {
				e := s1.last("progress")
				return e != nil && e.int("received") >= 300
			})
			s1.killAndWait()
			pump.until(t, "publication not connected", 3*clientLiveness+10*time.Second, func() bool {
				return !pub.IsConnected()
			})
			start := time.Now()
			r := pump.offerOnce()
			require.Equal(t, aeron.NotConnected, r)
			require.Less(t, time.Since(start), time.Second, "offer blocked")
			awaitDriverForgets(t, a, -999999, stream, s1.await("ready", 0, nil).int("clientId"), 20*time.Second)

			s2 := startPeer(t, tc.lang, "sub", subArgs...)
			s2.await("image", 30*time.Second, nil)
			pump.until(t, "second subscriber receives", 30*time.Second, func() bool {
				e := s2.last("progress")
				return e != nil && e.int("received") >= 300
			})
			sessions, images := s2.report()
			require.EqualValues(t, 1, images)
			st := sessions[pub.SessionID()]
			require.NotNil(t, st)
			require.Zero(t, st.int("gaps")+st.int("duplicates")+st.int("corrupt"), "%v", st)
			require.Equal(t, st.int("next")-st.int("first"), st.int("received"))
		})
	}
}

// pump offers sequenced messages from the test goroutine.
type pump struct {
	pub  aeron.Publisher
	msg  []byte
	next int64
}

func newPump(t *testing.T, pub aeron.Publisher, size int) *pump {
	return &pump{pub: pub, msg: make([]byte, size)}
}

func (p *pump) offerOnce() int64 {
	r := offerSeq(p.pub, p.msg, p.next)
	if r > 0 {
		p.next++
	}
	return r
}

func (p *pump) until(t *testing.T, what string, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %v waiting for %s (offered %d)", timeout, what, p.next)
		}
		r := p.offerOnce()
		if r < 0 && r != aeron.NotConnected && r != aeron.BackPressured && r != aeron.AdminAction {
			t.Fatalf("offer: %d", r)
		}
		time.Sleep(200 * time.Microsecond)
	}
}

// TestDriverKilledUnderPeers SIGKILLs the driver container while a Go publisher process, a Java subscriber
// process and the test's in-process subscriber are connected. Java 1.53.3: every client detects the dead driver
// within its driver timeout and terminates with DriverTimeoutException (Go: *aeron.DriverTimeoutError), nothing
// hangs, and after the driver is restarted new processes on the same channel resume.
func TestDriverKilledUnderPeers(t *testing.T) {
	requireAdv(t)
	checkLeaks(t)
	const stream = int32(9280)
	ch := channelsFor(udp, 27298)
	a := connect(t)
	c := newConsumer(t, a, controlledCFA, ch.sub, stream)
	pub := startPeer(t, goLang, "pub", pubArgs(ch, stream, "size=3000", "intervalUs=200")...)
	sub := startPeer(t, javaLang, "sub", "channel="+ch.sub, fmt.Sprintf("stream=%d", stream))
	session := int32(pub.await("ready", 30*time.Second, nil).int("sessionId"))
	c.pollUntil("messages", 30*time.Second, func() bool { return c.session(session).received >= 200 })
	sub.await("progress", 30*time.Second, nil)

	docker(t, "kill", "-s", "KILL", advDriver)
	c.pollUntil("in-process client terminated", 15*time.Second, a.IsClosed)
	var timeoutErr *aeron.DriverTimeoutError
	require.True(t, errors.As(a.TerminalError(), &timeoutErr), "terminal error: %v", a.TerminalError())
	for _, p := range []*peer{pub, sub} {
		e := p.await("error", 20*time.Second, func(e event) bool { return strings.HasPrefix(e.str("type"), "DriverTimeout") })
		t.Logf("%s: %v", p.name, e)
		select {
		case <-p.exited:
		case <-time.After(20 * time.Second):
			t.Fatalf("%s did not exit after the driver died", p.name)
		}
	}
	c.requireClean()
	require.NoError(t, a.Close())

	restartDriver(t)
	a2 := connect(t)
	c2 := newConsumer(t, a2, pollFA, ch.sub, stream)
	pub2 := startPeer(t, javaLang, "pub", pubArgs(ch, stream, "size=3000", "intervalUs=200", "count=500")...)
	session2 := int32(pub2.await("ready", 30*time.Second, nil).int("sessionId"))
	c2.pollUntil("messages after driver restart", 30*time.Second, func() bool { return c2.session(session2).next == 500 })
	c2.requireClean()
}
