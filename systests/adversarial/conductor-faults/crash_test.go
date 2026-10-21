package conductorfaults

import (
	"fmt"
	"strconv"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/lirm/aeron-go/aeron"
	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/logbuffer"
	"github.com/stretchr/testify/require"
)

// sessionLog verifies per-session ordering and payload integrity of everything a subscription receives.
type sessionLog struct {
	mu          sync.Mutex
	next        map[int32]int64
	problems    []string
	available   map[int32]bool
	unavailable map[int32]bool
	// rejoin lets a new image of a session start at any sequence (a late join after the previous image of the
	// session went unavailable); ordering is then verified from that first message on.
	rejoin bool
}

func newSessionLog() *sessionLog {
	return &sessionLog{next: map[int32]int64{}, available: map[int32]bool{}, unavailable: map[int32]bool{}}
}

func (l *sessionLog) onFragment(buf *atomic.Buffer, offset, length int32, h *logbuffer.Header) {
	seq, err := decode(buf.GetBytesArray(offset, length))
	l.mu.Lock()
	defer l.mu.Unlock()
	sid := h.SessionId()
	if err != nil {
		l.problems = append(l.problems, err.Error())
	} else if want, ok := l.next[sid]; ok && want < 0 {
		// first message of a rejoined image
	} else if seq != want {
		l.problems = append(l.problems, fmt.Sprintf("session %d: got seq %d want %d", sid, seq, want))
	}
	l.next[sid] = seq + 1
}

func (l *sessionLog) received(sid int32) int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.next[sid]
}

func (l *sessionLog) imageEvent(m map[int32]bool) func(aeron.Image) {
	return func(img aeron.Image) {
		l.mu.Lock()
		m[img.SessionID()] = true
		l.mu.Unlock()
	}
}

// onAvailable records the image and, with rejoin set, lets the new image start at any sequence.
func (l *sessionLog) onAvailable(img aeron.Image) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.available[img.SessionID()] = true
	if l.rejoin {
		l.next[img.SessionID()] = -1
	}
}

func (l *sessionLog) is(m map[int32]bool, sid int32) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return m[sid]
}

// pollUntil polls sub until cond holds.
func pollUntil(t *testing.T, sub *aeron.Subscription, handler func(*atomic.Buffer, int32, int32, *logbuffer.Header),
	timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	awaitTrue(t, timeout, what, func() bool {
		for sub.Poll(handler, 64) > 0 {
		}
		return cond()
	})
}

// TestPublisherProcessSIGKILL kill -9s a child Go publisher process (concurrent/exclusive, Offer/TryClaim, IPC/UDP,
// fragmented) mid stream. The surviving subscriber must see an ordered, gap free, uncorrupted prefix, the
// unavailable image handler must fire, the driver must reclaim every counter of the dead client after the client
// liveness timeout, and a new publisher process must be received from sequence 0.
func TestPublisherProcessSIGKILL(t *testing.T) {
	setup(t)
	cases := []struct {
		name             string
		udp              bool
		exclusive, claim bool
		size             int
	}{
		{name: "IPCConcurrentOffer", size: 64},
		{name: "IPCExclusiveTryClaim", exclusive: true, claim: true, size: 256},
		{name: "UDPConcurrentFragmented", udp: true, size: 5000},
		{name: "UDPExclusiveTryClaim", udp: true, exclusive: true, claim: true, size: 512},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			before := snapshot(t)
			observer := connect(t, 5*time.Second)
			c := connect(t, 5*time.Second)
			channel, stream := "aeron:ipc", nextStream()
			if tc.udp {
				channel = udp(nextPort())
			}
			log := newSessionLog()
			sub, err := c.AddSubscriptionWithHandlers(channel, stream, log.imageEvent(log.available),
				log.imageEvent(log.unavailable))
			require.NoError(t, err)
			handler := aeron.NewFragmentAssembler(log.onFragment, 0).OnFragment

			args := []string{"-mode", "pub", "-channel", channel, "-stream", strconv.Itoa(int(stream)),
				"-exclusive=" + strconv.FormatBool(tc.exclusive), "-claim=" + strconv.FormatBool(tc.claim),
				"-size", strconv.Itoa(tc.size), "-interval", "20us"}
			p := startHelper(t, args...)
			ready := p.await("ready", 30*time.Second)
			childID, _ := strconv.ParseInt(ready[1], 10, 64)
			sid64, _ := strconv.ParseInt(ready[2], 10, 32)
			sid := int32(sid64)
			pollUntil(t, sub, handler, 20*time.Second, "messages from child", func() bool { return log.received(sid) >= 2000 })

			p.signal(t, syscall.SIGKILL)
			p.awaitExit(t, 5*time.Second)
			pollUntil(t, sub, handler, 20*time.Second, "unavailable image", func() bool { return log.is(log.unavailable, sid) })
			t.Logf("received %d messages before kill -9", log.received(sid))
			awaitNoClientCounters(t, observer, childID, 20*time.Second)

			p2 := startHelper(t, args...)
			ready2 := p2.await("ready", 30*time.Second)
			sid2, _ := strconv.ParseInt(ready2[2], 10, 32)
			require.NotEqual(t, sid, int32(sid2))
			pollUntil(t, sub, handler, 20*time.Second, "messages from new child", func() bool {
				return log.received(int32(sid2)) >= 1000
			})
			p2.signal(t, syscall.SIGKILL)
			p2.awaitExit(t, 5*time.Second)
			log.mu.Lock()
			require.Empty(t, log.problems)
			log.mu.Unlock()
			require.Empty(t, c.errs.all())
			require.NoError(t, sub.Close())
			require.NoError(t, c.Close())
			require.NoError(t, observer.Close())
			assertNoLeaks(t, before)
		})
	}
}

// TestSubscriberProcessSIGKILL kill -9s a child subscriber process: the in-process publisher keeps publishing
// without errors, the driver reclaims the dead client's counters, and a new subscriber process receives data.
func TestSubscriberProcessSIGKILL(t *testing.T) {
	setup(t)
	before := snapshot(t)
	observer := connect(t, 5*time.Second)
	c := connect(t, 5*time.Second)
	channel, stream := udp(nextPort()), nextStream()
	pub, err := c.AddExclusivePublication(channel, stream)
	require.NoError(t, err)
	args := []string{"-mode", "sub", "-channel", channel, "-stream", strconv.Itoa(int(stream)), "-report", "100"}
	p := startHelper(t, args...)
	ready := p.await("ready", 30*time.Second)
	childID, _ := strconv.ParseInt(ready[1], 10, 64)

	msg := make([]byte, 128)
	buf := atomic.MakeBuffer(msg)
	seq := int64(0)
	publish := func(until func() bool, what string) {
		awaitTrue(t, 20*time.Second, what, func() bool {
			encode(msg, seq)
			r := pub.Offer(buf, 0, int32(len(msg)), nil)
			require.False(t, r == aeron.PublicationClosed || r == aeron.MaxPositionExceeded, "offer %d", r)
			if r > 0 {
				seq++
			}
			return until()
		})
	}
	publish(func() bool { return p.has("recv 999") }, "child received")
	require.False(t, p.has("gap"))
	p.signal(t, syscall.SIGKILL)
	p.awaitExit(t, 5*time.Second)
	awaitNoClientCounters(t, observer, childID, 20*time.Second)
	publish(func() bool { return !pub.IsConnected() }, "publication not connected")

	p2 := startHelper(t, args...)
	p2.await("ready", 30*time.Second)
	seq = 0
	publish(func() bool { return p2.has("recv 99") || p2.has("gap") }, "new child receives")
	require.False(t, p2.has("gap"))
	require.Empty(t, c.errs.all())
	require.NoError(t, pub.Close())
	require.NoError(t, c.Close())
	require.NoError(t, observer.Close())
	p2.signal(t, syscall.SIGKILL)
	p2.awaitExit(t, 5*time.Second)
	assertNoLeaks(t, before)
}

// TestClientProcessSIGSTOPTimedOutByDriver SIGSTOPs a child subscriber process for longer than the driver's client
// liveness timeout. The driver must reclaim the client; after SIGCONT the child must terminate with a typed timeout
// (Java checks the conductor inter-service interval first, so ConductorServiceTimeoutException, else
// ClientTimeoutException on ON_CLIENT_TIMEOUT) instead of hanging.
func TestClientProcessSIGSTOPTimedOutByDriver(t *testing.T) {
	setup(t)
	observer := connect(t, 5*time.Second)
	p := startHelper(t, "-mode", "sub", "-channel", "aeron:ipc", "-stream", strconv.Itoa(int(nextStream())))
	ready := p.await("ready", 30*time.Second)
	childID, _ := strconv.ParseInt(ready[1], 10, 64)
	awaitTrue(t, 5*time.Second, "child heartbeat", func() bool {
		return heartbeatCounter(observer.CounterReader(), childID) >= 0
	})
	p.signal(t, syscall.SIGSTOP)
	awaitNoClientCounters(t, observer, childID, 20*time.Second)
	p.signal(t, syscall.SIGCONT)
	terminal := p.await("terminal", 15*time.Second)
	t.Logf("child terminal: %v", terminal)
	require.Contains(t, []string{"*aeron.ConductorServiceTimeoutError", "*aeron.ClientTimeoutError"}, terminal[1])
	p.awaitExit(t, 10*time.Second)
	requireIPCRoundTrip(t)
}
