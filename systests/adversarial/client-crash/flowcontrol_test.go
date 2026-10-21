package clientcrash

import (
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lirm/aeron-go/aeron"
	ab "github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/e2e"
	"github.com/stretchr/testify/require"
)

// hotPublisher offers sequenced messages on its own goroutine as fast as flow control allows.
type hotPublisher struct {
	pub        *aeron.ExclusivePublication
	next       atomic.Int64 // next sequence to offer
	lastOK     atomic.Int64 // unix nanos of the last successful offer
	failure    atomic.Value // unexpected offer result
	stop, done chan struct{}
}

func startHotPublisher(t *testing.T, pub *aeron.ExclusivePublication, size int) *hotPublisher {
	h := &hotPublisher{pub: pub, stop: make(chan struct{}), done: make(chan struct{})}
	h.lastOK.Store(time.Now().UnixNano())
	msg := make([]byte, size)
	buf := ab.MakeBuffer(msg)
	go func() {
		defer close(h.done)
		for {
			select {
			case <-h.stop:
				return
			default:
			}
			seq := h.next.Load()
			e2e.EncodeMessage(msg, seq)
			r := pub.Offer(buf, 0, int32(size), nil)
			switch {
			case r > 0:
				h.next.Store(seq + 1)
				h.lastOK.Store(time.Now().UnixNano())
			case r == aeron.BackPressured || r == aeron.NotConnected || r == aeron.AdminAction:
				time.Sleep(20 * time.Microsecond)
			default:
				h.failure.Store(fmt.Sprintf("offer seq %d: %d", seq, r))
				return
			}
		}
	}()
	t.Cleanup(h.close)
	return h
}

func (h *hotPublisher) close() {
	select {
	case <-h.stop:
	default:
		close(h.stop)
	}
	<-h.done
}

func (h *hotPublisher) stalledFor() time.Duration {
	return time.Since(time.Unix(0, h.lastOK.Load()))
}

type fcCase struct {
	name    string
	pubChan string
	subChan string
	stream  int32
	java    bool
}

// TestKillSubscriberUnblocksFlowControl SIGKILLs a subscriber process (Go crashpeer or Java InteropTool) that is
// consuming a stream alongside a subscriber in the test process. The dead subscriber's position stops, so the
// publication back-pressures; as in Java, once the driver times the client out it removes that subscriber
// position (and for UDP the receiver's min-flow-control window follows the remaining subscriber), so the publisher
// must resume within the client liveness timeout without any gap for the surviving subscriber. A replacement
// subscriber process must then join and consume.
func TestKillSubscriberUnblocksFlowControl(t *testing.T) {
	const tl = "term-length=65536"
	cases := []fcCase{
		{name: "ipc", pubChan: "aeron:ipc?" + tl + "|alias=advk-fc-ipc", subChan: "aeron:ipc", stream: 9120},
		{name: "udp-unicast", pubChan: "aeron:udp?endpoint=localhost:27120|" + tl + "|alias=advk-fc-udp",
			subChan: "aeron:udp?endpoint=localhost:27120", stream: 9121},
		{name: "mdc-dynamic-min-fc",
			pubChan: "aeron:udp?control=localhost:27121|control-mode=dynamic|fc=min|" + tl + "|alias=advk-fc-mdc",
			subChan: "aeron:udp?endpoint=localhost:27122|control=localhost:27121|control-mode=dynamic", stream: 9122},
		{name: "multicast-min-fc",
			pubChan: "aeron:udp?endpoint=224.0.1.1:27123|interface=localhost|fc=min|" + tl + "|alias=advk-fc-mc",
			subChan: "aeron:udp?endpoint=224.0.1.1:27123|interface=localhost", stream: 9123},
		{name: "java-ipc", pubChan: "aeron:ipc?" + tl + "|alias=advk-fc-jipc", subChan: "aeron:ipc", stream: 9124,
			java: true},
		{name: "java-udp", pubChan: "aeron:udp?endpoint=localhost:27124|" + tl + "|alias=advk-fc-judp",
			subChan: "aeron:udp?endpoint=localhost:27124", stream: 9125, java: true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			requireAdv(t)
			leaks := baseline()
			a := connect(t, newContext())
			s := newLiveSub(t, a, c.subChan, c.stream)
			startSub := func(label string) *peer {
				if c.java {
					return startJava(t, label, "sub", "channel="+c.subChan, fmt.Sprintf("stream=%d", c.stream),
						"count=1000000000000", "timeoutMs=600000")
				}
				return startPeer(t, label, "-mode", "sub", "-channel", c.subChan, "-stream", fmt.Sprint(c.stream))
			}
			heartbeats := len(counterLabels(a, "client-heartbeat"))
			victim := startSub("victim")
			victim.await("ready", 30*time.Second)

			pub, err := a.AddExclusivePublication(c.pubChan, c.stream)
			require.NoError(t, err)
			require.NoError(t, e2e.WaitConnected(pub, 20*time.Second))
			awaitTrue(t, "both subscribers to join", 20*time.Second, func() bool {
				return len(counterLabels(a, "sub-pos")) >= 2
			})
			h := startHotPublisher(t, pub, 64)
			s.awaitReceived(pub.SessionID(), 2000, 20*time.Second)
			if !c.java {
				awaitTrue(t, "victim to consume", 10*time.Second, func() bool {
					return victim.last("recv").int("next") > 1000
				})
			}

			victim.kill()
			// The window fills up behind the dead subscriber...
			awaitTrue(t, "publication to back-pressure behind the dead subscriber", 5*time.Second, func() bool {
				return h.stalledFor() > 300*time.Millisecond
			})
			stalledAt := h.next.Load()
			// ...and must open again once the driver reclaims the dead client.
			unblocked := awaitTrue(t, "publication to resume", reclaimTO, func() bool {
				return h.next.Load() > stalledAt+1000
			})
			require.Nil(t, h.failure.Load())
			s.awaitReceived(pub.SessionID(), stalledAt+1000, 10*time.Second)
			awaitTrue(t, "dead client counters to be reclaimed", reclaimTO, func() bool {
				return len(counterLabels(a, "client-heartbeat")) == heartbeats && len(counterLabels(a, "sub-pos")) == 1
			})
			t.Logf("publisher blocked at seq %d for %v after the kill, %s", stalledAt, unblocked,
				ptr(s.checker(pub.SessionID())))

			replacement := startSub("replacement")
			replacement.await("ready", 30*time.Second)
			if !c.java {
				joinedAt := h.next.Load()
				awaitTrue(t, "replacement to consume", 20*time.Second, func() bool {
					return replacement.last("recv").int("next") > joinedAt+1000
				})
			} else {
				before := h.next.Load()
				awaitTrue(t, "flow with the replacement Java subscriber", 20*time.Second, func() bool {
					return h.next.Load() > before+10000
				})
			}
			replacement.kill()
			h.close()
			require.Nil(t, h.failure.Load())
			require.NoError(t, ptr(s.checker(pub.SessionID())).Err())
			s.close()
			require.NoError(t, a.Close())
			leaks.requireNoLeaks(t)
		})
	}
}
