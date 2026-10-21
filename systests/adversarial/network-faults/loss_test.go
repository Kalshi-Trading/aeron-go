package networkfaults

import (
	"fmt"
	"testing"
	"time"

	"github.com/lirm/aeron-go/aeron"
	"github.com/stretchr/testify/require"
)

// lossCase streams count messages from a Go publisher on driver a to a Go subscriber on driver b over unicast UDP
// while rule degrades the path, and requires reliable, ordered, uncorrupted delivery plus NAK/retransmit activity.
type lossCase struct {
	name       string
	rule       func(dataPort int) netemRule
	count      int64
	size       func(int64) int
	exclusive  bool
	mode       pubMode
	controlled bool
	host       string
	params     string
	wantNaks   bool
}

func runLossCase(t *testing.T, lc lossCase) {
	requireStack(t)
	base := snapshot(t)
	a, b := connect(t, dirA), connect(t, dirB)
	p, stream := port(), streamID()
	host := lc.host
	if host == "" {
		host = "localhost"
	}
	tag := fmt.Sprintf("nfloss-%d-%d", p, time.Now().UnixNano())
	channel := udp(host, p, "alias="+tag+lc.params)
	sub, err := b.AddSubscription(channel, stream)
	require.NoError(t, err)
	var pub aeron.Publisher
	if lc.exclusive {
		pub, err = a.AddExclusivePublication(channel, stream)
	} else {
		pub, err = a.AddPublication(channel, stream)
	}
	require.NoError(t, err)
	awaitTrue(t, "publication connected", 10*time.Second, pub.IsConnected)

	naksBefore, rtxBefore := systemCounter(b, "NAKs sent"), systemCounter(a, "Retransmits sent")
	netem(t, lc.rule(p))
	size := lc.size
	if size == nil {
		size = smallSize
	}
	pb := newPublisher(pub, lc.mode, size)
	done := pb.publishAsync(lc.count, 0, 2*time.Minute)
	c := newConsumer(sub, 0, lc.controlled)
	require.NoError(t, c.consume(lc.count, 2*time.Minute), netemStats(t))
	require.NoError(t, <-done)
	require.Equal(t, lc.count, c.checker.Received, c.checker.String())
	naks, rtx := systemCounter(b, "NAKs sent")-naksBefore, systemCounter(a, "Retransmits sent")-rtxBefore
	t.Logf("%s: %s naks=%d retransmits=%d aborts=%d", lc.name, c.checker, naks, rtx, c.aborts)
	if lc.wantNaks {
		require.Positive(t, naks, "receiver sent no NAKs: fault not active?\n%s", netemStats(t))
		require.Positive(t, rtx, "sender retransmitted nothing")
	}
	if lc.controlled {
		require.Positive(t, c.aborts)
	}
	noImageEvent(t, b.unavailable, "unavailable")
	require.Empty(t, a.errors.all())
	require.Empty(t, b.errors.all())

	clearNetem(t)
	require.NoError(t, pub.Close())
	require.NoError(t, sub.Close())
	awaitNoChannelCounters(t, a, tag, 10*time.Second)
	awaitNoChannelCounters(t, b, tag, 10*time.Second)
	requireNoLeaks(t, base, a, b)
}

// TestLossRates: random loss of data frames from 1% to 50%. NAK driven retransmission must deliver every message
// exactly once, in order, with small and fragmented messages.
func TestLossRates(t *testing.T) {
	for _, rate := range []int{1, 5, 20, 50} {
		rate := rate
		count := int64(20000)
		if rate >= 50 {
			count = 3000
		}
		t.Run(fmt.Sprintf("%dpct", rate), func(t *testing.T) {
			runLossCase(t, lossCase{name: fmt.Sprintf("loss %d%%", rate), count: count, size: mixedSize, wantNaks: true,
				rule: func(p int) netemRule { return toPorts(fmt.Sprintf("loss %d%%", rate), p) }})
		})
	}
}

// TestLossBothDirections drops data, status messages and NAKs alike (loss on every packet to or from the port).
func TestLossBothDirections(t *testing.T) {
	runLossCase(t, lossCase{name: "bidirectional 20%", count: 10000, size: mixedSize, wantNaks: true,
		rule: func(p int) netemRule { return toBoth("loss 20%", p) }})
}

// TestLossPublicationTypes covers every publish/consume API under 10% loss.
func TestLossPublicationTypes(t *testing.T) {
	loss := func(p int) netemRule { return toPorts("loss 10%", p) }
	cases := []lossCase{
		{name: "concurrent-offer", count: 10000, size: mixedSize},
		{name: "exclusive-offer", count: 10000, size: mixedSize, exclusive: true},
		{name: "concurrent-tryclaim", count: 10000, mode: modeTryClaim},
		{name: "exclusive-tryclaim", count: 10000, mode: modeTryClaim, exclusive: true},
		{name: "controlled-poll", count: 10000, size: mixedSize, controlled: true},
		{name: "large-fragmented", count: 60, size: func(seq int64) int { return 64<<10 + int(seq)*7919 }, params: "term-length=8m"},
		{name: "small-mtu", count: 5000, size: mixedSize, params: "mtu=576"},
	}
	for _, lc := range cases {
		lc := lc
		lc.rule, lc.wantNaks = loss, true
		t.Run(lc.name, func(t *testing.T) { runLossCase(t, lc) })
	}
}

// TestReorderDuplicateJitter: delay with jitter, reordering and duplication. The image must deliver each message
// exactly once and in order.
func TestReorderDuplicateJitter(t *testing.T) {
	runLossCase(t, lossCase{name: "reorder+dup+jitter", count: 10000, size: mixedSize,
		rule: func(p int) netemRule { return toBoth("delay 2ms 2ms reorder 25% 50% duplicate 20%", p) }})
}

// TestCorruption: netem flips a bit in 10% of the packets. The kernel's UDP checksum discards them, so they are just
// loss to Aeron, which must never deliver a corrupt message.
func TestCorruption(t *testing.T) {
	runLossCase(t, lossCase{name: "corrupt 10%", count: 10000, size: mixedSize, wantNaks: true,
		rule: func(p int) netemRule { return toBoth("corrupt 10%", p) }})
}

// TestMTULimitedPath: an 8k Aeron mtu over a path with a 1280 byte IP MTU, so every large frame is IP fragmented and
// losing one fragment loses the whole datagram.
func TestMTULimitedPath(t *testing.T) {
	runLossCase(t, lossCase{name: "mtu 1280 path + 3% loss", count: 5000, size: mixedSize, host: mtuLimitedAddr,
		params: "mtu=8192", wantNaks: true,
		rule: func(p int) netemRule {
			limitMTU(t, 1280)
			return toPorts("loss 3%", p)
		}})
}
