package mixedmatrix

import (
	"fmt"
	"math/rand"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lirm/aeron-go/aeron"
	"github.com/lirm/aeron-go/systests/adversarial/mixed-matrix/internal/mm"
	"github.com/stretchr/testify/require"
)

// poller polls in-process subscriptions on its own goroutine until stopped.
type poller struct {
	mu   sync.Mutex
	subs []*mm.Sub
	stop chan struct{}
	done chan struct{}
}

func startPoller(t *testing.T, subs ...*mm.Sub) *poller {
	p := &poller{subs: subs, stop: make(chan struct{}), done: make(chan struct{})}
	go func() {
		defer close(p.done)
		for {
			select {
			case <-p.stop:
				return
			default:
			}
			p.mu.Lock()
			n := 0
			for _, s := range p.subs {
				if !s.Sub.IsClosed() {
					n += s.Poll()
				}
			}
			p.mu.Unlock()
			if n == 0 {
				time.Sleep(50 * time.Microsecond)
			}
		}
	}()
	t.Cleanup(p.Stop)
	return p
}

func (p *poller) Stop() {
	select {
	case <-p.stop:
	default:
		close(p.stop)
	}
	<-p.done
}

// pubLoop offers from an in-process publisher until stopped, recording any terminal error.
type pubLoop struct {
	p    *mm.Pub
	stop chan struct{}
	done chan struct{}
	err  error
}

func startPubLoop(t *testing.T, p *mm.Pub, rate time.Duration) *pubLoop {
	l := &pubLoop{p: p, stop: make(chan struct{}), done: make(chan struct{})}
	go func() {
		defer close(l.done)
		for {
			select {
			case <-l.stop:
				return
			default:
			}
			n, err := p.Step()
			if err != nil {
				l.err = err
				return
			}
			if n > 0 {
				time.Sleep(rate)
			} else {
				time.Sleep(20 * time.Microsecond)
			}
		}
	}()
	t.Cleanup(l.Stop)
	return l
}

func (l *pubLoop) Stop() {
	select {
	case <-l.stop:
	default:
		close(l.stop)
	}
	<-l.done
}

func writerCount(tr *mm.Tracker, w uint8) (int64, uint32) {
	var n int64
	var next uint32
	for _, img := range tr.Images() {
		if ws := img.Writers[w]; ws != nil {
			n += ws.Count
			next = ws.Next
		}
	}
	return n, next
}

// TestDriverFreezeBelowTimeout SIGSTOPs the driver for less than the driver timeout while Go and Java
// publishers and subscribers (in-process and child processes) are streaming over IPC and UDP. Java clients ride
// this out without errors and reliable streams lose nothing; so must Go.
func TestDriverFreezeBelowTimeout(t *testing.T) {
	requireAdv(t)
	rnd := seed(t)
	base := snapshot()
	freshStack(t)
	counters := driverCounters(t)
	for i, ch := range []string{"aeron:ipc?" + termParams, "aeron:udp?endpoint=localhost:27720|" + termParams} {
		stream := int32(9750 + i)
		subCh := strings.TrimSuffix(strings.TrimSuffix(ch, termParams), "|")
		subCh = strings.TrimSuffix(subCh, "?")
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			cl := connect(t)
			anchor, err := mm.AddSub(cl.Aeron, mm.SubControlled, subCh, stream, rand.New(rand.NewSource(rnd.Int63())), false)
			require.NoError(t, err)
			pl := startPoller(t, anchor)
			javaSub := startPeer(t, "java", "sub", "channel="+subCh, "stream="+strconv.Itoa(int(stream)))
			javaSub.await("ready", 30*time.Second)
			goPub := startPeer(t, "go", "pub", "channel="+ch, "stream="+strconv.Itoa(int(stream)), "kind=offerblock",
				"writer=1", "sizes="+mm.FormatSizes(cellSizes), "rateUs=300")
			javaPub := startPeer(t, "java", "pub", "channel="+ch, "stream="+strconv.Itoa(int(stream)),
				"exclusive=true", "writer=2", "sizes="+mm.FormatSizes(cellSizes), "rateUs=300")
			goPub.await("ready", 20*time.Second)
			javaPub.await("ready", 30*time.Second)
			awaitTrue(t, "traffic from both publishers", 20*time.Second, func() bool {
				a, _ := writerCount(anchor.Tracker, 1)
				b, _ := writerCount(anchor.Tracker, 2)
				return a > 100 && b > 100
			})
			driverSignal(t, "STOP")
			time.Sleep(driverTimeout / 2)
			driverSignal(t, "CONT")
			time.Sleep(2 * time.Second)
			c1 := goPub.close("closed")
			c2 := javaPub.close("closed")
			for _, c := range []struct {
				w    uint8
				next int64
			}{{1, c1.Int("next")}, {2, c2.Int("next")}} {
				awaitTrue(t, fmt.Sprintf("writer %d complete", c.w), 20*time.Second, func() bool {
					n, next := writerCount(anchor.Tracker, c.w)
					return n == c.next && int64(next) == c.next
				})
			}
			requireCleanReports(t, javaSub)
			pl.Stop()
			requireCleanTracker(t, "anchor", anchor.Tracker)
			require.Empty(t, cl.Errors(), "client errors after a short driver freeze")
			require.NoError(t, anchor.Close())
			require.NoError(t, cl.Close())
		})
	}
	requireCountersReturn(t, counters)
	requireNoLeaks(t, base)
}

// TestDriverFreezeBeyondTimeout SIGSTOPs the driver for longer than the driver timeout. Java clients (child
// MatrixPeer) and Go clients (in-process and mmpeer children) must all detect the dead driver within the driver
// timeout: DriverTimeoutException / *DriverTimeoutError via the error handler, unavailable-image handlers, closed
// publications and subscriptions, and no blocking call may hang. After the driver resumes (and times its old
// clients out) fresh clients must work.
func TestDriverFreezeBeyondTimeout(t *testing.T) {
	requireAdv(t)
	rnd := seed(t)
	base := snapshot()
	freshStack(t)
	ch := "aeron:udp?endpoint=localhost:27722|" + termParams
	subCh := "aeron:udp?endpoint=localhost:27722"
	stream := int32(9752)
	streamArg := "stream=" + strconv.Itoa(int(stream))

	cl := connect(t)
	anchor, err := mm.AddSub(cl.Aeron, mm.SubPoll, subCh, stream, rand.New(rand.NewSource(rnd.Int63())), false)
	require.NoError(t, err)
	pl := startPoller(t, anchor)
	inPub, err := mm.AddPub(cl.Aeron, mm.PubExclusive, ch, stream, 9, cellSizes)
	require.NoError(t, err)
	loop := startPubLoop(t, inPub, 300*time.Microsecond)
	peers := []*peer{
		startPeer(t, "go", "pub", "channel="+ch, streamArg, "kind=tryclaim", "writer=1", "sizes=64,0,1,1376", "rateUs=500"),
		startPeer(t, "java", "pub", "channel="+ch, streamArg, "writer=2", "sizes=64,0,1,1376", "rateUs=500"),
		startPeer(t, "go", "sub", "channel="+subCh, streamArg, "kind=controlled"),
		startPeer(t, "java", "sub", "channel="+subCh, streamArg),
	}
	for _, p := range peers {
		p.await("ready", 30*time.Second)
	}
	awaitTrue(t, "traffic", 20*time.Second, func() bool { return anchor.Tracker.Messages() > 500 })

	driverSignal(t, "STOP")
	frozen := time.Now()
	defer driverSignal(t, "CONT")
	// in-process: a blocking command must fail within the driver timeout rather than hang
	start := time.Now()
	_, addErr := cl.AddPublication("aeron:ipc", 9753)
	require.Error(t, addErr)
	require.Less(t, time.Since(start), driverTimeout+3*time.Second, "AddPublication against a frozen driver")
	awaitTrue(t, "Go client to close on driver timeout", driverTimeout+5*time.Second, cl.IsClosed)
	requireDriverTimeout(t, cl, addErr)
	// the loop exits by itself on its first offer after the close; Stop alone could end it before that offer
	select {
	case <-loop.done:
	case <-time.After(5 * time.Second):
	}
	loop.Stop()
	require.Error(t, loop.err, "offer after driver timeout")
	require.Contains(t, loop.err.Error(), strconv.Itoa(int(aeron.PublicationClosed)))
	awaitTrue(t, "anchor images unavailable", 5*time.Second, func() bool {
		for _, img := range anchor.Tracker.Images() {
			if !img.Unavailable {
				return false
			}
		}
		return true
	})
	require.True(t, anchor.Sub.IsClosed())
	// child peers of both languages detect the timeout too and exit with an error
	for _, p := range peers {
		requireTerminatedWith(t, p, "drivertimeout", driverTimeout+10*time.Second)
	}
	t.Logf("all clients detected the frozen driver within %v", time.Since(frozen))
	pl.Stop()
	requireCleanTracker(t, "anchor", anchor.Tracker)

	driverSignal(t, "CONT")
	// the resumed driver times out the dead clients; a new client and new publications/subscriptions work
	c2 := connect(t)
	sub2, err := mm.AddSub(c2.Aeron, mm.SubPoll, subCh, stream, rnd, false)
	require.NoError(t, err)
	pl2 := startPoller(t, sub2)
	jp := startPeer(t, "java", "pub", "channel="+ch, streamArg, "exclusive=true", "writer=3", "sizes=64,5000", "rateUs=200")
	jp.await("ready", 30*time.Second)
	awaitTrue(t, "traffic after the driver resumed", 20*time.Second, func() bool {
		n, _ := writerCount(sub2.Tracker, 3)
		return n > 200
	})
	jp.close("closed")
	pl2.Stop()
	requireCleanTracker(t, "post-resume", sub2.Tracker)
	require.NoError(t, sub2.Close())
	require.NoError(t, c2.Close())
	require.NoError(t, cl.Close())
	requireNoLeaks(t, base)
}

// TestClaimHolderKilled SIGKILLs a Go child process that holds an uncommitted TryClaim on a concurrent
// publication shared with an in-process Go publisher and a Java publisher. Java's driver unblocks the stuck
// position after publication.unblock.timeout (4s here) by padding the claimed frame, and every other writer on the
// session carries on with no loss or reordering.
func TestClaimHolderKilled(t *testing.T) {
	requireAdv(t)
	rnd := seed(t)
	base := snapshot()
	freshStack(t)
	counters := driverCounters(t)
	for i, ch := range []string{"aeron:ipc?" + termParams, "aeron:udp?endpoint=localhost:27724|" + termParams} {
		stream := int32(9760 + i)
		streamArg := "stream=" + strconv.Itoa(int(stream))
		subCh := "aeron:ipc"
		if i == 1 {
			subCh = "aeron:udp?endpoint=localhost:27724"
		}
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			cl := connect(t)
			anchor, err := mm.AddSub(cl.Aeron, mm.SubPoll, subCh, stream, rand.New(rand.NewSource(rnd.Int63())), true)
			require.NoError(t, err)
			pl := startPoller(t, anchor)
			inPub, err := mm.AddPub(cl.Aeron, mm.PubConcurrent, ch, stream, 9, []int{64, 1376, 3000})
			require.NoError(t, err)
			loop := startPubLoop(t, inPub, 500*time.Microsecond)
			jp := startPeer(t, "java", "pub", "channel="+ch, streamArg, "writer=2", "sizes=64,1", "rateUs=500")
			jp.await("ready", 30*time.Second)
			hold := startPeer(t, "go", "pub", "channel="+ch, streamArg, "kind=tryclaim", "writer=1", "sizes=64",
				"rateUs=100", "hangAfter=50")
			hold.await("claimed", 20*time.Second)
			hold.Kill()
			killed := time.Now()
			// the log is blocked behind the claim until the driver unblocks it; afterwards traffic flows again
			n0, _ := writerCount(anchor.Tracker, 9)
			awaitTrue(t, "writers to resume after the driver unblocked the dead claim", 15*time.Second, func() bool {
				n, _ := writerCount(anchor.Tracker, 9)
				m, _ := writerCount(anchor.Tracker, 2)
				return n > n0+500 && m > 200
			})
			t.Logf("resumed %v after the kill", time.Since(killed))
			loop.Stop()
			require.NoError(t, loop.err)
			jc := jp.close("closed")
			awaitTrue(t, "in-process writer complete", 20*time.Second, func() bool {
				n, next := writerCount(anchor.Tracker, 9)
				return n == int64(inPub.Next()) && next == inPub.Next()
			})
			awaitTrue(t, "java writer complete", 20*time.Second, func() bool {
				n, _ := writerCount(anchor.Tracker, 2)
				return n == jc.Int("next")
			})
			n1, next1 := writerCount(anchor.Tracker, 1)
			require.Equal(t, int64(50), n1, "the killed writer's committed messages")
			require.Equal(t, uint32(50), next1)
			pl.Stop()
			requireCleanTracker(t, "anchor", anchor.Tracker)
			require.Empty(t, cl.Errors())
			require.NoError(t, inPub.Close())
			require.NoError(t, anchor.Close())
			require.NoError(t, cl.Close())
		})
	}
	requireCountersReturn(t, counters)
	requireNoLeaks(t, base)
}

// TestSubscriberFrozenBeyondClientLiveness SIGSTOPs a Go and a Java subscriber process for longer than the
// driver's client liveness timeout. The driver times both clients out, which releases the flow-control hold on the
// publisher; when resumed, both clients must learn they were timed out (Java: ClientTimeoutException to the error
// handler, client closed) and exit.
func TestSubscriberFrozenBeyondClientLiveness(t *testing.T) {
	requireAdv(t)
	rnd := seed(t)
	base := snapshot()
	freshStack(t)
	counters := driverCounters(t)
	ch := "aeron:udp?endpoint=localhost:27726|" + termParams
	subCh := "aeron:udp?endpoint=localhost:27726"
	stream := int32(9765)
	streamArg := "stream=" + strconv.Itoa(int(stream))
	cl := connect(t)
	anchor, err := mm.AddSub(cl.Aeron, mm.SubControlled, subCh, stream, rand.New(rand.NewSource(rnd.Int63())), false)
	require.NoError(t, err)
	pl := startPoller(t, anchor)
	subs := []*peer{
		startPeer(t, "go", "sub", "channel="+subCh, streamArg, "kind=poll"),
		startPeer(t, "java", "sub", "channel="+subCh, streamArg),
	}
	for _, p := range subs {
		p.await("ready", 30*time.Second)
	}
	inPub, err := mm.AddPub(cl.Aeron, mm.PubOfferBlock, ch, stream, 9, []int{64, 1376, 0})
	require.NoError(t, err)
	loop := startPubLoop(t, inPub, 0)
	awaitTrue(t, "traffic", 20*time.Second, func() bool { return anchor.Tracker.Messages() > 1000 })
	for _, p := range subs {
		p.signal("STOP")
	}
	time.Sleep(6 * time.Second)
	// the frozen subscribers must not hold the publisher back once they have been timed out
	before := inPub.Next()
	time.Sleep(time.Second)
	require.Greater(t, inPub.Next()-before, uint32(500), "publisher still blocked by timed-out subscribers")
	for _, p := range subs {
		p.signal("CONT")
	}
	for _, p := range subs {
		// Java's ClientConductor checks the service interval before reading driver events, so a client
		// resumed after more than the liveness timeout fails with ConductorServiceTimeoutException
		requireTerminatedWith(t, p, "service interval exceeded", 15*time.Second)
	}
	loop.Stop()
	require.NoError(t, loop.err)
	n := int64(inPub.Next())
	awaitTrue(t, "anchor complete", 20*time.Second, func() bool {
		c, _ := writerCount(anchor.Tracker, 9)
		return c == n
	})
	pl.Stop()
	requireCleanTracker(t, "anchor", anchor.Tracker)
	require.Empty(t, cl.Errors())
	require.NoError(t, inPub.Close())
	require.NoError(t, anchor.Close())
	require.NoError(t, cl.Close())
	requireCountersReturn(t, counters)
	requireNoLeaks(t, base)
}

func netem(t *testing.T, args ...string) bool {
	t.Helper()
	if err := exec.Command("sudo", "-n", "tc", "qdisc", "show", "dev", "lo").Run(); err != nil {
		t.Skipf("tc netem not permitted (sudo -n tc failed: %v)", err)
	}
	return true
}

// TestNetemLossReorderDuplicate runs UDP unicast, multicast and dynamic MDC through `tc netem` on lo (loss,
// delay, reordering and duplication on ports 27712-27727 only) with Go OfferBlock and Java ExclusivePublication
// publishers. Aeron's NAK based reliability must hide every fault: no gaps, duplicates or corruption.
func TestNetemLossReorderDuplicate(t *testing.T) {
	requireAdv(t)
	netem(t)
	rnd := seed(t)
	base := snapshot()
	freshStack(t)
	tc := func(args ...string) error { return run("sudo", append([]string{"-n", "tc"}, args...)...) }
	_ = tc("qdisc", "del", "dev", "lo", "root")
	require.NoError(t, tc("qdisc", "add", "dev", "lo", "root", "handle", "1:", "prio", "bands", "4",
		"priomap", "3", "3", "3", "3", "3", "3", "3", "3", "3", "3", "3", "3", "3", "3", "3", "3"))
	t.Cleanup(func() { _ = tc("qdisc", "del", "dev", "lo", "root") })
	require.NoError(t, tc("qdisc", "add", "dev", "lo", "parent", "1:1", "handle", "10:", "netem",
		"loss", "5%", "delay", "2ms", "1ms", "reorder", "20%", "duplicate", "2%"))
	// 27712-27727 = 0x6c40/0xfff0, as source or destination port
	require.NoError(t, tc("filter", "add", "dev", "lo", "parent", "1:", "protocol", "ip", "prio", "1", "u32",
		"match", "ip", "dport", "27712", "0xfff0", "flowid", "1:1"))
	require.NoError(t, tc("filter", "add", "dev", "lo", "parent", "1:", "protocol", "ip", "prio", "2", "u32",
		"match", "ip", "sport", "27712", "0xfff0", "flowid", "1:1"))
	counters := driverCounters(t)
	cases := []struct{ name, pub, sub string }{
		{"udp", "aeron:udp?endpoint=localhost:27712|" + termParams, "aeron:udp?endpoint=localhost:27712"},
		{"multicast", "aeron:udp?endpoint=224.20.30.97:27714|interface=localhost|" + termParams,
			"aeron:udp?endpoint=224.20.30.97:27714|interface=localhost"},
		{"mdc-dynamic", "aeron:udp?control=localhost:27716|control-mode=dynamic|" + termParams,
			"aeron:udp?control=localhost:27716|endpoint=localhost:27718|control-mode=dynamic"},
	}
	for i, c := range cases {
		stream := int32(9770 + i)
		streamArg := "stream=" + strconv.Itoa(int(stream))
		t.Run(c.name, func(t *testing.T) {
			cl := connect(t)
			anchor, err := mm.AddSub(cl.Aeron, mm.SubControlled, c.sub, stream, rand.New(rand.NewSource(rnd.Int63())), false)
			require.NoError(t, err)
			pl := startPoller(t, anchor)
			js := startPeer(t, "java", "sub", "channel="+c.sub, streamArg)
			js.await("ready", 30*time.Second)
			pubs := []*peer{
				startPeer(t, "go", "pub", "channel="+c.pub, streamArg, "kind=offerblock", "writer=1",
					"sizes="+mm.FormatSizes(cellSizes), "rateUs=400"),
				startPeer(t, "java", "pub", "channel="+c.pub, streamArg, "exclusive=true", "writer=2",
					"sizes="+mm.FormatSizes(cellSizes), "rateUs=400"),
			}
			for _, p := range pubs {
				p.await("ready", 30*time.Second)
			}
			time.Sleep(8 * time.Second)
			for w, p := range pubs {
				ev := p.close("closed")
				next := ev.Int("next")
				require.Greater(t, next, int64(100))
				awaitTrue(t, fmt.Sprintf("writer %d complete through loss", w+1), 30*time.Second, func() bool {
					n, _ := writerCount(anchor.Tracker, uint8(w+1))
					return n == next
				})
			}
			requireCleanReports(t, js)
			pl.Stop()
			requireCleanTracker(t, "anchor", anchor.Tracker)
			require.Empty(t, cl.Errors())
			require.NoError(t, anchor.Close())
			require.NoError(t, cl.Close())
		})
	}
	requireCountersReturn(t, counters)
	requireNoLeaks(t, base)
}
