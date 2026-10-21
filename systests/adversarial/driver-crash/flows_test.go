package drivercrash

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lirm/aeron-go/aeron"
	aatomic "github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/logbuffer"
	"github.com/lirm/aeron-go/aeron/logbuffer/term"
	"github.com/lirm/aeron-go/e2e"
	"github.com/stretchr/testify/require"
)

// sendMode is how a flow's publisher writes a message.
type sendMode int

const (
	offer sendMode = iota
	tryClaim
	offerBlock // ExclusivePublication.OfferBlock of one pre-framed message
)

// flowSpec describes one publisher -> subscriber pair of a given kind.
type flowSpec struct {
	name       string
	pubChannel string
	subChannel string
	stream     int32
	exclusive  bool
	mode       sendMode
	size       int
	controlled bool     // poll with ControlledPoll
	spy        bool     // add an extra aeron-spy: subscription on pubChannel
	dests      []string // manual multi-destination subscription destinations
}

// specs covers every publisher/subscriber kind the suite exercises; ports and streams are offset by base so loops and
// subtests never share a channel with a previous (possibly lingering) one.
func specs(base int) []flowSpec {
	s := int32(9000 + base*10)
	return []flowSpec{
		{name: "concurrent-udp-offer", pubChannel: "aeron:udp?endpoint=" + udp(base*10+1), stream: s, mode: offer,
			size: 128, spy: true},
		{name: "exclusive-ipc-tryclaim", pubChannel: "aeron:ipc", stream: s + 1, exclusive: true, mode: tryClaim,
			size: 256},
		{name: "concurrent-udp-fragmented", pubChannel: "aeron:udp?endpoint=" + udp(base*10+2), stream: s + 2,
			mode: offer, size: 100 * 1024},
		{name: "exclusive-mdc-dynamic-offerblock",
			pubChannel: "aeron:udp?control=" + udp(base*10+3) + "|control-mode=dynamic",
			subChannel: "aeron:udp?endpoint=" + udp(base*10+4) + "|control=" + udp(base*10+3) + "|control-mode=dynamic",
			stream:     s + 3, exclusive: true, mode: offerBlock, size: 512},
		{name: "concurrent-multicast-controlled", pubChannel: "aeron:udp?endpoint=224.20.30.39:" +
			udp(base*10 + 5)[len("localhost:"):] + "|interface=localhost", stream: s + 4, mode: offer, size: 64,
			controlled: true},
		{name: "exclusive-udp-multidestination-sub", pubChannel: "aeron:udp?endpoint=" + udp(base*10+6),
			subChannel: "aeron:udp?control-mode=manual", dests: []string{"aeron:udp?endpoint=" + udp(base*10+6)},
			stream: s + 5, exclusive: true, mode: offer, size: 64},
		{name: "concurrent-ipc-tryclaim", pubChannel: "aeron:ipc", stream: s + 6, mode: tryClaim, size: 1024},
	}
}

// flow is a running flowSpec: a publisher goroutine sending sequenced messages and one subscriber goroutine per
// subscription (main and spy) checking them, until the client closes or stop is called.
type flow struct {
	spec     flowSpec
	pub      aeron.Publisher
	subs     []*aeron.Subscription
	checkers []*e2e.SequenceChecker
	sent     atomic.Int64
	results  chan int64 // the publisher's last non-positive results, after the client terminated
	lastRes  atomic.Int64
	stopCh   chan struct{}
	subStop  chan struct{}
	pubDone  chan struct{}
	wg       sync.WaitGroup
}

func startFlow(t *testing.T, a *aeron.Aeron, spec flowSpec) *flow {
	t.Helper()
	f := &flow{spec: spec, stopCh: make(chan struct{}), subStop: make(chan struct{}), pubDone: make(chan struct{})}
	subChannel := spec.subChannel
	if subChannel == "" {
		subChannel = spec.pubChannel
	}
	sub, err := a.AddSubscription(subChannel, spec.stream)
	require.NoError(t, err, spec.name)
	for _, d := range spec.dests {
		require.NoError(t, sub.AddDestination(d), spec.name)
	}
	f.subs = append(f.subs, sub)
	if spec.exclusive {
		f.pub, err = a.AddExclusivePublication(spec.pubChannel, spec.stream)
	} else {
		f.pub, err = a.AddPublication(spec.pubChannel, spec.stream)
	}
	require.NoError(t, err, spec.name)
	if spec.spy {
		spy, err := a.AddSubscription("aeron-spy:"+spec.pubChannel, spec.stream)
		require.NoError(t, err, spec.name)
		f.subs = append(f.subs, spy)
	}
	for _, s := range f.subs {
		s := s
		awaitTrue(t, spec.name+" subscription connected", 10*time.Second, func() bool { return s.IsConnected() })
	}
	require.NoError(t, e2e.WaitConnected(f.pub, 10*time.Second), spec.name)
	for _, s := range f.subs {
		c := e2e.NewSequenceChecker(0)
		f.checkers = append(f.checkers, c)
		f.wg.Add(1)
		go f.consume(s, c)
	}
	f.wg.Add(1)
	go f.publish()
	return f
}

func (f *flow) publish() {
	defer f.wg.Done()
	defer close(f.pubDone)
	msg := make([]byte, f.spec.size)
	buf := aatomic.MakeBuffer(msg)
	var claim logbuffer.Claim
	var frame []byte
	var frameBuf *aatomic.Buffer
	if f.spec.mode == offerBlock {
		frame = make([]byte, align(logbuffer.DataFrameHeader.Length+int32(f.spec.size)))
		frameBuf = aatomic.MakeBuffer(frame)
	}
	seq := int64(0)
	for {
		select {
		case <-f.stopCh:
			return
		default:
		}
		var r int64
		switch f.spec.mode {
		case offer:
			e2e.EncodeMessage(msg, seq)
			r = f.pub.Offer(buf, 0, int32(f.spec.size), nil)
		case tryClaim:
			if r = f.pub.TryClaim(int32(f.spec.size), &claim); r > 0 {
				e2e.EncodeMessage(msg, seq)
				claim.Buffer().PutBytesArray(claim.Offset(), &msg, 0, int32(f.spec.size))
				claim.Commit()
			}
		case offerBlock:
			ex := f.pub.(*aeron.ExclusivePublication)
			e2e.EncodeMessage(msg, seq)
			frameBlock(frameBuf, ex, msg)
			r = ex.OfferBlock(frameBuf, 0, int32(len(frame)))
		}
		f.lastRes.Store(r)
		if r > 0 {
			seq++
			f.sent.Store(seq)
			time.Sleep(50 * time.Microsecond)
			continue
		}
		if r == aeron.PublicationClosed {
			return
		}
		time.Sleep(100 * time.Microsecond)
	}
}

func (f *flow) consume(sub *aeron.Subscription, c *e2e.SequenceChecker) {
	defer f.wg.Done()
	poll := e2e.ReassemblingPoller(sub)
	if f.spec.controlled {
		h := c.Handler()
		poll = func(_ term.FragmentHandler, limit int) (int, error) {
			return sub.ControlledPoll(func(b *aatomic.Buffer, o, l int32, hdr *logbuffer.Header) term.ControlledPollAction {
				h(b, o, l, hdr)
				return term.ControlledPollActionContinue
			}, limit), nil
		}
	}
	handler := c.Handler()
	for !sub.IsClosed() {
		select {
		case <-f.stopCh:
			return
		case <-f.subStop:
			return
		default:
		}
		if n, _ := poll(handler, 64); n == 0 {
			time.Sleep(100 * time.Microsecond)
		}
	}
}

// awaitProgress waits until every subscription of f has received at least n messages.
func (f *flow) awaitProgress(t *testing.T, n int64, timeout time.Duration) {
	t.Helper()
	for i, c := range f.checkers {
		c := c
		awaitTrue(t, f.spec.name+" progress", timeout, func() bool { return c.Next >= n || c.Err() != nil })
		require.NoError(t, c.Err(), "%s subscription %d: %s", f.spec.name, i, c)
	}
}

// awaitExit waits for the flow's goroutines to stop by themselves (the client terminated).
func (f *flow) awaitExit(t *testing.T, timeout time.Duration) {
	t.Helper()
	done := make(chan struct{})
	go func() { f.wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(timeout):
		t.Fatalf("%s: publisher/subscriber goroutines did not stop %v after the client terminated", f.spec.name, timeout)
	}
}

// stopSubscribers stops polling (the subscriptions stay open), so the publisher becomes back pressured.
func (f *flow) stopSubscribers() { close(f.subStop) }

// awaitPublisherExit waits for the publisher goroutine to stop by itself (its publication returned CLOSED).
func (f *flow) awaitPublisherExit(t *testing.T, timeout time.Duration) {
	t.Helper()
	select {
	case <-f.pubDone:
	case <-time.After(timeout):
		t.Fatalf("%s: publisher still spinning %v after the driver died (last result %d)", f.spec.name, timeout,
			f.lastRes.Load())
	}
}

func (f *flow) stop() {
	select {
	case <-f.stopCh:
	default:
		close(f.stopCh)
	}
	f.wg.Wait()
}

// requireIntact asserts that no subscription saw corruption, a duplicate or a gap.
func (f *flow) requireIntact(t *testing.T) {
	t.Helper()
	for i, c := range f.checkers {
		require.NoError(t, c.Err(), "%s subscription %d: %s", f.spec.name, i, c)
	}
}

func align(n int32) int32 { return (n + 31) &^ 31 }

// frameBlock writes msg as one complete, pre-framed DATA frame at offset 0 of frameBuf for OfferBlock, with the
// header fields OfferBlock requires (session, stream, term offset and term id of the current position).
func frameBlock(frameBuf *aatomic.Buffer, pub *aeron.ExclusivePublication, msg []byte) {
	frameLen := logbuffer.DataFrameHeader.Length + int32(len(msg))
	frameBuf.PutInt32(logbuffer.DataFrameHeader.FrameLengthFieldOffset, frameLen)
	frameBuf.PutInt8(logbuffer.DataFrameHeader.VersionFieldOffset, logbuffer.DataFrameHeader.CurrentVersion)
	frameBuf.PutUInt8(logbuffer.DataFrameHeader.FlagsFieldOffset, logbuffer.DataFrameHeader.BeginAndEndFlags)
	frameBuf.PutUInt16(logbuffer.DataFrameHeader.TypeFieldOffset, logbuffer.DataFrameHeader.TypeData)
	frameBuf.PutInt32(logbuffer.DataFrameHeader.TermOffsetFieldOffset, pub.TermOffset())
	frameBuf.PutInt32(logbuffer.DataFrameHeader.SessionIDFieldOffset, pub.SessionID())
	frameBuf.PutInt32(logbuffer.DataFrameHeader.StreamIDFieldOffset, pub.StreamID())
	frameBuf.PutInt32(logbuffer.DataFrameHeader.TermIDFieldOffset, pub.TermID())
	frameBuf.PutBytesArray(logbuffer.DataFrameHeader.Length, &msg, 0, int32(len(msg)))
	if pad := frameBuf.Capacity() - frameLen; pad > 0 {
		frameBuf.SetMemory(frameLen, pad, 0)
	}
}
