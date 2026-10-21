// Command crashpeer is the Go client process that the client-crash suite SIGKILLs and SIGSTOPs. It runs one role
// against the driver in -dir and prints one JSON event per line on stdout:
//
//	pub      publish sequenced e2e messages (-kind concurrent|exclusive|tryclaim|offerblock, -dest for MDC)
//	sub      subscribe and verify sequences per session (-dest for a multi-destination subscription, -controlled)
//	mixed    pub and sub on the same channel in one client
//	archive  start a recording of -channel through the archive at -control, then publish to it
//	stall    pub+sub hot loops; waits to be SIGSTOPped past the client liveness timeout, then reports how the client
//	         terminated, closes it, checks for leaks and proves a new client works
//
// A panic exits with status 2 (the Go runtime default), so the suite can tell a crash from a clean exit.
package main

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lirm/aeron-go/aeron"
	ab "github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/logbuffer"
	"github.com/lirm/aeron-go/aeron/logbuffer/term"
	"github.com/lirm/aeron-go/archive"
	"github.com/lirm/aeron-go/e2e"
)

type multiFlag []string

func (m *multiFlag) String() string     { return strings.Join(*m, ",") }
func (m *multiFlag) Set(v string) error { *m = append(*m, v); return nil }

var (
	dir        = flag.String("dir", "/dev/shm/aeron-adv-client-crash", "aeron dir")
	mode       = flag.String("mode", "pub", "pub|sub|mixed|archive|stall")
	channel    = flag.String("channel", "aeron:ipc", "channel")
	stream     = flag.Int("stream", 9100, "stream id")
	kind       = flag.String("kind", "concurrent", "concurrent|exclusive|tryclaim|offerblock")
	size       = flag.Int("size", 64, "message size")
	interval   = flag.Duration("interval", 0, "pause between messages")
	count      = flag.Int64("count", 0, "messages to publish (0 = until killed)")
	controlled = flag.Bool("controlled", false, "sub: use ControlledPoll")
	control    = flag.String("control", "aeron:udp?endpoint=localhost:27110", "archive control channel")
	spy        = flag.Bool("spy", false, "mixed: subscribe with aeron-spy: prefix")
	dests      multiFlag
)

var outMu sync.Mutex

func emit(event string, kv ...any) {
	m := map[string]any{"event": event}
	for i := 0; i+1 < len(kv); i += 2 {
		m[kv[i].(string)] = kv[i+1]
	}
	b, _ := json.Marshal(m)
	outMu.Lock()
	fmt.Println(string(b))
	outMu.Unlock()
}

func fatal(err error) {
	emit("error", "message", err.Error())
	os.Exit(1)
}

func main() {
	flag.Var(&dests, "dest", "destination (repeatable)")
	flag.Parse()
	var err error
	switch *mode {
	case "pub":
		err = runPub()
	case "sub":
		err = runSub()
	case "mixed":
		err = runMixed()
	case "archive":
		err = runArchive()
	case "stall":
		err = runStall()
	default:
		err = fmt.Errorf("unknown mode %q", *mode)
	}
	if err != nil {
		fatal(err)
	}
}

func connect(ctx *aeron.Context) (*aeron.Aeron, error) {
	return aeron.Connect(ctx.AeronDir(*dir).MediaDriverTimeout(5 * time.Second))
}

func addPub(a *aeron.Aeron) (aeron.Publisher, error) {
	var pub aeron.Publisher
	var err error
	if *kind == "concurrent" {
		pub, err = a.AddPublication(*channel, int32(*stream))
	} else {
		pub, err = a.AddExclusivePublication(*channel, int32(*stream))
	}
	if err != nil {
		return nil, err
	}
	for _, d := range dests {
		switch p := pub.(type) {
		case *aeron.Publication:
			err = p.AddDestination(d)
		case *aeron.ExclusivePublication:
			err = p.AddDestination(d)
		}
		if err != nil {
			return nil, err
		}
	}
	return pub, nil
}

// publisher offers sequenced e2e messages with the configured publication kind.
type publisher struct {
	pub   aeron.Publisher
	msg   []byte
	buf   *ab.Buffer
	claim logbuffer.Claim
	block []byte
}

func newPublisher(pub aeron.Publisher) *publisher {
	p := &publisher{pub: pub, msg: make([]byte, *size)}
	p.buf = ab.MakeBuffer(p.msg)
	return p
}

func (p *publisher) offer(seq int64) int64 {
	e2e.EncodeMessage(p.msg, seq)
	switch *kind {
	case "tryclaim":
		r := p.pub.TryClaim(int32(len(p.msg)), &p.claim)
		if r > 0 {
			b := p.claim.Buffer()
			b.PutBytesArray(p.claim.Offset(), &p.msg, 0, int32(len(p.msg)))
			p.claim.Commit()
		}
		return r
	case "offerblock":
		return p.offerBlock()
	default:
		return p.pub.Offer(p.buf, 0, int32(len(p.msg)), nil)
	}
}

// offerBlock writes the message as one pre-framed, unfragmented data frame through ExclusivePublication.OfferBlock.
func (p *publisher) offerBlock() int64 {
	ex := p.pub.(*aeron.ExclusivePublication)
	const hdr = 32
	frameLen := hdr + len(p.msg)
	aligned := (frameLen + 31) &^ 31
	if cap(p.block) < aligned {
		p.block = make([]byte, aligned)
	}
	blk := p.block[:aligned]
	for i := range blk {
		blk[i] = 0
	}
	le := binary.LittleEndian
	le.PutUint32(blk[0:], uint32(frameLen))
	blk[4] = 0
	blk[5] = 0xC0
	le.PutUint16(blk[6:], 1)
	le.PutUint32(blk[8:], uint32(ex.TermOffset()))
	le.PutUint32(blk[12:], uint32(ex.SessionID()))
	le.PutUint32(blk[16:], uint32(ex.StreamID()))
	le.PutUint32(blk[20:], uint32(ex.TermID()))
	copy(blk[hdr:], p.msg)
	return ex.OfferBlock(ab.MakeBuffer(blk), 0, int32(aligned))
}

// publishLoop publishes until count (or forever), emitting "sent" progress with the publication position.
func publishLoop(p *publisher) error {
	lastEmit := time.Now()
	for seq := int64(0); *count == 0 || seq < *count; seq++ {
		for {
			r := p.offer(seq)
			if r > 0 {
				break
			}
			switch r {
			case aeron.NotConnected, aeron.BackPressured, aeron.AdminAction:
			default:
				return fmt.Errorf("offer seq %d: %d", seq, r)
			}
			time.Sleep(50 * time.Microsecond)
		}
		if seq%50 == 0 || time.Since(lastEmit) > 20*time.Millisecond {
			emit("sent", "seq", seq, "position", p.pub.Position())
			lastEmit = time.Now()
		}
		if *interval > 0 {
			time.Sleep(*interval)
		}
	}
	emit("done", "count", *count, "position", p.pub.Position())
	select {}
}

func runPub() error {
	a, err := connect(aeron.NewContext())
	if err != nil {
		return err
	}
	pub, err := addPub(a)
	if err != nil {
		return err
	}
	if *kind == "offerblock" && *size+32 > int(pub.TermBufferLength())/2 {
		return errors.New("offerblock message too large")
	}
	emit("ready", "clientId", a.ClientID(), "sessionId", pub.SessionID(), "registrationId", pub.RegistrationID())
	if err := e2e.WaitConnected(pub, 20*time.Second); err != nil {
		return err
	}
	emit("connected")
	return publishLoop(newPublisher(pub))
}

// sessionCheckers verifies every image's sequence independently.
type sessionCheckers struct {
	mu       sync.Mutex
	checkers map[int32]*e2e.SequenceChecker
}

func (s *sessionCheckers) onMessage(buf *ab.Buffer, offset, length int32, header *logbuffer.Header) {
	c := s.checkers[header.SessionId()]
	msg := buf.GetBytesArray(offset, length)
	if c == nil {
		// A late joiner starts mid-stream: verify from the first message it sees.
		first, err := e2e.DecodeMessage(msg)
		if err != nil {
			emit("verify-error", "sessionId", header.SessionId(), "message", err.Error())
			os.Exit(3)
		}
		c = e2e.NewSequenceChecker(first)
		s.checkers[header.SessionId()] = c
	}
	c.OnMessage(msg)
	if err := c.Err(); err != nil {
		emit("verify-error", "sessionId", header.SessionId(), "message", err.Error())
		os.Exit(3)
	}
	if c.Received%50 == 0 {
		emit("recv", "sessionId", header.SessionId(), "next", c.Next)
	}
}

func runSub() error {
	a, err := connect(aeron.NewContext())
	if err != nil {
		return err
	}
	sub, err := a.AddSubscriptionWithHandlers(*channel, int32(*stream),
		func(img aeron.Image) { emit("image-available", "sessionId", img.SessionID()) },
		func(img aeron.Image) { emit("image-unavailable", "sessionId", img.SessionID()) })
	if err != nil {
		return err
	}
	for _, d := range dests {
		if err := sub.AddDestination(d); err != nil {
			return err
		}
	}
	emit("ready", "clientId", a.ClientID(), "registrationId", sub.RegistrationID())
	pollLoop(sub, &sessionCheckers{checkers: map[int32]*e2e.SequenceChecker{}})
	return nil
}

func pollLoop(sub *aeron.Subscription, s *sessionCheckers) {
	if *controlled {
		assembler := aeron.NewControlledFragmentAssembler(
			func(buf *ab.Buffer, offset, length int32, header *logbuffer.Header) term.ControlledPollAction {
				s.onMessage(buf, offset, length, header)
				return term.ControlledPollActionCommit
			}, aeron.DefaultFragmentAssemblyBufferLength)
		for {
			if sub.ControlledPoll(assembler.OnFragment, 10) == 0 {
				time.Sleep(100 * time.Microsecond)
			}
		}
	}
	assembler := aeron.NewFragmentAssembler(s.onMessage, aeron.DefaultFragmentAssemblyBufferLength)
	for {
		if sub.Poll(assembler.OnFragment, 10) == 0 {
			time.Sleep(100 * time.Microsecond)
		}
	}
}

func runMixed() error {
	a, err := connect(aeron.NewContext())
	if err != nil {
		return err
	}
	subChannel := *channel
	if *spy {
		subChannel = "aeron-spy:" + subChannel
	}
	sub, err := a.AddSubscription(subChannel, int32(*stream))
	if err != nil {
		return err
	}
	pub, err := addPub(a)
	if err != nil {
		return err
	}
	emit("ready", "clientId", a.ClientID(), "sessionId", pub.SessionID(), "registrationId", pub.RegistrationID(),
		"subRegistrationId", sub.RegistrationID())
	go pollLoop(sub, &sessionCheckers{checkers: map[int32]*e2e.SequenceChecker{}})
	if err := e2e.WaitConnected(pub, 20*time.Second); err != nil {
		return err
	}
	emit("connected")
	return publishLoop(newPublisher(pub))
}

func runArchive() error {
	options := archive.DefaultOptions()
	options.RequestChannel = *control
	options.RequestStream = 10
	options.ResponseChannel = "aeron:udp?endpoint=localhost:0"
	options.Timeout = 5 * time.Second
	arch, err := archive.NewArchive(options, aeron.NewContext().AeronDir(*dir).MediaDriverTimeout(5*time.Second))
	if err != nil {
		return err
	}
	subID, err := arch.StartRecording(*channel, int32(*stream), false, false)
	if err != nil {
		return err
	}
	pub, err := arch.AddExclusivePublication(*channel, int32(*stream))
	if err != nil {
		return err
	}
	emit("ready", "clientId", arch.Aeron().ClientID(), "sessionId", pub.SessionID(), "recordingSubscriptionId", subID,
		"archiveSessionId", arch.SessionID)
	if err := e2e.WaitConnected(pub, 20*time.Second); err != nil {
		return err
	}
	emit("connected")
	return publishLoop(newPublisher(pub))
}

func errType(err error) string {
	if err == nil {
		return ""
	}
	var cst *aeron.ConductorServiceTimeoutError
	var ct *aeron.ClientTimeoutError
	var dt *aeron.DriverTimeoutError
	switch {
	case errors.As(err, &cst):
		return "ConductorServiceTimeoutError"
	case errors.As(err, &ct):
		return "ClientTimeoutError"
	case errors.As(err, &dt):
		return "DriverTimeoutError"
	}
	return reflect.TypeOf(err).String()
}

func mappedFiles() int {
	maps, _ := os.ReadFile("/proc/self/maps")
	return strings.Count(string(maps), *dir+"/")
}

func runStall() error {
	base := runtime.NumGoroutine()
	var mu sync.Mutex
	var handled []string
	var closeHandlerCalls atomic.Int32
	ctx := aeron.NewContext().ErrorHandler(func(err error) {
		mu.Lock()
		handled = append(handled, errType(err)+": "+err.Error())
		mu.Unlock()
	}).CloseHandler(func() { closeHandlerCalls.Add(1) })
	a, err := connect(ctx)
	if err != nil {
		return err
	}
	sub, err := a.AddSubscriptionWithHandlers(*channel, int32(*stream), nil,
		func(img aeron.Image) { emit("image-unavailable", "sessionId", img.SessionID()) })
	if err != nil {
		return err
	}
	pub, err := addPub(a)
	if err != nil {
		return err
	}
	if err := e2e.WaitConnected(pub, 20*time.Second); err != nil {
		return err
	}
	var lastOffer atomic.Int64
	var received atomic.Int64
	var wg sync.WaitGroup
	wg.Add(2)
	p := newPublisher(pub)
	go func() {
		defer wg.Done()
		for seq := int64(0); ; {
			r := p.offer(seq)
			lastOffer.Store(r)
			if r > 0 {
				seq++
			} else if r == aeron.PublicationClosed {
				return
			}
			if *interval > 0 {
				time.Sleep(*interval)
			}
		}
	}()
	go func() {
		defer wg.Done()
		checker := &sessionCheckers{checkers: map[int32]*e2e.SequenceChecker{}}
		h := aeron.NewFragmentAssembler(func(buf *ab.Buffer, o, l int32, hd *logbuffer.Header) {
			checker.onMessage(buf, o, l, hd)
			received.Add(1)
		}, aeron.DefaultFragmentAssemblyBufferLength)
		for !sub.IsClosed() {
			if sub.Poll(h.OnFragment, 10) == 0 {
				time.Sleep(50 * time.Microsecond)
			}
		}
	}()
	for received.Load() < 100 {
		time.Sleep(time.Millisecond)
	}
	emit("ready", "clientId", a.ClientID(), "sessionId", pub.SessionID())

	// Stopped here by the suite (SIGSTOP); after SIGCONT the conductor must notice it was timed out.
	stalledAt := time.Now()
	for !a.IsClosed() {
		time.Sleep(time.Millisecond)
	}
	detected := time.Since(stalledAt)
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		return errors.New("offer/poll goroutines did not observe the closed client")
	}
	_, addErr := a.AddPublication(*channel, int32(*stream))
	offerAfter := pub.Offer(p.buf, 0, int32(len(p.msg)), nil)
	terminal := a.TerminalError()
	closeErr := a.Close()
	secondCloseErr := a.Close()
	mu.Lock()
	h := append([]string(nil), handled...)
	mu.Unlock()
	emit("terminated", "terminalType", errType(terminal), "terminal", fmt.Sprint(terminal),
		"handled", h, "closeHandlerCalls", closeHandlerCalls.Load(), "detectedAfterMs", detected.Milliseconds(),
		"addErrIsClientClosed", errors.Is(addErr, aeron.ErrClientClosed), "addErr", fmt.Sprint(addErr),
		"offerAfter", offerAfter, "subClosed", sub.IsClosed(), "pubClosed", pub.IsClosed(),
		"closeErr", fmt.Sprint(closeErr), "secondCloseErr", fmt.Sprint(secondCloseErr), "mapped", mappedFiles())

	deadline := time.Now().Add(5 * time.Second)
	for runtime.NumGoroutine() > base && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if n := runtime.NumGoroutine(); n > base {
		buf := make([]byte, 1<<20)
		return fmt.Errorf("goroutine leak after close: %d > %d\n%s", n, base, buf[:runtime.Stack(buf, true)])
	}

	// A replacement client in the same process must work.
	b, err := connect(aeron.NewContext())
	if err != nil {
		return fmt.Errorf("reconnect: %w", err)
	}
	sub2, err := b.AddSubscription(*channel, int32(*stream))
	if err != nil {
		return err
	}
	pub2, err := b.AddExclusivePublication(*channel, int32(*stream))
	if err != nil {
		return err
	}
	if err := e2e.WaitConnected(pub2, 10*time.Second); err != nil {
		return err
	}
	go func() { _ = e2e.Publish(pub2, e2e.PublishOptions{Count: 200, Size: *size, Timeout: 10 * time.Second}) }()
	checker := e2e.NewSequenceChecker(0)
	if err := e2e.Consume(e2e.ReassemblingPoller(sub2), checker, 200, 10*time.Second); err != nil {
		return fmt.Errorf("replacement client: %w (%s)", err, checker)
	}
	if err := b.Close(); err != nil {
		return err
	}
	emit("recovered", "mapped", mappedFiles())
	return nil
}
