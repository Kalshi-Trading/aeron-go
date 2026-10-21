// Package rmrun holds the publisher, ReplayMerge and PersistentSubscription drivers shared by the adversarial
// replaymerge tests (in process) and by the rmhelper child process that the tests SIGKILL. Every runner reports
// through an Emitter using the same JSON event vocabulary as docker/adversarial-replaymerge/RmOracle.java so Go and
// Java outcomes can be compared directly.
package rmrun

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/lirm/aeron-go/aeron"
	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/logbuffer"
	"github.com/lirm/aeron-go/aeron/logbuffer/term"
	"github.com/lirm/aeron-go/archive"
	ps "github.com/lirm/aeron-go/archive/persistentsubscription"
	"github.com/lirm/aeron-go/archive/replaymerge"
	"github.com/lirm/aeron-go/e2e"
)

// ControlChannel is the adv-replaymerge archive control request channel.
const ControlChannel = "aeron:udp?endpoint=localhost:27410"

// Event is one reported event.
type Event map[string]any

// Emitter reports an event with alternating key/value pairs.
type Emitter func(event string, kv ...any)

// JSONEmitter writes one JSON object per event to w.
func JSONEmitter(w io.Writer) Emitter {
	var mu sync.Mutex
	enc := json.NewEncoder(w)
	return func(event string, kv ...any) {
		mu.Lock()
		defer mu.Unlock()
		_ = enc.Encode(toEvent(event, kv))
	}
}

func toEvent(event string, kv []any) Event {
	e := Event{"event": event}
	for i := 0; i+1 < len(kv); i += 2 {
		e[fmt.Sprint(kv[i])] = kv[i+1]
	}
	return e
}

// Nop discards events.
func Nop(string, ...any) {}

// Checker validates the e2e payload sequence and tracks the position after the last delivered message.
type Checker struct {
	*e2e.SequenceChecker
	LastPosition int64
}

// NewChecker expects sequence from first, delivered from position.
func NewChecker(from, position int64) *Checker {
	return &Checker{SequenceChecker: e2e.NewSequenceChecker(from), LastPosition: position}
}

// OnFragment is a term.FragmentHandler for reassembled messages.
func (c *Checker) OnFragment(buffer *atomic.Buffer, offset int32, length int32, header *logbuffer.Header) {
	c.LastPosition = header.Position()
	c.OnMessage(buffer.GetBytesArray(offset, length))
}

func (c *Checker) result(kv ...any) []any {
	return append([]any{"next", c.Next, "received", c.Received, "gaps", c.Gaps, "duplicates", c.Duplicates,
		"corrupt", c.Corrupt, "lastPosition", c.LastPosition}, kv...)
}

// ArchiveOptions returns options for the adv-replaymerge archive on client a.
func ArchiveOptions(a *aeron.Aeron) *archive.Options {
	o := archive.DefaultOptions()
	o.RequestChannel = ControlChannel
	o.RequestStream = 10
	o.ResponseChannel = "aeron:udp?endpoint=localhost:0"
	o.Timeout = 3 * time.Second
	o.Aeron = a
	return o
}

// ConnectArchive connects to the archive on client a, retrying until deadline.
func ConnectArchive(a *aeron.Aeron, deadline time.Time, emit Emitter) (*archive.Archive, error) {
	for {
		arch, err := archive.NewArchive(ArchiveOptions(a), nil)
		if err == nil {
			return arch, nil
		}
		emit("archiveConnectError", "message", err.Error())
		if time.Now().After(deadline) || a.IsClosed() {
			return nil, err
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// MergeConfig configures RunReplayMerge.
type MergeConfig struct {
	RecordingID       int64
	SessionID         int32
	StreamID          int32
	ReplayDestination string
	LiveDestination   string
	Position          int64
	From              int64
	Count             int64 // run until sequence From+Count-1 is delivered
	Timeout           time.Duration
	ProgressTimeoutMs int64
	Retries           int           // failed merges are retried from the last delivered position
	RetryDelay        time.Duration // before a retry
	Controlled        bool          // use ControlledPoll instead of Poll
	PollDelay         time.Duration // sleep after each poll: a slow consumer
	ProgressEvery     int64         // emit "progress" every N messages, 0 for never
	// OnState is called on every state change (in-process tests use it to inject faults at a state).
	OnState func(state replaymerge.State, c *Checker)
}

// MergeResult is the outcome of RunReplayMerge.
type MergeResult struct {
	*Checker
	Merged      bool
	TimedOut    bool
	Failures    []error
	FailStates  []replaymerge.State
	MaxCloseDur time.Duration
}

// RunReplayMerge runs a ReplayMerge on client a until Count messages are delivered, retrying failed merges.
func RunReplayMerge(a *aeron.Aeron, cfg MergeConfig, emit Emitter) MergeResult {
	return RunReplayMergeUntil(a, cfg, emit, nil)
}

// RunReplayMergeUntil is RunReplayMerge that also stops, without failing, when stop returns true.
func RunReplayMergeUntil(a *aeron.Aeron, cfg MergeConfig, emit Emitter, stop func() bool) MergeResult {
	if cfg.Timeout == 0 {
		cfg.Timeout = time.Minute
	}
	if cfg.ProgressTimeoutMs == 0 {
		cfg.ProgressTimeoutMs = 5000
	}
	if cfg.RetryDelay == 0 {
		cfg.RetryDelay = 500 * time.Millisecond
	}
	deadline := time.Now().Add(cfg.Timeout)
	checker := NewChecker(cfg.From, cfg.Position)
	res := MergeResult{Checker: checker}
	assembler := aeron.NewFragmentAssembler(checker.OnFragment, aeron.DefaultFragmentAssemblyBufferLength)
	controlled := aeron.NewControlledFragmentAssembler(
		func(buffer *atomic.Buffer, offset int32, length int32, header *logbuffer.Header) term.ControlledPollAction {
			checker.OnFragment(buffer, offset, length, header)
			return term.ControlledPollActionCommit
		}, aeron.DefaultFragmentAssemblyBufferLength)
	position := cfg.Position
	emit("ready")
	for attempt := 0; ; attempt++ {
		err := runMergeAttempt(a, cfg, position, deadline, checker, assembler, controlled, &res, emit, stop)
		if err == nil {
			break
		}
		res.Failures = append(res.Failures, err)
		if res.TimedOut || attempt >= cfg.Retries || time.Now().After(deadline) || a.IsClosed() {
			break
		}
		position = checker.LastPosition
		time.Sleep(cfg.RetryDelay)
	}
	emit("result", checker.result("merged", res.Merged, "failures", len(res.Failures), "timedOut", res.TimedOut)...)
	return res
}

func runMergeAttempt(a *aeron.Aeron, cfg MergeConfig, position int64, deadline time.Time, checker *Checker,
	assembler *aeron.FragmentAssembler, controlled *aeron.ControlledFragmentAssembler, res *MergeResult,
	emit Emitter, stop func() bool) (err error) {
	var merge *replaymerge.ReplayMerge
	var sub *aeron.Subscription
	var arch *archive.Archive
	defer func() {
		start := time.Now()
		if merge != nil {
			if cerr := merge.Close(); cerr != nil {
				emit("closeError", "message", cerr.Error())
			}
		}
		if sub != nil {
			_ = sub.Close()
		}
		if arch != nil {
			_ = arch.Close()
		}
		d := time.Since(start)
		if d > res.MaxCloseDur {
			res.MaxCloseDur = d
		}
		emit("closed", "ms", d.Milliseconds())
	}()
	fail := func(err error) error {
		state := "NONE"
		if merge != nil {
			state = merge.State().String()
			res.FailStates = append(res.FailStates, merge.State())
		}
		var ae *archive.ArchiveException
		code := int64(-1)
		if errors.As(err, &ae) {
			code = int64(ae.ErrorCode)
		}
		emit("failed", "message", err.Error(), "state", state, "hasFailed", merge != nil && merge.HasFailed(),
			"archiveErrorCode", code, "next", checker.Next, "position", checker.LastPosition)
		return err
	}
	if arch, err = ConnectArchive(a, deadline, emit); err != nil {
		return fail(err)
	}
	subChannel := fmt.Sprintf("aeron:udp?control-mode=manual|session-id=%d", cfg.SessionID)
	if sub, err = a.AddSubscription(subChannel, cfg.StreamID); err != nil {
		return fail(err)
	}
	merge, err = replaymerge.NewReplayMerge(sub, arch, fmt.Sprintf("aeron:udp?session-id=%d", cfg.SessionID),
		cfg.ReplayDestination, cfg.LiveDestination, cfg.RecordingID, position, cfg.ProgressTimeoutMs)
	if err != nil {
		return fail(err)
	}
	last := replaymerge.State(-1)
	nextProgress := checker.Received + cfg.ProgressEvery
	for checker.Next < cfg.From+cfg.Count && (stop == nil || !stop()) {
		var n int
		if cfg.Controlled {
			n, err = merge.ControlledPoll(controlled.OnFragment, 10)
		} else {
			n, err = merge.Poll(assembler.OnFragment, 10)
		}
		if s := merge.State(); s != last {
			last = s
			emit("state", "state", s.String(), "next", checker.Next)
			if cfg.OnState != nil {
				cfg.OnState(s, checker)
			}
		}
		if err != nil {
			return fail(err)
		}
		if merge.HasFailed() {
			return fail(errors.New("ReplayMerge failed without returning an error"))
		}
		if merge.IsMerged() {
			res.Merged = true
		}
		if cfg.ProgressEvery > 0 && checker.Received >= nextProgress {
			nextProgress = checker.Received + cfg.ProgressEvery
			emit("progress", "next", checker.Next, "position", checker.LastPosition, "state", merge.State().String())
		}
		if time.Now().After(deadline) {
			res.TimedOut = true
			return fail(fmt.Errorf("timed out in state %s: %s", merge.State(), checker))
		}
		if cfg.PollDelay > 0 {
			time.Sleep(cfg.PollDelay)
		} else if n == 0 {
			time.Sleep(50 * time.Microsecond)
		}
	}
	return nil
}

// PSConfig configures RunPersistentSubscription.
type PSConfig struct {
	AeronDir      string
	RecordingID   int64
	Position      int64
	LiveChannel   string
	StreamID      int32
	ReplayChannel string
	ReplayStream  int32
	From          int64
	Count         int64
	Timeout       time.Duration
	Controlled    bool
	PollDelay     time.Duration
	ProgressEvery int64
	// OnState is called on every state change.
	OnState func(state ps.State, c *Checker)
}

// PSResult is the outcome of RunPersistentSubscription.
type PSResult struct {
	*Checker
	Joined, Left int
	Errors       []error
	States       []ps.State
	Failed       bool
	FailureErr   error
	TimedOut     bool
	CloseDur     time.Duration
}

// RunPersistentSubscription consumes Count messages through a PersistentSubscription on client a.
func RunPersistentSubscription(a *aeron.Aeron, cfg PSConfig, emit Emitter) (res PSResult, err error) {
	if cfg.Timeout == 0 {
		cfg.Timeout = time.Minute
	}
	checker := NewChecker(cfg.From, cfg.Position)
	res.Checker = checker
	ctx := ps.NewContext()
	ctx.Aeron = a
	ctx.AeronDir = cfg.AeronDir
	ctx.RecordingID = cfg.RecordingID
	ctx.StartPosition = cfg.Position
	ctx.LiveChannel = cfg.LiveChannel
	ctx.LiveStreamID = cfg.StreamID
	ctx.ReplayChannel = cfg.ReplayChannel
	ctx.ReplayStreamID = cfg.ReplayStream
	opts := ArchiveOptions(nil)
	ctx.ArchiveOptions = opts
	ctx.Listener = ps.ListenerFuncs{
		LiveJoined: func() { res.Joined++; emit("liveJoined", "next", checker.Next) },
		LiveLeft:   func() { res.Left++; emit("liveLeft", "next", checker.Next) },
		Error: func(err error) {
			res.Errors = append(res.Errors, err)
			emit("error", "message", err.Error())
		},
	}
	sub, err := ps.New(ctx)
	if err != nil {
		return res, err
	}
	defer func() {
		start := time.Now()
		if cerr := sub.Close(); cerr != nil {
			emit("closeError", "message", cerr.Error())
		}
		res.CloseDur = time.Since(start)
		emit("closed", "ms", res.CloseDur.Milliseconds())
	}()
	emit("ready")
	assembler := aeron.NewFragmentAssembler(checker.OnFragment, aeron.DefaultFragmentAssemblyBufferLength)
	controlled := func(buffer *atomic.Buffer, offset int32, length int32, header *logbuffer.Header) term.ControlledPollAction {
		checker.OnFragment(buffer, offset, length, header)
		return term.ControlledPollActionCommit
	}
	controlledAssembler := aeron.NewControlledFragmentAssembler(controlled, aeron.DefaultFragmentAssemblyBufferLength)
	deadline := time.Now().Add(cfg.Timeout)
	last := ps.State(-1)
	nextProgress := cfg.ProgressEvery
	for checker.Next < cfg.From+cfg.Count && !sub.HasFailed() {
		var n int
		if cfg.Controlled {
			n = sub.ControlledPoll(controlledAssembler.OnFragment, 10)
		} else {
			n = sub.Poll(assembler.OnFragment, 10)
		}
		if s := sub.State(); s != last {
			last = s
			res.States = append(res.States, s)
			emit("state", "state", s.String(), "next", checker.Next)
			if cfg.OnState != nil {
				cfg.OnState(s, checker)
			}
		}
		if cfg.ProgressEvery > 0 && checker.Received >= nextProgress {
			nextProgress = checker.Received + cfg.ProgressEvery
			emit("progress", "next", checker.Next, "position", checker.LastPosition, "state", sub.State().String())
		}
		if time.Now().After(deadline) {
			res.TimedOut = true
			break
		}
		if cfg.PollDelay > 0 {
			time.Sleep(cfg.PollDelay)
		} else if n == 0 {
			time.Sleep(50 * time.Microsecond)
		}
	}
	res.Failed = sub.HasFailed()
	res.FailureErr = sub.FailureReason()
	emit("result", checker.result("live", sub.IsLive(), "failed", res.Failed, "joined", res.Joined,
		"left", res.Left, "timedOut", res.TimedOut)...)
	return res, nil
}

// PubConfig configures RunPublisher.
type PubConfig struct {
	Channel  string
	StreamID int32
	Kind     string // "pub" (concurrent Publication), "excl" (ExclusivePublication), "tryclaim" (excl + TryClaim)
	From     int64
	Count    int64 // 0 publishes until stop is closed
	Sizes    []int // message sizes cycled by sequence
	Interval time.Duration
	// Rate limits messages per second when > 0 (applied every 100 messages).
	Rate int
	// SlowAfter switches to SlowRate messages per second once SlowAfter messages have been sent.
	SlowAfter int64
	SlowRate  int
	SentEvery int64
}

// Publisher publishes the e2e payload sequence.
type Publisher struct {
	pub  aeron.Publisher
	excl *aeron.ExclusivePublication
	cfg  PubConfig
}

// NewPublisher adds the publication for cfg.
func NewPublisher(a *aeron.Aeron, cfg PubConfig) (*Publisher, error) {
	if len(cfg.Sizes) == 0 {
		cfg.Sizes = []int{64}
	}
	p := &Publisher{cfg: cfg}
	var err error
	switch cfg.Kind {
	case "pub", "":
		p.pub, err = a.AddPublication(cfg.Channel, cfg.StreamID)
	case "excl", "tryclaim":
		p.excl, err = a.AddExclusivePublication(cfg.Channel, cfg.StreamID)
		p.pub = p.excl
	default:
		err = fmt.Errorf("unknown publisher kind %q", cfg.Kind)
	}
	if err != nil {
		return nil, err
	}
	return p, nil
}

// Pub returns the publication.
func (p *Publisher) Pub() aeron.Publisher { return p.pub }

// Run publishes until Count messages are sent, stop is closed or an offer fails terminally. sent is updated with
// the next sequence to send.
func (p *Publisher) Run(stop <-chan struct{}, emit Emitter, sent func(next int64)) error {
	cfg := p.cfg
	maxSize := 0
	for _, s := range cfg.Sizes {
		maxSize = max(maxSize, s)
	}
	msg := make([]byte, maxSize)
	buf := atomic.MakeBuffer(msg)
	var claim logbuffer.Claim
	start := time.Now()
	for seq := cfg.From; cfg.Count == 0 || seq < cfg.From+cfg.Count; seq++ {
		size := cfg.Sizes[int(seq)%len(cfg.Sizes)]
		e2e.EncodeMessage(msg[:size], seq)
		for {
			var ret int64
			if cfg.Kind == "tryclaim" && int32(size) <= p.excl.MaxPayloadLength() {
				ret = p.excl.TryClaim(int32(size), &claim)
				if ret >= 0 {
					claim.Buffer().PutBytesArray(claim.Offset(), &msg, 0, int32(size))
					claim.Commit()
				}
			} else {
				ret = p.pub.Offer(buf, 0, int32(size), nil)
			}
			if ret >= 0 {
				break
			}
			switch ret {
			case aeron.NotConnected, aeron.BackPressured, aeron.AdminAction:
			default:
				return fmt.Errorf("offer seq %d failed: %d", seq, ret)
			}
			select {
			case <-stop:
				return nil
			default:
			}
			time.Sleep(100 * time.Microsecond)
		}
		if sent != nil {
			sent(seq + 1)
		}
		if cfg.SentEvery > 0 && (seq+1-cfg.From)%cfg.SentEvery == 0 {
			emit("sent", "next", seq+1, "position", p.pub.Position())
		}
		if cfg.Interval > 0 {
			time.Sleep(cfg.Interval)
		}
		if cfg.SlowAfter > 0 && seq+1-cfg.From == cfg.SlowAfter {
			cfg.Rate, cfg.From, start = cfg.SlowRate, seq+1, time.Now()
			cfg.Count -= cfg.SlowAfter
			cfg.SlowAfter = 0
		}
		if cfg.Rate > 0 && (seq+1-cfg.From)%100 == 0 {
			want := time.Duration(float64(seq+1-cfg.From) / float64(cfg.Rate) * float64(time.Second))
			if d := want - time.Since(start); d > 0 {
				time.Sleep(d)
			}
		}
		select {
		case <-stop:
			return nil
		default:
		}
	}
	emit("sent", "next", cfg.From+cfg.Count, "position", p.pub.Position(), "done", true)
	return nil
}
