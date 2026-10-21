package advproto

import (
	"sync"
	"time"

	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/logbuffer"
	"github.com/lirm/aeron-go/aeron/logbuffer/term"
	"github.com/lirm/aeron-go/cluster/client"
	"github.com/lirm/aeron-go/cluster/codecs"
)

// Cluster ingress/egress stream ids (docker/adversarial-cluster.compose.yml).
const (
	IngressStreamID int32 = 9601
	EgressStreamID  int32 = 9602
)

// SessionEvent is a session event received on egress.
type SessionEvent struct {
	Code   codecs.EventCodeEnum
	Detail string
}

// Sender drives an AeronCluster: it offers sequenced messages under one tag and tracks the echoes the service
// returns once a message is committed and applied by the leader.
type Sender struct {
	AC  *client.AeronCluster
	Tag int64

	mu            sync.Mutex
	acked         Ranges
	dumpsAcked    map[int64]bool
	newLeaders    []int32
	sessionEvents []SessionEvent
	errors        []error
	bad           []error

	arr   []byte
	buf   *atomic.Buffer
	claim logbuffer.Claim
}

// Options configure NewSender.
type Options struct {
	AeronDir   string
	Endpoints  string
	Egress     string
	Tag        int64
	Controlled bool
	// Exclusive uses an ExclusivePublication for ingress.
	Exclusive      bool
	MessageTimeout time.Duration
}

// NewSender connects a cluster client.
func NewSender(o Options) (*Sender, error) {
	s := &Sender{Tag: o.Tag, dumpsAcked: map[int64]bool{}, arr: make([]byte, 1<<20)}
	s.buf = atomic.MakeBuffer(s.arr)
	ctx := client.NewContext()
	ctx.AeronDirectoryName = o.AeronDir
	ctx.IngressChannel = "aeron:udp"
	ctx.IngressEndpoints = o.Endpoints
	ctx.IngressStreamId = IngressStreamID
	ctx.EgressStreamId = EgressStreamID
	ctx.EgressChannel = o.Egress
	ctx.IsIngressExclusive = o.Exclusive
	if o.MessageTimeout > 0 {
		ctx.MessageTimeout = o.MessageTimeout
	}
	if o.Controlled {
		ctx.ControlledEgressListener = controlledListener{s}
	} else {
		ctx.EgressListener = listener{s}
	}
	ctx.ErrorHandler = func(err error) {
		s.mu.Lock()
		s.errors = append(s.errors, err)
		s.mu.Unlock()
	}
	ac, err := client.Connect(ctx)
	if err != nil {
		return nil, err
	}
	s.AC = ac
	return s, nil
}

// Offer sends one message; claim uses TryClaim (payload must fit in one frame). Returns the publication result.
func (s *Sender) Offer(kind int32, seq int64, payloadLen int, claim bool) int64 {
	n := Encode(s.arr, kind, s.Tag, seq, payloadLen)
	if !claim {
		return s.AC.Offer(s.buf, 0, int32(n))
	}
	r := s.AC.TryClaim(int32(n), &s.claim)
	if r > 0 {
		s.claim.Buffer().PutBytes(s.claim.Offset()+client.SessionHeaderLength, s.buf, 0, int32(n))
		s.claim.Commit()
	}
	return r
}

// Poll polls egress and state changes.
func (s *Sender) Poll(controlled bool) int {
	if controlled {
		return s.AC.ControlledPollEgress() + s.AC.PollStateChanges()
	}
	return s.AC.PollEgress() + s.AC.PollStateChanges()
}

// Acked returns a copy of the acknowledged sequence set.
func (s *Sender) Acked() Ranges {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append(Ranges(nil), s.acked...)
}

// IsAcked reports whether seq was echoed.
func (s *Sender) IsAcked(seq int64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.acked.Contains(seq)
}

// DumpAcked reports whether dump id was echoed.
func (s *Sender) DumpAcked(id int64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.dumpsAcked[id]
}

// NewLeaders returns the leader ids from NewLeader events.
func (s *Sender) NewLeaders() []int32 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]int32(nil), s.newLeaders...)
}

// SessionEvents returns the received session events.
func (s *Sender) SessionEvents() []SessionEvent {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]SessionEvent(nil), s.sessionEvents...)
}

// Errors returns errors passed to the ErrorHandler.
func (s *Sender) Errors() []error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]error(nil), s.errors...)
}

// BadEchoes returns echoes that failed validation, duplicated, or carried a foreign tag.
func (s *Sender) BadEchoes() []error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]error(nil), s.bad...)
}

func (s *Sender) onMessage(buffer *atomic.Buffer, offset, length int32) {
	kind, tag, seq, err := Decode(buffer.GetBytesArray(offset, length))
	s.mu.Lock()
	defer s.mu.Unlock()
	switch {
	case err != nil:
		s.bad = append(s.bad, err)
	case tag != s.Tag:
		s.bad = append(s.bad, errorf("foreign tag %d seq %d", tag, seq))
	case kind == KindDump:
		s.dumpsAcked[seq] = true
	case !s.acked.Add(seq):
		s.bad = append(s.bad, errorf("duplicate echo seq %d", seq))
	}
}

func (s *Sender) onSessionEvent(code codecs.EventCodeEnum, detail string) {
	s.mu.Lock()
	s.sessionEvents = append(s.sessionEvents, SessionEvent{code, detail})
	s.mu.Unlock()
}

func (s *Sender) onNewLeader(leader int32) {
	s.mu.Lock()
	s.newLeaders = append(s.newLeaders, leader)
	s.mu.Unlock()
}

type listener struct{ s *Sender }

func (l listener) OnMessage(_ int64, _ int64, buffer *atomic.Buffer, offset, length int32, _ *logbuffer.Header) {
	l.s.onMessage(buffer, offset, length)
}
func (l listener) OnSessionEvent(_ int64, _ int64, _ int64, _ int32, code codecs.EventCodeEnum, detail string) {
	l.s.onSessionEvent(code, detail)
}
func (l listener) OnNewLeader(_ int64, _ int64, leader int32, _ string) { l.s.onNewLeader(leader) }
func (l listener) OnAdminResponse(int64, int64, codecs.AdminRequestTypeEnum, codecs.AdminResponseCodeEnum, string,
	*atomic.Buffer, int32, int32) {
}

type controlledListener struct{ s *Sender }

func (l controlledListener) OnMessage(_ int64, _ int64, buffer *atomic.Buffer, offset, length int32,
	_ *logbuffer.Header) term.ControlledPollAction {
	l.s.onMessage(buffer, offset, length)
	return term.ControlledPollActionContinue
}
func (l controlledListener) OnSessionEvent(_ int64, _ int64, _ int64, _ int32, code codecs.EventCodeEnum,
	detail string) {
	l.s.onSessionEvent(code, detail)
}
func (l controlledListener) OnNewLeader(_ int64, _ int64, leader int32, _ string) {
	l.s.onNewLeader(leader)
}
func (l controlledListener) OnAdminResponse(int64, int64, codecs.AdminRequestTypeEnum, codecs.AdminResponseCodeEnum,
	string, *atomic.Buffer, int32, int32) {
}
