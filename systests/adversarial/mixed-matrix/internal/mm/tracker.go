package mm

import (
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/lirm/aeron-go/aeron"
	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/logbuffer"
	"github.com/lirm/aeron-go/aeron/util"
)

// WriterState is the delivery state of one writer seen on an image.
type WriterState struct {
	First, Next uint32 // first sequence seen, next expected
	Count       int64
}

// ImageState tracks what one consumer received on one image.
type ImageState struct {
	CorrelationID int64
	SessionID     int32
	JoinPosition  int64
	TermLength    int32
	LastEnd       int64 // end position of the last fragment consumed, -1 before the first
	Messages      int64
	Errors        []string
	Writers       map[uint8]*WriterState
	Available     bool // available handler fired
	Unavailable   bool // unavailable handler fired
	EOS           bool // image was at end of stream when it went unavailable
	// AllowGaps tolerates mid-term position jumps (unblocked/aborted claims turn into padding).
	AllowGaps bool
}

func (s *ImageState) errorf(format string, args ...interface{}) {
	if len(s.Errors) < 20 {
		s.Errors = append(s.Errors, fmt.Sprintf("image %d session %d: ", s.CorrelationID, s.SessionID)+
			fmt.Sprintf(format, args...))
	}
}

// Writer summary "w:first:next;" sorted by writer, the same as MatrixPeer.
func (s *ImageState) WriterSummary() string {
	var keys []int
	for w := range s.Writers {
		keys = append(keys, int(w))
	}
	sort.Ints(keys)
	var sb strings.Builder
	for _, w := range keys {
		ws := s.Writers[uint8(w)]
		fmt.Fprintf(&sb, "%d:%d:%d;", w, ws.First, ws.Next)
	}
	return sb.String()
}

// Tracker verifies per image: fragment positions are contiguous from the join position (late joiners must start
// exactly at Image.JoinPosition), and messages carry a valid CRC and are in sequence, exactly once, per writer.
type Tracker struct {
	mu        sync.Mutex
	images    map[int64]*ImageState
	current   *ImageState
	allowGaps bool
}

// NewTracker creates an empty tracker. allowGaps relaxes the mid-term contiguity check.
func NewTracker(allowGaps bool) *Tracker {
	return &Tracker{images: map[int64]*ImageState{}, allowGaps: allowGaps}
}

func (t *Tracker) stateLocked(image aeron.Image) *ImageState {
	s := t.images[image.CorrelationID()]
	if s == nil {
		s = &ImageState{CorrelationID: image.CorrelationID(), SessionID: image.SessionID(),
			JoinPosition: image.JoinPosition(), TermLength: image.TermBufferLength(), LastEnd: -1,
			Writers: map[uint8]*WriterState{}, AllowGaps: t.allowGaps}
		t.images[image.CorrelationID()] = s
	}
	return s
}

// OnAvailable is an AvailableImageHandler.
func (t *Tracker) OnAvailable(image aeron.Image) {
	t.mu.Lock()
	defer t.mu.Unlock()
	s := t.stateLocked(image)
	if s.Available {
		s.errorf("available handler fired twice")
	}
	s.Available = true
}

// OnUnavailable is an UnavailableImageHandler.
func (t *Tracker) OnUnavailable(image aeron.Image) {
	t.mu.Lock()
	defer t.mu.Unlock()
	s := t.stateLocked(image)
	if s.Unavailable {
		s.errorf("unavailable handler fired twice")
	}
	s.Unavailable = true
	s.EOS = image.IsEndOfStream()
}

// Fragment checks the position of a raw fragment; call it for every fragment before reassembly.
func (t *Tracker) Fragment(header *logbuffer.Header) {
	image, ok := header.Context().(aeron.Image)
	t.mu.Lock()
	defer t.mu.Unlock()
	if !ok {
		t.current = nil
		return
	}
	s := t.stateLocked(image)
	t.current = s
	end := header.Position()
	start := end - int64(util.AlignInt32(header.FrameLength(), logbuffer.FrameAlignment))
	expected := s.LastEnd
	if expected < 0 {
		expected = s.JoinPosition
	}
	switch {
	case start == expected:
	case start > expected && start%int64(s.TermLength) == 0 && start-expected < int64(s.TermLength):
		// term end padding
	case start > expected && s.AllowGaps:
	default:
		s.errorf("fragment at %d, expected %d (join %d)", start, expected, s.JoinPosition)
	}
	s.LastEnd = end
}

// Unconsume marks the current fragment as aborted (it will be delivered again).
func (t *Tracker) Unconsume(header *logbuffer.Header) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.current != nil {
		t.current.LastEnd = header.Position() - int64(util.AlignInt32(header.FrameLength(), logbuffer.FrameAlignment))
	}
}

// Message verifies a whole (reassembled) message on the image of the last Fragment call.
func (t *Tracker) Message(buffer *atomic.Buffer, offset, length int32, header *logbuffer.Header) {
	t.mu.Lock()
	defer t.mu.Unlock()
	s := t.current
	if s == nil {
		return
	}
	s.Messages++
	rv := header.GetReservedValue()
	if e := Check(buffer.GetBytesArray(offset, length), rv); e != "" {
		s.errorf("%s (length %d)", e, length)
		return
	}
	_, writer, seq := Decode(rv)
	w := s.Writers[writer]
	if w == nil {
		s.Writers[writer] = &WriterState{First: seq, Next: (seq + 1) & SeqMask, Count: 1}
		return
	}
	if w.Next != seq {
		s.errorf("writer %d: expected seq %d, got %d", writer, w.Next, seq)
	}
	w.Next = (seq + 1) & SeqMask
	w.Count++
}

// Images returns a snapshot copy of every image state.
func (t *Tracker) Images() []ImageState {
	t.mu.Lock()
	defer t.mu.Unlock()
	var out []ImageState
	for _, s := range t.images {
		c := *s
		c.Errors = append([]string(nil), s.Errors...)
		c.Writers = map[uint8]*WriterState{}
		for k, v := range s.Writers {
			vv := *v
			c.Writers[k] = &vv
		}
		out = append(out, c)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CorrelationID < out[j].CorrelationID })
	return out
}

// Messages returns the total number of verified messages.
func (t *Tracker) Messages() int64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	var n int64
	for _, s := range t.images {
		n += s.Messages
	}
	return n
}
