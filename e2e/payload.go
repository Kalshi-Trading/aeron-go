package e2e

import (
	"encoding/binary"
	"fmt"

	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/logbuffer"
	"github.com/lirm/aeron-go/aeron/logbuffer/term"
)

// HeaderLength is the size of the sequence + length prefix of every e2e message.
const HeaderLength = 12

// EncodeMessage fills buf with a message carrying seq: 8-byte little endian sequence, 4-byte payload length
// and a deterministic fill pattern derived from seq so that corruption can be detected.
func EncodeMessage(buf []byte, seq int64) {
	if len(buf) < HeaderLength {
		panic(fmt.Sprintf("e2e message buffer too small: %d < %d", len(buf), HeaderLength))
	}
	binary.LittleEndian.PutUint64(buf, uint64(seq))
	binary.LittleEndian.PutUint32(buf[8:], uint32(len(buf)))
	for i := HeaderLength; i < len(buf); i++ {
		buf[i] = fill(seq, i)
	}
}

// DecodeMessage returns the sequence number of msg, validating its length and fill pattern.
func DecodeMessage(msg []byte) (int64, error) {
	if len(msg) < HeaderLength {
		return 0, fmt.Errorf("message too short: %d bytes", len(msg))
	}
	seq := int64(binary.LittleEndian.Uint64(msg))
	if l := int(binary.LittleEndian.Uint32(msg[8:])); l != len(msg) {
		return seq, fmt.Errorf("seq %d: encoded length %d != received %d", seq, l, len(msg))
	}
	for i := HeaderLength; i < len(msg); i++ {
		if msg[i] != fill(seq, i) {
			return seq, fmt.Errorf("seq %d: corrupt payload at offset %d", seq, i)
		}
	}
	return seq, nil
}

func fill(seq int64, i int) byte {
	return byte(seq) ^ byte(i)
}

// SequenceChecker verifies that messages arrive in order, without gaps, duplicates or corruption.
type SequenceChecker struct {
	Next       int64 // next expected sequence
	Received   int64
	Gaps       int64 // number of discontinuities
	Missing    int64 // number of skipped sequences
	Duplicates int64 // messages with seq < Next
	Corrupt    int64
	FirstError error
}

// NewSequenceChecker expects the first message to carry sequence from.
func NewSequenceChecker(from int64) *SequenceChecker {
	return &SequenceChecker{Next: from}
}

// OnMessage records the message in msg.
func (c *SequenceChecker) OnMessage(msg []byte) {
	c.Received++
	seq, err := DecodeMessage(msg)
	if err != nil {
		c.Corrupt++
		c.recordError(err)
		return
	}
	switch {
	case seq == c.Next:
		c.Next++
	case seq > c.Next:
		c.Gaps++
		c.Missing += seq - c.Next
		c.recordError(fmt.Errorf("gap: expected seq %d, got %d", c.Next, seq))
		c.Next = seq + 1
	default:
		c.Duplicates++
		c.recordError(fmt.Errorf("duplicate/out of order: expected seq %d, got %d", c.Next, seq))
	}
}

func (c *SequenceChecker) recordError(err error) {
	if c.FirstError == nil {
		c.FirstError = err
	}
}

// Handler returns a fragment handler feeding c. Fragments must be reassembled already (or fit in one frame).
func (c *SequenceChecker) Handler() term.FragmentHandler {
	return func(buffer *atomic.Buffer, offset int32, length int32, _ *logbuffer.Header) {
		c.OnMessage(buffer.GetBytesArray(offset, length))
	}
}

// Err returns an error describing any gap, duplicate or corruption seen so far.
func (c *SequenceChecker) Err() error {
	if c.Gaps == 0 && c.Duplicates == 0 && c.Corrupt == 0 {
		return nil
	}
	return fmt.Errorf("%s: first error: %w", c, c.FirstError)
}

func (c *SequenceChecker) String() string {
	return fmt.Sprintf("received=%d next=%d gaps=%d missing=%d duplicates=%d corrupt=%d",
		c.Received, c.Next, c.Gaps, c.Missing, c.Duplicates, c.Corrupt)
}
