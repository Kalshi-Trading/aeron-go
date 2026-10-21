package broadcast

import (
	"fmt"

	"github.com/lirm/aeron-go/aeron/atomic"
	rb "github.com/lirm/aeron-go/aeron/ringbuffer"
	"github.com/lirm/aeron-go/aeron/util"
)

// Transmitter is the single producer of a broadcast buffer (Agrona BroadcastTransmitter). The media driver
// owns the transmitter for the CnC to-clients buffer; this implementation is used for testing and for
// Go-side broadcast buffers.
type Transmitter struct {
	buffer                 *atomic.Buffer
	capacity               int32
	maxMsgLength           int32
	tailIntentCounterIndex int32
	tailCounterIndex       int32
	latestCounterIndex     int32
}

// NewTransmitter wraps buffer, whose capacity must be a power of two plus TrailerLength.
func NewTransmitter(buffer *atomic.Buffer) (*Transmitter, error) {
	capacity := buffer.Capacity() - TrailerLength
	if err := checkCapacity(capacity); err != nil {
		return nil, err
	}
	if err := buffer.VerifyAlignment(); err != nil {
		return nil, err
	}
	return &Transmitter{
		buffer:                 buffer,
		capacity:               capacity,
		maxMsgLength:           capacity >> 3,
		tailIntentCounterIndex: capacity + BufferDescriptor.tailIntentCounterOffset,
		tailCounterIndex:       capacity + BufferDescriptor.tailCounterOffset,
		latestCounterIndex:     capacity + BufferDescriptor.latestCounterOffset,
	}, nil
}

// Capacity of the data section of the buffer.
func (t *Transmitter) Capacity() int32 { return t.capacity }

// MaxMsgLength is the largest message that can be transmitted.
func (t *Transmitter) MaxMsgLength() int32 { return t.maxMsgLength }

// Transmit broadcasts length bytes of srcBuffer at srcIndex as msgTypeID. It panics on invalid type id or
// a message longer than MaxMsgLength.
func (t *Transmitter) Transmit(msgTypeID int32, srcBuffer *atomic.Buffer, srcIndex, length int32) {
	rb.CheckMsgTypeID(msgTypeID)
	if length < 0 || length > t.maxMsgLength {
		panic(fmt.Sprintf("encoded message exceeds maxMsgLength of %d, length=%d", t.maxMsgLength, length))
	}

	buffer := t.buffer
	currentTail := buffer.GetInt64(t.tailCounterIndex)
	recordOffset := int32(currentTail & int64(t.capacity-1))
	recordLength := length + rb.RecordDescriptor.HeaderLength
	recordLengthAligned := util.AlignInt32(recordLength, rb.RecordDescriptor.RecordAlignment)
	newTail := currentTail + int64(recordLengthAligned)

	toEndOfBuffer := t.capacity - recordOffset
	if toEndOfBuffer < recordLengthAligned {
		t.signalTailIntent(newTail + int64(toEndOfBuffer))
		buffer.PutInt32(rb.LengthOffset(recordOffset), toEndOfBuffer)
		buffer.PutInt32(rb.TypeOffset(recordOffset), rb.RecordDescriptor.PaddingMsgTypeID)
		currentTail += int64(toEndOfBuffer)
		recordOffset = 0
	} else {
		t.signalTailIntent(newTail)
	}

	buffer.PutInt32(rb.LengthOffset(recordOffset), recordLength)
	buffer.PutInt32(rb.TypeOffset(recordOffset), msgTypeID)
	buffer.PutBytes(rb.EncodedMsgOffset(recordOffset), srcBuffer, srcIndex, length)

	buffer.PutInt64Ordered(t.latestCounterIndex, currentTail)
	buffer.PutInt64Ordered(t.tailCounterIndex, currentTail+int64(recordLengthAligned))
}

func (t *Transmitter) signalTailIntent(newTail int64) {
	t.buffer.PutInt64Ordered(t.tailIntentCounterIndex, newTail)
	atomic.ReleaseFence()
}
