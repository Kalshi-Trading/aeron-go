package driver

import (
	"testing"

	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/broadcast"
	rb "github.com/lirm/aeron-go/aeron/ringbuffer"
	"github.com/lirm/aeron-go/aeron/util"
	"github.com/stretchr/testify/require"
)

const (
	onStaticCounter          int32 = 0x0F0B
	onPublicationError       int32 = 0x0F0C
	onNextAvailableSessionID int32 = 0x0F0D
)

type counterReady struct {
	correlationID int64
	counterID     int32
}

type recordingListener struct {
	Listener
	countersReady []counterReady
}

func (l *recordingListener) OnAvailableCounter(correlationID int64, counterID int32) {
	l.countersReady = append(l.countersReady, counterReady{correlationID: correlationID, counterID: counterID})
}

type broadcastBuffer struct {
	buffer   *atomic.Buffer
	capacity int32
}

func newBroadcastBuffer(capacity int32) *broadcastBuffer {
	return &broadcastBuffer{
		buffer:   atomic.MakeBuffer(make([]byte, capacity+2*util.CacheLineLength)),
		capacity: capacity,
	}
}

func (b *broadcastBuffer) transmit(msgTypeID int32, msg []byte) {
	tailIntentIndex := b.capacity
	tailIndex := b.capacity + util.SizeOfInt64
	latestIndex := b.capacity + 2*util.SizeOfInt64

	tail := b.buffer.GetInt64(tailIndex)
	recordOffset := int32(tail) & (b.capacity - 1)
	recordLength := rb.RecordDescriptor.HeaderLength + int32(len(msg))
	newTail := tail + int64(util.AlignInt32(recordLength, rb.RecordDescriptor.RecordAlignment))

	b.buffer.PutInt64Ordered(tailIntentIndex, newTail)
	b.buffer.PutInt32(rb.LengthOffset(recordOffset), recordLength)
	b.buffer.PutInt32(rb.TypeOffset(recordOffset), msgTypeID)
	b.buffer.PutBytesArray(rb.EncodedMsgOffset(recordOffset), &msg, 0, int32(len(msg)))
	b.buffer.PutInt64Ordered(latestIndex, tail)
	b.buffer.PutInt64Ordered(tailIndex, newTail)
}

func TestReceiveMessagesIgnoresUnknownDriverEvents(t *testing.T) {
	bcast := newBroadcastBuffer(1024)
	receiver, err := broadcast.NewReceiver(bcast.buffer)
	require.NoError(t, err)
	listener := &recordingListener{}
	adapter := NewAdapter(listener, broadcast.NewCopyReceiver(receiver))

	for _, msgTypeID := range []int32{onStaticCounter, onPublicationError, onNextAvailableSessionID} {
		bcast.transmit(msgTypeID, make([]byte, 16))
		require.Equal(t, 1, adapter.ReceiveMessages(), "message type %#x", msgTypeID)
		require.Empty(t, listener.countersReady, "message type %#x", msgTypeID)
	}

	ready := make([]byte, 12)
	var msg counterUpdate
	msg.Wrap(atomic.MakeBuffer(ready), 0)
	msg.correlationID.Set(42)
	msg.counterID.Set(7)
	bcast.transmit(Events.OnCounterReady, ready)
	require.Equal(t, 1, adapter.ReceiveMessages())
	require.Equal(t, []counterReady{{correlationID: 42, counterID: 7}}, listener.countersReady)

	require.Equal(t, 0, adapter.ReceiveMessages())
}
