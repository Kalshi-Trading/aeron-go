/*
Copyright 2016 Stanislav Liberman

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package broadcast

import (
	"fmt"

	"github.com/lirm/aeron-go/aeron/atomic"
	rb "github.com/lirm/aeron-go/aeron/ringbuffer"
	"github.com/lirm/aeron-go/aeron/util"
)

// BufferDescriptor is the layout of the broadcast buffer trailer (Agrona BroadcastBufferDescriptor).
var BufferDescriptor = struct {
	tailIntentCounterOffset int32
	tailCounterOffset       int32
	latestCounterOffset     int32
	trailerLength           int32
}{
	0,
	util.SizeOfInt64,
	util.SizeOfInt64 * 2,
	util.CacheLineLength * 2,
}

// TrailerLength is the length of the broadcast buffer trailer.
const TrailerLength = util.CacheLineLength * 2

func checkCapacity(capacity int32) error {
	if util.IsPowerOfTwo(int64(capacity)) {
		return nil
	}
	return fmt.Errorf("capacity must be a positive power of 2 + TRAILER_LENGTH: capacity=%d", capacity)
}

// Receiver receives messages from a single-producer broadcast buffer (Agrona BroadcastReceiver). It is
// not safe for concurrent use; each consumer needs its own Receiver.
type Receiver struct {
	buffer                 *atomic.Buffer
	capacity               int32
	mask                   int64
	tailIntentCounterIndex int32
	tailCounterIndex       int32
	latestCounterIndex     int32

	recordOffset int32
	cursor       int64
	nextRecord   int64

	lappedCount atomic.Long
}

// NewReceiver wraps buffer, whose capacity must be a power of two plus TrailerLength. Like Agrona, the
// receiver starts at the latest record in the buffer.
func NewReceiver(buffer *atomic.Buffer) (*Receiver, error) {
	recv := new(Receiver)
	recv.buffer = buffer
	recv.capacity = buffer.Capacity() - BufferDescriptor.trailerLength

	if err := checkCapacity(recv.capacity); err != nil {
		return nil, err
	}
	if err := buffer.VerifyAlignment(); err != nil {
		return nil, err
	}

	recv.mask = int64(recv.capacity) - 1
	recv.tailIntentCounterIndex = recv.capacity + BufferDescriptor.tailIntentCounterOffset
	recv.tailCounterIndex = recv.capacity + BufferDescriptor.tailCounterOffset
	recv.latestCounterIndex = recv.capacity + BufferDescriptor.latestCounterOffset

	recv.cursor = buffer.GetInt64Volatile(recv.latestCounterIndex)
	recv.nextRecord = recv.cursor
	recv.recordOffset = int32(recv.cursor & recv.mask)

	return recv, nil
}

// Capacity of the data section of the buffer.
func (recv *Receiver) Capacity() int32 { return recv.capacity }

// Buffer returns the underlying buffer.
func (recv *Receiver) Buffer() *atomic.Buffer { return recv.buffer }

// LappedCount is the number of times the transmitter has lapped this receiver.
func (recv *Receiver) LappedCount() int64 { return recv.lappedCount.Get() }

// GetLappedCount is the number of times the transmitter has lapped this receiver.
//
// Deprecated: use LappedCount.
func (recv *Receiver) GetLappedCount() int64 { return recv.lappedCount.Get() }

// TypeID of the message received by the last ReceiveNext.
func (recv *Receiver) TypeID() int32 { return recv.buffer.GetInt32(rb.TypeOffset(recv.recordOffset)) }

// Offset in Buffer of the body of the message received by the last ReceiveNext.
func (recv *Receiver) Offset() int32 { return rb.EncodedMsgOffset(recv.recordOffset) }

// Length of the body of the message received by the last ReceiveNext.
func (recv *Receiver) Length() int32 {
	return recv.buffer.GetInt32(rb.LengthOffset(recv.recordOffset)) - rb.RecordDescriptor.HeaderLength
}

// Validate reports whether the message received by the last ReceiveNext is still valid, i.e. it has not
// been overwritten by the transmitter. Call it after reading the message.
func (recv *Receiver) Validate() bool {
	atomic.AcquireFence()
	return recv.validate(recv.cursor)
}

func (recv *Receiver) validate(cursor int64) bool {
	return (cursor + int64(recv.capacity)) > recv.buffer.GetInt64Volatile(recv.tailIntentCounterIndex)
}

// ReceiveNext advances to the next available message, returning false if there is none. If the receiver
// has been lapped it skips to the latest message and increments LappedCount.
func (recv *Receiver) ReceiveNext() bool {
	buffer := recv.buffer
	tail := buffer.GetInt64Volatile(recv.tailCounterIndex)
	cursor := recv.nextRecord

	if tail <= cursor {
		return false
	}

	recordOffset := int32(cursor & recv.mask)
	if !recv.validate(cursor) {
		recv.lappedCount.Inc()
		cursor = buffer.GetInt64Volatile(recv.latestCounterIndex)
		recordOffset = int32(cursor & recv.mask)
	}

	recv.cursor = cursor
	recv.nextRecord = cursor + int64(util.AlignInt32(buffer.GetInt32(rb.LengthOffset(recordOffset)), rb.RecordDescriptor.RecordAlignment))

	if rb.RecordDescriptor.PaddingMsgTypeID == buffer.GetInt32(rb.TypeOffset(recordOffset)) {
		recordOffset = 0
		recv.cursor = recv.nextRecord
		recv.nextRecord += int64(util.AlignInt32(buffer.GetInt32(rb.LengthOffset(recordOffset)), rb.RecordDescriptor.RecordAlignment))
	}

	recv.recordOffset = recordOffset
	return true
}
