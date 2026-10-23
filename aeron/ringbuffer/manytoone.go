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

package rb

import (
	"fmt"
	"math"

	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/util"
)

// InsufficientCapacity is returned by TryClaim when there is no space for the message.
const InsufficientCapacity int32 = -2

const insufficientCapacity = InsufficientCapacity

// MinCapacity is the minimum data capacity of a ManyToOne ring buffer.
var MinCapacity = RecordDescriptor.HeaderLength

// ManyToOne is a ring buffer supporting many concurrent producers and a single consumer, binary
// compatible with Agrona's ManyToOneRingBuffer (used for the client -> driver command buffer in CnC).
type ManyToOne struct {
	buffer                    *atomic.Buffer
	capacity                  int32
	maxMsgLength              int32
	headPositionIndex         int32
	headCachePositionIndex    int32
	tailPositionIndex         int32
	correlationIDCounterIndex int32
	consumerHeartbeatIndex    int32
}

// NewManyToOne wraps buffer, which must be a power of two plus TrailerLength in size and 8-byte aligned.
func NewManyToOne(buffer *atomic.Buffer) (*ManyToOne, error) {
	rb := new(ManyToOne)
	if err := rb.init(buffer); err != nil {
		return nil, err
	}
	return rb, nil
}

// Init is the main initialization method. It panics if buffer has an invalid capacity or alignment; use
// NewManyToOne to get an error instead.
func (buf *ManyToOne) Init(buffer *atomic.Buffer) *ManyToOne {
	if err := buf.init(buffer); err != nil {
		panic(err)
	}
	return buf
}

func (buf *ManyToOne) init(buffer *atomic.Buffer) error {
	capacity, err := CheckCapacity(buffer.Capacity(), MinCapacity)
	if err != nil {
		return err
	}
	if err := buffer.VerifyAlignment(); err != nil {
		return err
	}

	buf.buffer = buffer
	buf.capacity = capacity
	if capacity == MinCapacity {
		buf.maxMsgLength = 0
	} else {
		buf.maxMsgLength = capacity >> 3
		if buf.maxMsgLength < RecordDescriptor.HeaderLength {
			buf.maxMsgLength = RecordDescriptor.HeaderLength
		}
	}
	buf.tailPositionIndex = capacity + descriptor.tailPositionOffset
	buf.headCachePositionIndex = capacity + descriptor.headCachePositionOffset
	buf.headPositionIndex = capacity + descriptor.headPositionOffset
	buf.correlationIDCounterIndex = capacity + descriptor.correlationCounterOffset
	buf.consumerHeartbeatIndex = capacity + descriptor.consumerHeartbeatOffset
	return nil
}

// Capacity of the data section of the ring buffer.
func (buf *ManyToOne) Capacity() int32 {
	return buf.capacity
}

// MaxMsgLength is the largest message that can be written.
func (buf *ManyToOne) MaxMsgLength() int32 {
	return buf.maxMsgLength
}

// Buffer returns the underlying buffer.
func (buf *ManyToOne) Buffer() *atomic.Buffer {
	return buf.buffer
}

// NextCorrelationID atomically allocates a new correlation id, unique across all users of the buffer.
func (buf *ManyToOne) NextCorrelationID() int64 {
	return buf.buffer.GetAndAddInt64(buf.correlationIDCounterIndex, 1)
}

// ConsumerHeartbeatTime returns the time of the last consumer heartbeat (epoch ms for the driver).
func (buf *ManyToOne) ConsumerHeartbeatTime() int64 {
	return buf.buffer.GetInt64Volatile(buf.consumerHeartbeatIndex)
}

// SetConsumerHeartbeatTime records the consumer heartbeat time.
func (buf *ManyToOne) SetConsumerHeartbeatTime(time int64) {
	buf.buffer.PutInt64Ordered(buf.consumerHeartbeatIndex, time)
}

// ProducerPosition is the position up to which producers have claimed space.
func (buf *ManyToOne) ProducerPosition() int64 {
	return buf.buffer.GetInt64Volatile(buf.tailPositionIndex)
}

// ConsumerPosition is the position up to which the consumer has read.
func (buf *ManyToOne) ConsumerPosition() int64 {
	return buf.buffer.GetInt64Volatile(buf.headPositionIndex)
}

// Size returns the number of bytes currently occupied, consistent between head and tail.
func (buf *ManyToOne) Size() int32 {
	var headBefore, tail int64
	headAfter := buf.buffer.GetInt64Volatile(buf.headPositionIndex)
	for {
		headBefore = headAfter
		tail = buf.buffer.GetInt64Volatile(buf.tailPositionIndex)
		headAfter = buf.buffer.GetInt64Volatile(buf.headPositionIndex)
		if headAfter == headBefore {
			break
		}
	}
	size := tail - headAfter
	if size < 0 {
		return 0
	} else if size > int64(buf.capacity) {
		return buf.capacity
	}
	return int32(size)
}

func (buf *ManyToOne) checkMsgLength(length int32) {
	if length < 0 {
		panic(fmt.Sprintf("invalid message length=%d", length))
	} else if length > buf.maxMsgLength {
		panic(fmt.Sprintf("encoded message exceeds maxMsgLength=%d, length=%d", buf.maxMsgLength, length))
	}
}

// Write appends length bytes from srcBuffer at srcIndex as a message of msgTypeID. It returns false if
// the ring buffer is full. It panics if msgTypeID < 1 or length exceeds MaxMsgLength.
func (buf *ManyToOne) Write(msgTypeID int32, srcBuffer *atomic.Buffer, srcIndex int32, length int32) bool {
	checkMsgTypeID(msgTypeID)
	buf.checkMsgLength(length)

	recordLength := length + RecordDescriptor.HeaderLength
	recordIndex := buf.claimCapacity(recordLength)
	if insufficientCapacity == recordIndex {
		return false
	}

	buffer := buf.buffer
	buffer.PutInt64Ordered(recordIndex, makeHeader(-recordLength, msgTypeID))
	atomic.ReleaseFence()
	buffer.PutBytes(EncodedMsgOffset(recordIndex), srcBuffer, srcIndex, length)
	buffer.PutInt32Ordered(LengthOffset(recordIndex), recordLength)
	return true
}

// TryClaim claims space for a message of length bytes and returns the index at which to write the
// message body, or InsufficientCapacity. The claim must be completed with Commit or Abort.
func (buf *ManyToOne) TryClaim(msgTypeID int32, length int32) int32 {
	checkMsgTypeID(msgTypeID)
	buf.checkMsgLength(length)

	recordLength := length + RecordDescriptor.HeaderLength
	recordIndex := buf.claimCapacity(recordLength)
	if insufficientCapacity == recordIndex {
		return recordIndex
	}

	buf.buffer.PutInt64Ordered(recordIndex, makeHeader(-recordLength, msgTypeID))
	atomic.ReleaseFence()
	return EncodedMsgOffset(recordIndex)
}

// Commit publishes a message previously claimed with TryClaim; index is the value TryClaim returned.
func (buf *ManyToOne) Commit(index int32) {
	recordIndex := buf.computeRecordIndex(index)
	recordLength := buf.verifyClaimedSpaceNotReleased(recordIndex)
	buf.buffer.PutInt32Ordered(LengthOffset(recordIndex), -recordLength)
}

// Abort turns a message previously claimed with TryClaim into padding.
func (buf *ManyToOne) Abort(index int32) {
	recordIndex := buf.computeRecordIndex(index)
	recordLength := buf.verifyClaimedSpaceNotReleased(recordIndex)
	buf.buffer.PutInt32(TypeOffset(recordIndex), RecordDescriptor.PaddingMsgTypeID)
	buf.buffer.PutInt32Ordered(LengthOffset(recordIndex), -recordLength)
}

func (buf *ManyToOne) computeRecordIndex(index int32) int32 {
	recordIndex := index - RecordDescriptor.HeaderLength
	if recordIndex < 0 || recordIndex > buf.capacity-RecordDescriptor.HeaderLength {
		panic(fmt.Sprintf("invalid message index %d", index))
	}
	return recordIndex
}

func (buf *ManyToOne) verifyClaimedSpaceNotReleased(recordIndex int32) int32 {
	recordLength := buf.buffer.GetInt32(LengthOffset(recordIndex))
	if recordLength < 0 {
		return recordLength
	}
	state := "committed"
	if RecordDescriptor.PaddingMsgTypeID == buf.buffer.GetInt32(TypeOffset(recordIndex)) {
		state = "aborted"
	}
	panic("claimed space previously " + state)
}

// Read dispatches up to messageCountLimit messages (<= 0 means no limit) to handler and returns the
// number read. Must only be called by the single consumer. The head is advanced even if handler panics.
func (buf *ManyToOne) Read(handler MessageHandler, messageCountLimit int) int {
	if messageCountLimit <= 0 {
		messageCountLimit = math.MaxInt
	}
	buffer := buf.buffer
	head := buffer.GetInt64(buf.headPositionIndex)
	headIndex := int32(head & int64(buf.capacity-1))
	maxBlockLength := buf.capacity - headIndex
	var bytesRead int32
	messagesRead := 0

	defer func() {
		if bytesRead > 0 {
			buffer.SetMemory(headIndex, bytesRead, 0)
			buffer.PutInt64Ordered(buf.headPositionIndex, head+int64(bytesRead))
		}
	}()

	for bytesRead < maxBlockLength && messagesRead < messageCountLimit {
		recordIndex := headIndex + bytesRead
		recordLength := buffer.GetInt32Volatile(LengthOffset(recordIndex))
		if recordLength <= 0 {
			break
		}
		bytesRead += util.AlignInt32(recordLength, RecordDescriptor.RecordAlignment)

		msgTypeID := buffer.GetInt32(TypeOffset(recordIndex))
		if RecordDescriptor.PaddingMsgTypeID == msgTypeID {
			continue
		}
		messagesRead++
		handler(msgTypeID, buffer, recordIndex+RecordDescriptor.HeaderLength, recordLength-RecordDescriptor.HeaderLength)
	}
	return messagesRead
}

// ControlledRead dispatches up to messageCountLimit messages (<= 0 means no limit) to handler, which
// controls how the head advances, and returns the number of messages consumed.
func (buf *ManyToOne) ControlledRead(handler ControlledMessageHandler, messageCountLimit int) int {
	if messageCountLimit <= 0 {
		messageCountLimit = math.MaxInt
	}
	buffer := buf.buffer
	head := buffer.GetInt64(buf.headPositionIndex)
	headIndex := int32(head & int64(buf.capacity-1))
	maxBlockLength := buf.capacity - headIndex
	var bytesRead int32
	messagesRead := 0

	defer func() {
		if bytesRead > 0 {
			buffer.SetMemory(headIndex, bytesRead, 0)
			buffer.PutInt64Ordered(buf.headPositionIndex, head+int64(bytesRead))
		}
	}()

	for bytesRead < maxBlockLength && messagesRead < messageCountLimit {
		recordIndex := headIndex + bytesRead
		recordLength := buffer.GetInt32Volatile(LengthOffset(recordIndex))
		if recordLength <= 0 {
			break
		}
		alignedLength := util.AlignInt32(recordLength, RecordDescriptor.RecordAlignment)
		bytesRead += alignedLength

		msgTypeID := buffer.GetInt32(TypeOffset(recordIndex))
		if RecordDescriptor.PaddingMsgTypeID == msgTypeID {
			continue
		}

		action := handler(msgTypeID, buffer, recordIndex+RecordDescriptor.HeaderLength, recordLength-RecordDescriptor.HeaderLength)
		if action == Abort {
			bytesRead -= alignedLength
			break
		}
		messagesRead++
		if action == Break {
			break
		}
		if action == Commit {
			buffer.SetMemory(headIndex, bytesRead, 0)
			buffer.PutInt64Ordered(buf.headPositionIndex, head+int64(bytesRead))
			headIndex += bytesRead
			head += int64(bytesRead)
			bytesRead = 0
		}
	}
	return messagesRead
}

// Unblock attempts to unblock the buffer when a producer has died mid-write, turning the blocking
// record into padding. Called by the consumer; returns true if a record was unblocked.
func (buf *ManyToOne) Unblock() bool {
	buffer := buf.buffer
	headPosition := buffer.GetInt64Volatile(buf.headPositionIndex)
	tailPosition := buffer.GetInt64Volatile(buf.tailPositionIndex)
	if headPosition == tailPosition {
		return false
	}

	mask := int64(buf.capacity - 1)
	consumerIndex := int32(headPosition & mask)
	producerIndex := int32(tailPosition & mask)

	unblocked := false
	length := buffer.GetInt32Volatile(consumerIndex)
	if length < 0 {
		buffer.PutInt32(TypeOffset(consumerIndex), RecordDescriptor.PaddingMsgTypeID)
		buffer.PutInt32Ordered(LengthOffset(consumerIndex), -length)
		unblocked = true
	} else if length == 0 {
		limit := buf.capacity
		if producerIndex > consumerIndex {
			limit = producerIndex
		}
		for i := consumerIndex + RecordDescriptor.RecordAlignment; i < limit; i += RecordDescriptor.RecordAlignment {
			if buffer.GetInt32Volatile(i) != 0 {
				if scanBackToConfirmStillZeroed(buffer, i, consumerIndex) {
					buffer.PutInt32(TypeOffset(consumerIndex), RecordDescriptor.PaddingMsgTypeID)
					buffer.PutInt32Ordered(LengthOffset(consumerIndex), i-consumerIndex)
					unblocked = true
				}
				break
			}
		}
	}
	return unblocked
}

func scanBackToConfirmStillZeroed(buffer *atomic.Buffer, from, limit int32) bool {
	for i := from - RecordDescriptor.RecordAlignment; i >= limit; i -= RecordDescriptor.RecordAlignment {
		if buffer.GetInt32Volatile(i) != 0 {
			return false
		}
	}
	return true
}

func (buf *ManyToOne) claimCapacity(recordLength int32) int32 {
	requiredCapacity := util.AlignInt32(recordLength, RecordDescriptor.RecordAlignment)
	buffer := buf.buffer
	capacity := buf.capacity
	mask := int64(capacity - 1)

	head := buffer.GetInt64Volatile(buf.headCachePositionIndex)

	var tail, newTail int64
	var tailIndex, padding, writeIndex int32
	for {
		tail = buffer.GetInt64Volatile(buf.tailPositionIndex)
		availableCapacity := capacity - int32(tail-head)

		if requiredCapacity > availableCapacity {
			head = buffer.GetInt64Volatile(buf.headPositionIndex)
			if requiredCapacity > capacity-int32(tail-head) {
				return insufficientCapacity
			}
			buffer.PutInt64Ordered(buf.headCachePositionIndex, head)
		}
		newTail = tail + int64(requiredCapacity)

		padding = 0
		tailIndex = int32(tail & mask)
		writeIndex = tailIndex
		toBufferEndLength := capacity - tailIndex

		if requiredCapacity > toBufferEndLength {
			headIndex := int32(head & mask)
			writeIndex = 0

			if requiredCapacity > headIndex {
				head = buffer.GetInt64Volatile(buf.headPositionIndex)
				headIndex = int32(head & mask)
				if requiredCapacity > headIndex {
					writeIndex = insufficientCapacity
					newTail = tail // only pad to the end of the buffer, claim no space
				}
				buffer.PutInt64Ordered(buf.headCachePositionIndex, head)
			}

			padding = toBufferEndLength
			newTail += int64(padding)
		}

		if buffer.CompareAndSetInt64(buf.tailPositionIndex, tail, newTail) {
			break
		}
	}

	if padding != 0 {
		buffer.PutInt64Ordered(tailIndex, makeHeader(-padding, RecordDescriptor.PaddingMsgTypeID))
		atomic.ReleaseFence()
		buffer.PutInt32Ordered(LengthOffset(tailIndex), padding)
	}

	return writeIndex
}
