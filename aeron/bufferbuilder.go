// Copyright 2026 Kalshi Trading
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package aeron

import (
	"fmt"
	"math"
	"unsafe"

	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/logbuffer"
)

const (
	// BufferBuilderMaxCapacity is the maximum capacity to which a BufferBuilder can grow.
	BufferBuilderMaxCapacity = int32(math.MaxInt32 - 8)
	// BufferBuilderInitMinCapacity is the minimum capacity a BufferBuilder allocates once it grows.
	BufferBuilderInitMinCapacity = int32(4096)
)

// BufferBuilder is a reusable, growable buffer used to reassemble fragmented messages. It mirrors
// io.aeron.BufferBuilder: besides the payload it captures the header of the first fragment so that
// the Header delivered for a reassembled message describes the whole message (see CompleteHeader).
//
// A BufferBuilder is not safe for concurrent use.
type BufferBuilder struct {
	limit            int32
	nextTermOffset   int32
	firstFrameLength int32
	data             []byte
	buffer           atomic.Buffer
	headerData       []byte
	headerBuffer     atomic.Buffer
	completeHeader   logbuffer.Header
}

// NewBufferBuilder creates a BufferBuilder with the given initial capacity, which may be 0 to
// allocate lazily on first Append.
func NewBufferBuilder(initialCapacity int32) (*BufferBuilder, error) {
	if initialCapacity < 0 || initialCapacity > BufferBuilderMaxCapacity {
		return nil, fmt.Errorf("initialCapacity outside range 0 - %d: initialCapacity=%d",
			BufferBuilderMaxCapacity, initialCapacity)
	}
	b := &BufferBuilder{nextTermOffset: NullValue}
	if initialCapacity > 0 {
		b.wrapData(make([]byte, initialCapacity))
	}
	b.headerData = make([]byte, logbuffer.DataFrameHeader.Length)
	b.headerBuffer.Wrap(unsafe.Pointer(&b.headerData[0]), int32(len(b.headerData)))
	return b, nil
}

// Capacity is the current allocated capacity of the builder.
func (b *BufferBuilder) Capacity() int32 {
	return int32(len(b.data))
}

// Limit is the number of bytes appended so far.
func (b *BufferBuilder) Limit() int32 {
	return b.limit
}

// SetLimit sets the limit (e.g. to roll back an append). It must be in the range 0..Capacity().
func (b *BufferBuilder) SetLimit(limit int32) error {
	if limit < 0 || limit > b.Capacity() {
		return fmt.Errorf("limit outside range: capacity=%d limit=%d", b.Capacity(), limit)
	}
	b.limit = limit
	return nil
}

// NextTermOffset is the term offset expected for the next fragment, or NullValue if not assembling.
func (b *BufferBuilder) NextTermOffset() int32 {
	return b.nextTermOffset
}

// SetNextTermOffset sets the term offset expected for the next fragment.
func (b *BufferBuilder) SetNextTermOffset(offset int32) *BufferBuilder {
	b.nextTermOffset = offset
	return b
}

// Buffer is the underlying buffer, valid from 0 to Limit(). It is re-wrapped when the builder
// grows, so do not retain it across calls to Append or Compact.
func (b *BufferBuilder) Buffer() *atomic.Buffer {
	return &b.buffer
}

// Bytes returns the appended bytes, aliasing the builder's storage until the next mutation.
func (b *BufferBuilder) Bytes() []byte {
	return b.data[:b.limit]
}

// Reset the builder so it can be reused for a new message. Capacity is retained.
func (b *BufferBuilder) Reset() *BufferBuilder {
	b.limit = 0
	b.nextTermOffset = NullValue
	b.completeHeader.SetContext(nil).SetFragmentedFrameLength(NullValue)
	return b
}

// Compact shrinks the capacity to max(BufferBuilderInitMinCapacity, Limit()) if that is smaller than
// the current capacity.
func (b *BufferBuilder) Compact() *BufferBuilder {
	newCapacity := b.limit
	if newCapacity < BufferBuilderInitMinCapacity {
		newCapacity = BufferBuilderInitMinCapacity
	}
	if newCapacity < b.Capacity() {
		b.resize(newCapacity)
	}
	return b
}

// Append length bytes from srcBuffer at srcOffset, growing the capacity as required. It panics if
// the resulting length would exceed BufferBuilderMaxCapacity, like bytes.Buffer does on overflow.
func (b *BufferBuilder) Append(srcBuffer *atomic.Buffer, srcOffset int32, length int32) *BufferBuilder {
	b.ensureCapacity(length)
	if length > 0 {
		b.buffer.PutBytes(b.limit, srcBuffer, srcOffset, length)
		b.limit += length
	}
	return b
}

// AppendBytes appends the given bytes, growing the capacity as required.
func (b *BufferBuilder) AppendBytes(src []byte) *BufferBuilder {
	b.ensureCapacity(int32(len(src)))
	b.limit += int32(copy(b.data[b.limit:], src))
	return b
}

// CaptureHeader copies the frame header of the first fragment of a message so it can later be
// completed into a header describing the whole reassembled message.
func (b *BufferBuilder) CaptureHeader(header *logbuffer.Header) *BufferBuilder {
	b.completeHeader.
		SetInitialTermID(header.InitialTermID()).
		SetPositionBitsToShift(header.PositionBitsToShift()).
		SetOffset(0).
		Wrap(b.headerBuffer.Ptr(), b.headerBuffer.Capacity())
	b.firstFrameLength = header.FrameLength()
	b.headerBuffer.PutBytes(0, header.Buffer(), header.Offset(), logbuffer.DataFrameHeader.Length)
	return b
}

// CompleteHeader builds the header for the reassembled message from the captured first-fragment header
// and the header of the last fragment: the frame length covers the whole message, flags are the union
// of first and last fragment flags (so BEGIN|END), and position, term offset, session id, stream id,
// term id and reserved value are those of the first fragment, with Position() reporting the position
// after the last fragment. The returned Header is owned by the builder.
func (b *BufferBuilder) CompleteHeader(header *logbuffer.Header) *logbuffer.Header {
	maxPayloadLength := b.firstFrameLength - logbuffer.DataFrameHeader.Length
	fragmentedFrameLength := logbuffer.ComputeFragmentedFrameLength(b.limit, maxPayloadLength)
	b.completeHeader.SetContext(header.Context()).SetFragmentedFrameLength(fragmentedFrameLength)

	b.headerBuffer.PutInt32(logbuffer.DataFrameHeader.FrameLengthFieldOffset, logbuffer.DataFrameHeader.Length+b.limit)
	flagsOffset := logbuffer.DataFrameHeader.FlagsFieldOffset
	b.headerBuffer.PutUInt8(flagsOffset, b.headerBuffer.GetUInt8(flagsOffset)|header.Flags())

	return &b.completeHeader
}

func (b *BufferBuilder) ensureCapacity(additionalLength int32) {
	requiredCapacity := int64(b.limit) + int64(additionalLength)
	capacity := b.Capacity()
	if requiredCapacity > int64(capacity) {
		if requiredCapacity > int64(BufferBuilderMaxCapacity) {
			panic(fmt.Sprintf("insufficient capacity: maxCapacity=%d limit=%d additionalLength=%d",
				BufferBuilderMaxCapacity, b.limit, additionalLength))
		}
		b.resize(findSuitableCapacity(capacity, requiredCapacity))
	}
}

func (b *BufferBuilder) resize(newCapacity int32) {
	data := make([]byte, newCapacity)
	copy(data, b.data[:b.limit])
	b.wrapData(data)
}

func (b *BufferBuilder) wrapData(data []byte) {
	b.data = data
	if len(data) == 0 {
		b.buffer.Wrap(nil, 0)
		return
	}
	b.buffer.Wrap(unsafe.Pointer(&data[0]), int32(len(data)))
}

func findSuitableCapacity(capacity int32, requiredCapacity int64) int32 {
	newCapacity := int64(capacity)
	if newCapacity < int64(BufferBuilderInitMinCapacity) {
		newCapacity = int64(BufferBuilderInitMinCapacity)
	}
	for newCapacity < requiredCapacity {
		newCapacity = newCapacity + (newCapacity >> 1)
		if newCapacity > int64(BufferBuilderMaxCapacity) {
			newCapacity = int64(BufferBuilderMaxCapacity)
			break
		}
	}
	return int32(newCapacity)
}
