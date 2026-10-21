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

package logbuffer

import (
	"unsafe"

	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/util"
)

// Header is a flyweight over a data frame header in a term buffer, as passed to fragment handlers.
// It also carries the stream context (initial term id, position bits to shift, an optional user context) and the
// fragmented frame length set by fragment assemblers.
type Header struct {
	buffer              atomic.Buffer
	offset              int32
	initialTermID       int32
	positionBitsToShift int32
	context             interface{}
	// fragmentedFrameLength is stored +1 so that the zero value means "not set" (NullValue).
	fragmentedFrameLength int32
}

// NullValue is the value returned by FragmentedFrameLength when it has not been set.
const NullValue int32 = -1

// NewHeader constructs a header for a stream with the given initial term id, position bits to shift and context.
func NewHeader(initialTermID int32, positionBitsToShift int32, context interface{}) *Header {
	return &Header{initialTermID: initialTermID, positionBitsToShift: positionBitsToShift, context: context}
}

//go:norace
func (hdr *Header) Wrap(ptr unsafe.Pointer, length int32) *Header {
	hdr.buffer.Wrap(ptr, length)
	return hdr
}

// SetBuffer points the header at buffer (the term buffer containing the frame).
func (hdr *Header) SetBuffer(buffer *atomic.Buffer) *Header {
	hdr.buffer.Wrap(buffer.Ptr(), buffer.Capacity())
	return hdr
}

// Buffer returns the buffer containing the frame.
func (hdr *Header) Buffer() *atomic.Buffer {
	return &hdr.buffer
}

// Position calculates the position the image will have advanced to on consuming this message, taking into account
// the fragmented frame length when the message was reassembled.
func (hdr *Header) Position() int64 {
	return ComputePosition(hdr.TermId(), hdr.NextTermOffset(), hdr.positionBitsToShift, hdr.initialTermID)
}

// PositionBitsToShift is the number of times to left shift the term count to multiply by term length.
func (hdr *Header) PositionBitsToShift() int32 {
	return hdr.positionBitsToShift
}

// Context returns the user context associated with the stream, if any.
func (hdr *Header) Context() interface{} {
	return hdr.context
}

// SetContext sets the user context associated with the stream.
func (hdr *Header) SetContext(context interface{}) *Header {
	hdr.context = context
	return hdr
}

// Offset is the offset of the frame header within the buffer.
func (hdr *Header) Offset() int32 {
	return hdr.offset
}

func (hdr *Header) Flags() uint8 {
	return GetFlags(&hdr.buffer, hdr.offset)
}

// Type returns the frame type, which should always be DataFrameHeader.TypeData for frames passed to handlers.
func (hdr *Header) Type() uint16 {
	return FrameType(&hdr.buffer, hdr.offset)
}

func (hdr *Header) FrameLength() int32 {
	return GetFrameLength(&hdr.buffer, hdr.offset)
}

func (hdr *Header) TermId() int32 {
	return GetTermId(&hdr.buffer, hdr.offset)
}

// TermOffset is the offset of the frame in the term as recorded in the frame header.
func (hdr *Header) TermOffset() int32 {
	return GetFrameTermOffset(&hdr.buffer, hdr.offset)
}

// NextTermOffset is the term offset following this message (aligned), taking fragmentation into account.
func (hdr *Header) NextTermOffset() int32 {
	return util.AlignInt32(hdr.TermOffset()+hdr.termOccupancyLength(), FrameAlignment)
}

// FragmentedFrameLength is the total term occupancy of a reassembled message, or NullValue if not fragmented.
func (hdr *Header) FragmentedFrameLength() int32 {
	return hdr.fragmentedFrameLength - 1
}

// SetFragmentedFrameLength is set by fragment assemblers when delivering a reassembled message; pass NullValue
// to reset.
func (hdr *Header) SetFragmentedFrameLength(fragmentedFrameLength int32) *Header {
	hdr.fragmentedFrameLength = fragmentedFrameLength + 1
	return hdr
}

func (hdr *Header) termOccupancyLength() int32 {
	if hdr.fragmentedFrameLength == 0 {
		return hdr.FrameLength()
	}
	return hdr.fragmentedFrameLength - 1
}

func (hdr *Header) SessionId() int32 {
	return GetSessionId(&hdr.buffer, hdr.offset)
}

func (hdr *Header) StreamId() int32 {
	return GetStreamId(&hdr.buffer, hdr.offset)
}

func (hdr *Header) GetReservedValue() int64 {
	return GetReservedValue(&hdr.buffer, hdr.offset)
}

// ReservedValue returns the reserved value of the frame (alias of GetReservedValue matching Java naming).
func (hdr *Header) ReservedValue() int64 {
	return hdr.GetReservedValue()
}

func (hdr *Header) SetOffset(offset int32) *Header {
	hdr.offset = offset
	return hdr
}

func (hdr *Header) InitialTermId() int32 {
	return hdr.initialTermID
}

func (hdr *Header) SetInitialTermID(initialTermID int32) *Header {
	hdr.initialTermID = initialTermID
	return hdr
}

func (hdr *Header) SetPositionBitsToShift(positionBitsToShift int32) *Header {
	hdr.positionBitsToShift = positionBitsToShift
	return hdr
}

func (hdr *Header) SetReservedValue(reservedValue int64) *Header {
	hdr.buffer.PutInt64(reservedValueOffset(hdr.offset), reservedValue)
	return hdr
}

func (hdr *Header) SetSessionId(value int32) *Header {
	hdr.buffer.PutInt32(sessionIdOffset(hdr.offset), value)
	return hdr
}
