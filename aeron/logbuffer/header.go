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

// Header is a flyweight over the data frame header of a fragment delivered to a handler. It matches Java's
// io.aeron.logbuffer.Header.
type Header struct {
	buffer                atomic.Buffer
	offset                int32
	initialTermID         int32
	positionBitsToShift   int32
	fragmentedFrameLength int32
	context               interface{}
}

// NewHeader creates a Header for the given initial term id and position bits, with an optional context object
// (an Image when created by the client).
func NewHeader(initialTermID int32, positionBitsToShift int32, context interface{}) *Header {
	return &Header{initialTermID: initialTermID, positionBitsToShift: positionBitsToShift, context: context}
}

//go:norace
func (hdr *Header) Wrap(ptr unsafe.Pointer, length int32) *Header {
	hdr.buffer.Wrap(ptr, length)
	return hdr
}

// Position calculates the current position to which the image has advanced on
// reading this message.
func (hdr *Header) Position() int64 {
	return computePosition(hdr.TermId(), hdr.NextTermOffset(), hdr.positionBitsToShift, hdr.initialTermID)
}

// TermOffset returns the offset of the frame within its term, as stored in the frame header.
func (hdr *Header) TermOffset() int32 {
	return hdr.buffer.GetInt32(hdr.offset + DataFrameHeader.TermOffsetFieldOffset)
}

// NextTermOffset returns the term offset immediately after this frame (or after the whole reassembled message
// when FragmentedFrameLength has been set by an assembler), aligned to FrameAlignment.
func (hdr *Header) NextTermOffset() int32 {
	return util.AlignInt32(hdr.TermOffset()+hdr.termOccupancyLength(), FrameAlignment)
}

// Type returns the frame type (HDR_TYPE_DATA, HDR_TYPE_PAD, ...).
func (hdr *Header) Type() uint16 {
	return hdr.buffer.GetUInt16(typeOffset(hdr.offset))
}

// ReservedValue returns the reserved value of the frame header.
func (hdr *Header) ReservedValue() int64 {
	return GetReservedValue(&hdr.buffer, hdr.offset)
}

// PositionBitsToShift returns the number of bits to shift a term count by to get a position.
func (hdr *Header) PositionBitsToShift() int32 {
	return hdr.positionBitsToShift
}

// Buffer returns the underlying buffer (typically a term buffer) the header is wrapping.
func (hdr *Header) Buffer() *atomic.Buffer {
	return &hdr.buffer
}

// Context returns the context associated with the header. When delivered by an Image poll this is the Image.
func (hdr *Header) Context() interface{} {
	return hdr.context
}

// SetContext sets the context associated with the header.
func (hdr *Header) SetContext(context interface{}) *Header {
	hdr.context = context
	return hdr
}

// FragmentedFrameLength returns the total length of a reassembled fragmented message including the first frame
// header, as set by a fragment assembler, or a value <= 0 (NullValue) when not set.
func (hdr *Header) FragmentedFrameLength() int32 {
	if hdr.fragmentedFrameLength <= 0 {
		return -1
	}
	return hdr.fragmentedFrameLength
}

// SetFragmentedFrameLength is used by fragment assemblers so that Position() and NextTermOffset() account for the
// whole reassembled message. Set to -1 to clear.
func (hdr *Header) SetFragmentedFrameLength(fragmentedFrameLength int32) *Header {
	hdr.fragmentedFrameLength = fragmentedFrameLength
	return hdr
}

func (hdr *Header) termOccupancyLength() int32 {
	if hdr.fragmentedFrameLength > 0 {
		return hdr.fragmentedFrameLength
	}
	return hdr.FrameLength()
}

func (hdr *Header) Offset() int32 {
	return hdr.offset
}

func (hdr *Header) Flags() uint8 {
	return GetFlags(&hdr.buffer, hdr.offset)
}

func (hdr *Header) FrameLength() int32 {
	return GetFrameLength(&hdr.buffer, hdr.offset)
}

func (hdr *Header) TermId() int32 {
	return GetTermId(&hdr.buffer, hdr.offset)
}

func (hdr *Header) SessionId() int32 {
	return GetSessionId(&hdr.buffer, hdr.offset)
}

func (hdr *Header) StreamId() int32 {
	return GetStreamId(&hdr.buffer, hdr.offset)
}

// GetReservedValue returns the reserved value of the frame header.
//
// Deprecated: use ReservedValue.
func (hdr *Header) GetReservedValue() int64 {
	return GetReservedValue(&hdr.buffer, hdr.offset)
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

// computePosition computes the current position in absolute number of bytes.
func computePosition(activeTermId int32, termOffset int32, positionBitsToShift int32, initialTermId int32) int64 {
	termCount := activeTermId - initialTermId // copes with negative activeTermId on rollover
	return (int64(termCount) << uint32(positionBitsToShift)) + int64(termOffset)
}
