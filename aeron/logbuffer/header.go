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
)

// Header is a flyweight over a data frame header in a term buffer, as passed to fragment handlers.
// It also carries the stream context (initial term id, position bits to shift, an optional user context) and the
// fragmented frame length set by fragment assemblers.
type Header struct {
	buffer              atomic.Buffer
	offset              int32
	initialTermID       int32
	positionBitsToShift int32
	// fragmentedFrameLength is the term occupancy of a reassembled message, 0 when not reassembled.
	fragmentedFrameLength int32
	context               interface{}
}

// NullValue is the value returned by FragmentedFrameLength when it has not been set.
const NullValue int32 = -1

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

// Position calculates the position the image will have advanced to on consuming this message, taking into account
// the fragmented frame length when the message was reassembled.
func (hdr *Header) Position() int64 {
	return ComputePosition(hdr.TermID(), hdr.NextTermOffset(), hdr.positionBitsToShift, hdr.initialTermID)
}

// Offset is the offset of the frame header within the buffer.
func (hdr *Header) Offset() int32 {
	return hdr.offset
}

func (hdr *Header) Flags() uint8 {
	return GetFlags(&hdr.buffer, hdr.offset)
}

func (hdr *Header) FrameLength() int32 {
	return GetFrameLength(&hdr.buffer, hdr.offset)
}

func (hdr *Header) TermID() int32 {
	return frameTermID(&hdr.buffer, hdr.offset)
}

func (hdr *Header) SessionID() int32 {
	return frameSessionID(&hdr.buffer, hdr.offset)
}

func (hdr *Header) StreamID() int32 {
	return frameStreamID(&hdr.buffer, hdr.offset)
}

// Deprecated: use ReservedValue.
func (hdr *Header) GetReservedValue() int64 {
	return hdr.ReservedValue()
}

func (hdr *Header) SetOffset(offset int32) *Header {
	hdr.offset = offset
	return hdr
}

func (hdr *Header) InitialTermID() int32 {
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

func (hdr *Header) SetSessionID(value int32) *Header {
	hdr.buffer.PutInt32(sessionIdOffset(hdr.offset), value)
	return hdr
}
