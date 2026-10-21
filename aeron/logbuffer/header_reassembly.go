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

package logbuffer

import (
	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/util"
)

// NewHeader returns a Header for a stream with the given initial term id and position bits to shift.
func NewHeader(initialTermID int32, positionBitsToShift int32) *Header {
	return &Header{initialTermID: initialTermID, positionBitsToShift: positionBitsToShift}
}

// Buffer returns the buffer containing the frame header. The returned buffer is owned by the Header.
func (hdr *Header) Buffer() *atomic.Buffer {
	return &hdr.buffer
}

// PositionBitsToShift is the number of bits to shift a term count by to compute a position.
func (hdr *Header) PositionBitsToShift() int32 {
	return hdr.positionBitsToShift
}

// TermOffset is the offset in the term at which the frame begins, as written in the frame header.
func (hdr *Header) TermOffset() int32 {
	return hdr.buffer.GetInt32(hdr.offset + DataFrameHeader.TermOffsetFieldOffset)
}

// NextTermOffset is the offset in the term immediately after this frame (or, for a reassembled
// message, immediately after its last fragment).
func (hdr *Header) NextTermOffset() int32 {
	return util.AlignInt32(hdr.TermOffset()+hdr.termOccupancyLength(), FrameAlignment)
}

// Type is the frame type field of the header.
func (hdr *Header) Type() uint16 {
	return hdr.buffer.GetUInt16(typeOffset(hdr.offset))
}

// ReservedValue is the reserved value field of the header. Equivalent to GetReservedValue.
func (hdr *Header) ReservedValue() int64 {
	return hdr.GetReservedValue()
}

// FragmentedFrameLength is the total term occupancy of all fragments of a reassembled message, or -1
// if the header is for a single frame.
func (hdr *Header) FragmentedFrameLength() int32 {
	if hdr.fragmentedFrameLength == 0 {
		return -1
	}
	return hdr.fragmentedFrameLength
}

// SetFragmentedFrameLength sets the total term occupancy of a reassembled message, -1 (or 0) to clear.
func (hdr *Header) SetFragmentedFrameLength(fragmentedFrameLength int32) *Header {
	if fragmentedFrameLength < 0 {
		fragmentedFrameLength = 0
	}
	hdr.fragmentedFrameLength = fragmentedFrameLength
	return hdr
}

// Context is the optional user/context object associated with the header (e.g. the Image).
func (hdr *Header) Context() interface{} {
	return hdr.context
}

// SetContext sets the context object associated with the header.
func (hdr *Header) SetContext(context interface{}) *Header {
	hdr.context = context
	return hdr
}

func (hdr *Header) termOccupancyLength() int32 {
	if hdr.fragmentedFrameLength == 0 {
		return hdr.FrameLength()
	}
	return hdr.fragmentedFrameLength
}

// ComputeFragmentedFrameLength computes the term occupancy of a message of the given payload length
// once fragmented into frames carrying at most maxPayloadSize bytes each.
func ComputeFragmentedFrameLength(length int32, maxPayloadSize int32) int32 {
	numMaxPayloads := length / maxPayloadSize
	remainingPayload := length % maxPayloadSize
	var lastFrameLength int32
	if remainingPayload > 0 {
		lastFrameLength = util.AlignInt32(remainingPayload+DataFrameHeader.Length, FrameAlignment)
	}
	return numMaxPayloads*(maxPayloadSize+DataFrameHeader.Length) + lastFrameLength
}
