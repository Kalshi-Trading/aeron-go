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
	"fmt"

	"github.com/lirm/aeron-go/aeron/util"
)

// PackTail packs a term id and term offset into a raw tail value.
func PackTail(termID int32, termOffset int32) int64 {
	return int64(termID)<<32 + int64(termOffset)
}

// TermOffset extracts the term offset from a raw tail, capped at termLength.
func TermOffset(rawTail int64, termLength int64) int32 {
	tail := rawTail & 0xFFFFFFFF
	if tail > termLength {
		tail = termLength
	}
	return int32(tail)
}

// IndexByTerm returns the partition index for a term id.
func IndexByTerm(initialTermID int32, activeTermID int32) int32 {
	return (activeTermID - initialTermID) % PartitionCount
}

// IndexByTermCount returns the partition index for a term count.
func IndexByTermCount(termCount int64) int32 {
	return int32(termCount % int64(PartitionCount))
}

// IndexByPosition returns the partition index for a stream position.
func IndexByPosition(position int64, positionBitsToShift int32) int32 {
	return int32((uint64(position) >> uint(positionBitsToShift)) % uint64(PartitionCount))
}

// NextPartitionIndex returns the partition index following currentIndex.
func NextPartitionIndex(currentIndex int32) int32 { return (currentIndex + 1) % PartitionCount }

// PreviousPartitionIndex returns the partition index preceding currentIndex.
func PreviousPartitionIndex(currentIndex int32) int32 {
	return (currentIndex + (PartitionCount - 1)) % PartitionCount
}

// ComputePosition computes a stream position from a term id and offset. Copes with term id wrap around.
func ComputePosition(activeTermID int32, termOffset int32, positionBitsToShift int32, initialTermID int32) int64 {
	termCount := int64(activeTermID - initialTermID)
	return termCount<<uint(positionBitsToShift) + int64(termOffset)
}

// ComputeTermBeginPosition computes the stream position at which a term begins.
func ComputeTermBeginPosition(activeTermID int32, positionBitsToShift int32, initialTermID int32) int64 {
	termCount := int64(activeTermID - initialTermID)
	return termCount << uint(positionBitsToShift)
}

// ComputeTermIDFromPosition computes the term id containing a stream position.
func ComputeTermIDFromPosition(position int64, positionBitsToShift int32, initialTermID int32) int32 {
	return int32(uint64(position)>>uint(positionBitsToShift)) + initialTermID
}

// ComputeTermOffsetFromPosition computes the offset within a term of a stream position.
func ComputeTermOffsetFromPosition(position int64, positionBitsToShift int32) int32 {
	return int32(position & (int64(1)<<uint(positionBitsToShift) - 1))
}

// ComputeLogLength computes the total length of a log file given the term length and file page size.
func ComputeLogLength(termLength int32, filePageSize int32) int64 {
	if termLength < 1024*1024*1024 {
		return alignInt64(int64(termLength)*int64(PartitionCount)+int64(LogMetaDataLength), int64(filePageSize))
	}
	alignedTermLength := alignInt64(int64(termLength), int64(filePageSize))
	return alignedTermLength*int64(PartitionCount) + alignInt64(int64(LogMetaDataLength), int64(filePageSize))
}

// PositionBitsToShift returns log2(termBufferLength). It panics if termBufferLength is not a valid term length,
// mirroring the IllegalArgumentException thrown by Java.
func PositionBitsToShift(termBufferLength int32) int32 {
	if err := CheckTermLength(termBufferLength); err != nil {
		panic(fmt.Sprintf("invalid term buffer length: %d", termBufferLength))
	}
	return int32(util.NumberOfTrailingZeroes(uint32(termBufferLength)))
}

// ComputeFragmentedFrameLength computes the term occupancy of a message of length bytes once fragmented into
// frames of at most maxPayloadSize payload bytes.
func ComputeFragmentedFrameLength(length int32, maxPayloadSize int32) int32 {
	numMaxPayloads := length / maxPayloadSize
	remainingPayload := length % maxPayloadSize
	var lastFrameLength int32
	if remainingPayload > 0 {
		lastFrameLength = util.AlignInt32(remainingPayload+DataFrameHeader.Length, FrameAlignment)
	}
	return numMaxPayloads*(maxPayloadSize+DataFrameHeader.Length) + lastFrameLength
}

// ComputeAssembledFrameLength computes the length of a reassembled message including a single frame header.
func ComputeAssembledFrameLength(length int32, maxPayloadSize int32) int32 {
	numMaxPayloads := length / maxPayloadSize
	remainingPayload := length % maxPayloadSize
	return DataFrameHeader.Length + numMaxPayloads*maxPayloadSize + remainingPayload
}

func alignInt64(value, alignment int64) int64 {
	return (value + (alignment - 1)) &^ (alignment - 1)
}
