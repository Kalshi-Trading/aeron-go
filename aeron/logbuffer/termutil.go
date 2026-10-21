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

// TermRebuilderInsert inserts a packet of length bytes (starting at offset 0 of packet) into termBuffer at
// termOffset, if not already present. The header is written last with the frame length released so concurrent
// readers never see a partial frame. Port of io.aeron.logbuffer.TermRebuilder.insert.
func TermRebuilderInsert(termBuffer *atomic.Buffer, termOffset int32, packet *atomic.Buffer, length int32) {
	if termBuffer.GetInt32(termOffset) == 0 {
		hdrLen := DataFrameHeader.Length
		termBuffer.PutBytes(termOffset+hdrLen, packet, hdrLen, length-hdrLen)
		termBuffer.PutInt64(termOffset+24, packet.GetInt64(24))
		termBuffer.PutInt64(termOffset+16, packet.GetInt64(16))
		termBuffer.PutInt64(termOffset+8, packet.GetInt64(8))
		termBuffer.PutInt64Ordered(termOffset, packet.GetInt64(0))
	}
}

// GapHandler is called with the term id, offset and length of a detected gap.
type GapHandler func(termID int32, offset int32, length int32)

// TermGapScan scans termBuffer from termOffset up to limitOffset for the first gap, calling handler if one is found.
// Returns the offset at which the gap begins (or limit/last contiguous offset). Port of
// io.aeron.logbuffer.TermGapScanner.scanForGap.
func TermGapScan(termBuffer *atomic.Buffer, termID int32, termOffset int32, limitOffset int32,
	handler GapHandler) int32 {
	offset := termOffset
	for {
		frameLength := GetFrameLength(termBuffer, offset)
		if frameLength <= 0 {
			break
		}
		offset += util.AlignInt32(frameLength, FrameAlignment)
		if offset >= limitOffset {
			break
		}
	}

	gapBeginOffset := offset
	if offset < limitOffset {
		offset += DataFrameHeader.Length
		for offset < limitOffset {
			if GetFrameLength(termBuffer, offset) != 0 {
				break
			}
			offset += DataFrameHeader.Length
		}
		handler(termID, gapBeginOffset, offset-gapBeginOffset)
	}
	return gapBeginOffset
}

// TermGapFill fills a gap with a padding frame if the gap is still entirely zeroed. Returns true if filled.
// Port of io.aeron.logbuffer.TermGapFiller.tryFillGap.
func TermGapFill(meta *LogBufferMetaData, termBuffer *atomic.Buffer, termID int32, gapOffset int32,
	gapLength int32) bool {
	for offset := gapOffset + gapLength - FrameAlignment; offset >= gapOffset; offset -= FrameAlignment {
		if termBuffer.GetInt32(offset) != 0 {
			return false
		}
	}
	writePaddingHeader(meta, termBuffer, gapOffset, termID, gapLength)
	return true
}

// TermUnblockStatus is the result of TermUnblock.
type TermUnblockStatus int

const (
	// TermUnblockNoAction means no unblocking action was taken.
	TermUnblockNoAction TermUnblockStatus = iota
	// TermUnblockUnblocked means a blocked message was replaced with padding.
	TermUnblockUnblocked
	// TermUnblockUnblockedToEnd means padding was written to the end of the term.
	TermUnblockUnblockedToEnd
)

func (s TermUnblockStatus) String() string {
	switch s {
	case TermUnblockUnblocked:
		return "UNBLOCKED"
	case TermUnblockUnblockedToEnd:
		return "UNBLOCKED_TO_END"
	default:
		return "NO_ACTION"
	}
}

// TermUnblock attempts to unblock a log at blockedOffset by a publisher that failed to commit its message.
// Port of io.aeron.logbuffer.TermUnblocker.unblock.
func TermUnblock(meta *LogBufferMetaData, termBuffer *atomic.Buffer, blockedOffset int32, tailOffset int32,
	termID int32) TermUnblockStatus {
	status := TermUnblockNoAction
	frameLength := GetFrameLength(termBuffer, blockedOffset)

	if frameLength < 0 {
		writePaddingHeader(meta, termBuffer, blockedOffset, termID, -frameLength)
		status = TermUnblockUnblocked
	} else if frameLength == 0 {
		currentOffset := blockedOffset + FrameAlignment
		for currentOffset < tailOffset {
			frameLength = GetFrameLength(termBuffer, currentOffset)
			if frameLength != 0 {
				if scanBackToConfirmZeroed(termBuffer, currentOffset, blockedOffset) {
					writePaddingHeader(meta, termBuffer, blockedOffset, termID, currentOffset-blockedOffset)
					status = TermUnblockUnblocked
				}
				break
			}
			currentOffset += FrameAlignment
		}

		if currentOffset == termBuffer.Capacity() {
			if GetFrameLength(termBuffer, blockedOffset) == 0 {
				writePaddingHeader(meta, termBuffer, blockedOffset, termID, currentOffset-blockedOffset)
				status = TermUnblockUnblockedToEnd
			}
		}
	}
	return status
}

// LogBufferUnblock attempts to unblock a log at blockedPosition, rotating the log if required. Returns true if the
// log was unblocked. Port of io.aeron.logbuffer.LogBufferUnblocker.unblock.
func LogBufferUnblock(termBuffers []*atomic.Buffer, meta *LogBufferMetaData, blockedPosition int64,
	termLength int32) bool {
	positionBitsToShift := PositionBitsToShift(termLength)
	blockedTermCount := int32(blockedPosition >> uint(positionBitsToShift))
	blockedOffset := int32(blockedPosition) & (termLength - 1)
	activeTermCount := meta.ActiveTermCount()

	if activeTermCount == blockedTermCount-1 && blockedOffset == 0 {
		currentTermID := TermID(meta.RawTailVolatile(IndexByTermCount(int64(activeTermCount))))
		RotateLog(meta, activeTermCount, currentTermID)
		return true
	}

	blockedIndex := IndexByTermCount(int64(blockedTermCount))
	rawTail := meta.RawTailVolatile(blockedIndex)
	termID := TermID(rawTail)
	tailOffset := TermOffset(rawTail, int64(termLength))

	switch TermUnblock(meta, termBuffers[blockedIndex], blockedOffset, tailOffset, termID) {
	case TermUnblockUnblockedToEnd:
		RotateLog(meta, blockedTermCount, termID)
		return true
	case TermUnblockUnblocked:
		return true
	}
	return false
}

func writePaddingHeader(meta *LogBufferMetaData, termBuffer *atomic.Buffer, termOffset int32, termID int32,
	frameLength int32) {
	meta.ApplyDefaultHeader(termBuffer, termOffset)
	SetFrameType(termBuffer, termOffset, DataFrameHeader.TypePad)
	FrameTermOffset(termBuffer, termOffset, termOffset)
	FrameTermID(termBuffer, termOffset, termID)
	SetFrameLength(termBuffer, termOffset, frameLength)
}

func scanBackToConfirmZeroed(buffer *atomic.Buffer, from int32, limit int32) bool {
	for i := from - FrameAlignment; i >= limit; i -= FrameAlignment {
		if GetFrameLength(buffer, i) != 0 {
			return false
		}
	}
	return true
}
