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

package term

import (
	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/logbuffer"
	"github.com/lirm/aeron-go/aeron/util"
)

// ScanForAvailability scans a term buffer for availability of new message fragments from a given offset, up to
// maxLength bytes, in the same way as Java's TermScanner.scanForAvailability. The result packs the number of
// available bytes and the length of any padding that should be skipped; unpack it with Available and Padding.
//
//go:norace
func ScanForAvailability(termBuffer *atomic.Buffer, offset int32, maxLength int32) int64 {
	limit := termBuffer.Capacity() - offset
	if maxLength < limit {
		limit = maxLength
	}
	var available, padding int32

	for {
		termOffset := offset + available
		frameLength := logbuffer.GetFrameLength(termBuffer, termOffset)
		if frameLength <= 0 {
			break
		}

		alignedFrameLength := util.AlignInt32(frameLength, logbuffer.FrameAlignment)
		if logbuffer.IsPaddingFrame(termBuffer, termOffset) {
			padding = alignedFrameLength - logbuffer.DataFrameHeader.Length
			alignedFrameLength = logbuffer.DataFrameHeader.Length
		}

		available += alignedFrameLength

		if available > limit {
			if alignedFrameLength == available {
				available = -available
			} else {
				available -= alignedFrameLength
			}
			padding = 0
			break
		}

		if padding != 0 || available >= limit {
			break
		}
	}

	return Pack(padding, available)
}

// Pack the values for available and padding into a single int64 for returning on the stack.
func Pack(padding int32, available int32) int64 {
	return int64(padding)<<32 | int64(uint32(available))
}

// Available extracts the number of available bytes from a ScanForAvailability result.
func Available(result int64) int32 {
	return int32(result)
}

// Padding extracts the padding length from a ScanForAvailability result.
func Padding(result int64) int32 {
	return int32(uint64(result) >> 32)
}
