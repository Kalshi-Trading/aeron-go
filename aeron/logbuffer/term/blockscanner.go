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

// ScanBlock scans a term buffer for a block of complete message fragments from termOffset up to limitOffset, in
// the same way as Java's TermBlockScanner.scan. A padding frame terminates the block, and is only included when it
// is the first frame scanned. Returns the resulting offset at the end of the block of whole fragments.
//
//go:norace
func ScanBlock(termBuffer *atomic.Buffer, termOffset int32, limitOffset int32) int32 {
	offset := termOffset
	for offset < limitOffset {
		frameLength := logbuffer.GetFrameLength(termBuffer, offset)
		if frameLength <= 0 {
			break
		}

		alignedFrameLength := util.AlignInt32(frameLength, logbuffer.FrameAlignment)
		if logbuffer.IsPaddingFrame(termBuffer, offset) {
			if termOffset == offset {
				offset += alignedFrameLength
			}
			break
		}

		if offset+alignedFrameLength > limitOffset {
			break
		}
		offset += alignedFrameLength
	}
	return offset
}
