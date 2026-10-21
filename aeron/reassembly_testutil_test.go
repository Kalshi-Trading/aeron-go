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
	"unsafe"

	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/logbuffer"
	"github.com/lirm/aeron-go/aeron/util"
)

const hdrLen = int32(32)

// testTerm is a fake term buffer into which data frames are written as the driver/publisher would.
type testTerm struct {
	data   []byte
	buffer *atomic.Buffer
	header *logbuffer.Header
}

func newTestTerm(length int32, initialTermID int32) *testTerm {
	data := make([]byte, length)
	t := &testTerm{data: data, buffer: atomic.MakeBuffer(data, len(data))}
	t.header = logbuffer.NewHeader(initialTermID, int32(util.NumberOfTrailingZeroes(uint32(length))))
	return t
}

type testFrameFields struct {
	flags     uint8
	sessionID int32
	streamID  int32
	termID    int32
	reserved  int64
}

// writeFrame writes a data frame at termOffset and returns the aligned offset of the next frame.
func (t *testTerm) writeFrame(termOffset int32, f testFrameFields, payload []byte) int32 {
	b := t.buffer
	b.PutInt32(termOffset, hdrLen+int32(len(payload)))
	b.PutUInt8(termOffset+logbuffer.DataFrameHeader.VersionFieldOffset, 0)
	b.PutUInt8(termOffset+logbuffer.DataFrameHeader.FlagsFieldOffset, f.flags)
	b.PutUInt16(termOffset+logbuffer.DataFrameHeader.TypeFieldOffset, logbuffer.DataFrameHeader.TypeData)
	b.PutInt32(termOffset+logbuffer.DataFrameHeader.TermOffsetFieldOffset, termOffset)
	b.PutInt32(termOffset+logbuffer.DataFrameHeader.SessionIDFieldOffset, f.sessionID)
	b.PutInt32(termOffset+logbuffer.DataFrameHeader.StreamIDFieldOffset, f.streamID)
	b.PutInt32(termOffset+logbuffer.DataFrameHeader.TermIDFieldOffset, f.termID)
	b.PutInt64(termOffset+logbuffer.DataFrameHeader.ReservedValueFieldOffset, f.reserved)
	copy(t.data[termOffset+hdrLen:], payload)
	return util.AlignInt32(termOffset+hdrLen+int32(len(payload)), logbuffer.FrameAlignment)
}

type testFrame struct {
	termOffset int32
	length     int32
}

// writeMessage fragments msg into frames of at most maxPayload bytes starting at termOffset, using
// reservedFn to compute each frame's reserved value. Returns the frames and the next term offset.
func (t *testTerm) writeMessage(
	termOffset int32, msg []byte, maxPayload int32, f testFrameFields, reservedFn func(termOffset int32) int64,
) ([]testFrame, int32) {
	var frames []testFrame
	remaining := int32(len(msg))
	pos := int32(0)
	for {
		n := remaining
		if n > maxPayload {
			n = maxPayload
		}
		flags := uint8(0)
		if pos == 0 {
			flags |= beginFrag
		}
		if pos+n == int32(len(msg)) {
			flags |= endFrag
		}
		ff := f
		ff.flags = flags | (f.flags &^ unfragmented)
		if reservedFn != nil {
			ff.reserved = reservedFn(termOffset)
		}
		frames = append(frames, testFrame{termOffset: termOffset, length: n})
		termOffset = t.writeFrame(termOffset, ff, msg[pos:pos+n])
		pos += n
		remaining -= n
		if remaining == 0 {
			return frames, termOffset
		}
	}
}

// headerAt points the shared header at the frame at termOffset, as Image.Poll does.
func (t *testTerm) headerAt(termOffset int32) *logbuffer.Header {
	return t.header.Wrap(t.buffer.Ptr(), t.buffer.Capacity()).SetOffset(termOffset)
}

func (t *testTerm) deliver(handler func(*atomic.Buffer, int32, int32, *logbuffer.Header), frames ...testFrame) {
	for _, fr := range frames {
		handler(t.buffer, fr.termOffset+hdrLen, fr.length, t.headerAt(fr.termOffset))
	}
}

func patternBytes(n int, seed int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(seed*31 + i*7 + (i >> 8))
	}
	return b
}

func headerPtr(h *logbuffer.Header) unsafe.Pointer {
	return h.Buffer().Ptr()
}
