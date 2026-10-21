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

package persistentsubscription

import (
	"testing"

	"github.com/lirm/aeron-go/aeron"
	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/logbuffer"
	"github.com/lirm/aeron-go/aeron/logbuffer/term"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	asmTestTermLength    = int32(64 * 1024)
	asmTestInitialTermID = int32(10)
	asmTestTermID        = int32(12)

	frameAlignment   int32 = 32
	dataHeaderLength int32 = 32
)

type asmTestFrame struct {
	termOffset int32
	length     int32
}

type asmTestTerm struct {
	data   []byte
	buffer *atomic.Buffer
	header *logbuffer.Header
}

func newAsmTestTerm() *asmTestTerm {
	data := make([]byte, asmTestTermLength)
	return &asmTestTerm{data: data, buffer: atomic.MakeBuffer(data, len(data)),
		header: logbuffer.NewHeader(asmTestInitialTermID, 16).SetContext("image")}
}

func (t *asmTestTerm) writeFrame(termOffset int32, flags uint8, sessionID int32, reserved int64, payload []byte) int32 {
	b := t.buffer
	b.PutInt32(termOffset, dataHeaderLength+int32(len(payload)))
	b.PutUInt8(termOffset+logbuffer.DataFrameHeader.FlagsFieldOffset, flags)
	b.PutUInt16(termOffset+logbuffer.DataFrameHeader.TypeFieldOffset, logbuffer.DataFrameHeader.TypeData)
	b.PutInt32(termOffset+logbuffer.DataFrameHeader.TermOffsetFieldOffset, termOffset)
	b.PutInt32(termOffset+logbuffer.DataFrameHeader.SessionIDFieldOffset, sessionID)
	b.PutInt32(termOffset+logbuffer.DataFrameHeader.TermIDFieldOffset, asmTestTermID)
	b.PutInt64(termOffset+logbuffer.DataFrameHeader.ReservedValueFieldOffset, reserved)
	copy(t.data[termOffset+dataHeaderLength:], payload)
	return (termOffset + dataHeaderLength + int32(len(payload)) + frameAlignment - 1) &^ (frameAlignment - 1)
}

// writeMessage fragments msg into frames of at most maxPayload bytes, the reserved value being the term offset.
func (t *asmTestTerm) writeMessage(termOffset int32, msg []byte, maxPayload int) ([]asmTestFrame, int32) {
	var frames []asmTestFrame
	for pos := 0; pos < len(msg); {
		n := len(msg) - pos
		if n > maxPayload {
			n = maxPayload
		}
		var flags uint8
		if pos == 0 {
			flags |= beginFragFlag
		}
		if pos+n == len(msg) {
			flags |= endFragFlag
		}
		frames = append(frames, asmTestFrame{termOffset, int32(n)})
		termOffset = t.writeFrame(termOffset, flags, 5, int64(termOffset), msg[pos:pos+n])
		pos += n
	}
	return frames, termOffset
}

func (t *asmTestTerm) headerAt(termOffset int32) *logbuffer.Header {
	return t.header.Wrap(t.buffer.Ptr(), t.buffer.Capacity()).SetOffset(termOffset)
}

type asmDelivered struct {
	data           []byte
	flags          uint8
	frameLength    int32
	termOffset     int32
	reserved       int64
	position       int64
	context        interface{}
	fragmentLength int32
}

func asmCapture(buffer *atomic.Buffer, offset, length int32, h *logbuffer.Header) asmDelivered {
	return asmDelivered{data: buffer.GetBytesArray(offset, length), flags: h.Flags(), frameLength: h.FrameLength(),
		termOffset: h.TermOffset(), reserved: h.ReservedValue(), position: h.Position(), context: h.Context(),
		fragmentLength: h.FragmentedFrameLength()}
}

func asmPattern(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(i*7 + i>>8)
	}
	return b
}

// asmTestPS returns a PersistentSubscription in replay catch-up whose replay can never reach the live position,
// so fragments delivered through the catch-up handlers go to the message assemblers.
func asmTestPS(t *testing.T, controlled term.ControlledFragmentHandler,
	uncontrolled term.FragmentHandler) *PersistentSubscription {
	ps := newHarness(t, 0, "aeron:udp?endpoint=localhost:0").ps
	ps.nextLivePosition = aeron.NullValue
	ps.controlledFragmentHandler = controlled
	ps.uncontrolledFragmentHandler = uncontrolled
	return ps
}

// Java PersistentSubscription reassembles with ImageControlledFragmentAssembler / ImageFragmentAssembler, so the
// Header for a reassembled message is that of the first fragment completed over the whole message.
func TestAssemblersDeliverCompleteHeaderForReassembledMessage(t *testing.T) {
	for _, controlled := range []bool{true, false} {
		tt := newAsmTestTerm()
		start := int32(1024)
		msg := asmPattern(3000)
		frames, next := tt.writeMessage(start, msg, 1376)
		require.Len(t, frames, 3)

		var got []asmDelivered
		ps := asmTestPS(t, func(b *atomic.Buffer, o, l int32, h *logbuffer.Header) term.ControlledPollAction {
			got = append(got, asmCapture(b, o, l, h))
			return term.ControlledPollActionContinue
		}, func(b *atomic.Buffer, o, l int32, h *logbuffer.Header) {
			got = append(got, asmCapture(b, o, l, h))
		})
		for _, f := range frames {
			if controlled {
				ps.onReplayCatchupFragmentControlled(tt.buffer, f.termOffset+dataHeaderLength, f.length, tt.headerAt(f.termOffset))
			} else {
				ps.onReplayCatchupFragmentUncontrolled(tt.buffer, f.termOffset+dataHeaderLength, f.length, tt.headerAt(f.termOffset))
			}
		}
		require.Len(t, got, 1, "controlled=%v", controlled)
		d := got[0]
		assert.Equal(t, msg, d.data)
		assert.Equal(t, beginFragFlag|endFragFlag, d.flags, "controlled=%v", controlled)
		assert.Equal(t, dataHeaderLength+int32(len(msg)), d.frameLength, "controlled=%v", controlled)
		assert.Equal(t, start, d.termOffset, "controlled=%v", controlled)
		assert.Equal(t, int64(start), d.reserved, "reserved value of the first fragment, controlled=%v", controlled)
		assert.Equal(t, next-start, d.fragmentLength, "controlled=%v", controlled)
		assert.Equal(t, int64(asmTestTermID-asmTestInitialTermID)<<16+int64(next), d.position, "controlled=%v", controlled)
		assert.Equal(t, "image", d.context, "controlled=%v", controlled)
	}
}

// Java's image assemblers leave a partially assembled message untouched when an unfragmented message arrives.
func TestAssemblersKeepPartialMessageAcrossUnfragmentedMessage(t *testing.T) {
	for _, controlled := range []bool{true, false} {
		tt := newAsmTestTerm()
		msg := asmPattern(2000)
		frames, next := tt.writeMessage(0, msg, 1376)
		require.Len(t, frames, 2)
		// An unfragmented frame elsewhere in the buffer, delivered between the two fragments.
		single := next + 4096
		tt.writeFrame(single, beginFragFlag|endFragFlag, 5, 0, []byte("x"))
		deliver := []asmTestFrame{frames[0], {single, 1}, frames[1]}

		var got [][]byte
		ps := asmTestPS(t, func(b *atomic.Buffer, o, l int32, _ *logbuffer.Header) term.ControlledPollAction {
			got = append(got, b.GetBytesArray(o, l))
			return term.ControlledPollActionContinue
		}, func(b *atomic.Buffer, o, l int32, _ *logbuffer.Header) {
			got = append(got, b.GetBytesArray(o, l))
		})
		for _, f := range deliver {
			if controlled {
				ps.onReplayCatchupFragmentControlled(tt.buffer, f.termOffset+dataHeaderLength, f.length, tt.headerAt(f.termOffset))
			} else {
				ps.onReplayCatchupFragmentUncontrolled(tt.buffer, f.termOffset+dataHeaderLength, f.length, tt.headerAt(f.termOffset))
			}
		}
		require.Len(t, got, 2, "controlled=%v", controlled)
		assert.Equal(t, []byte("x"), got[0])
		assert.Equal(t, msg, got[1])
	}
}

// On ABORT of a reassembled message the last fragment is redelivered and the whole message is then delivered.
func TestControlledAssemblerAbortRedeliversReassembledMessage(t *testing.T) {
	tt := newAsmTestTerm()
	msg := asmPattern(3000)
	frames, _ := tt.writeMessage(0, msg, 1376)
	action := term.ControlledPollActionAbort
	var got [][]byte
	ps := asmTestPS(t, func(b *atomic.Buffer, o, l int32, _ *logbuffer.Header) term.ControlledPollAction {
		a := action
		action = term.ControlledPollActionContinue
		if a != term.ControlledPollActionAbort {
			got = append(got, b.GetBytesArray(o, l))
		}
		return a
	}, nil)
	deliver := func(f asmTestFrame) term.ControlledPollAction {
		return ps.onReplayCatchupFragmentControlled(tt.buffer, f.termOffset+dataHeaderLength, f.length, tt.headerAt(f.termOffset))
	}
	deliver(frames[0])
	deliver(frames[1])
	assert.Equal(t, term.ControlledPollActionAbort, deliver(frames[2]))
	assert.Empty(t, got)
	assert.Equal(t, term.ControlledPollActionContinue, deliver(frames[2]))
	require.Len(t, got, 1)
	assert.Equal(t, msg, got[0])
}
