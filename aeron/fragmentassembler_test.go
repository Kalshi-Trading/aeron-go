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
	"math"
	"sync"
	"testing"

	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/logbuffer"
	"github.com/lirm/aeron-go/aeron/logbuffer/term"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	testSessionID     = int32(777)
	testStreamID      = int32(1001)
	testInitialTermID = int32(3)
	testTermLength    = int32(64 * 1024)
)

type delivered struct {
	data           []byte
	offset         int32
	length         int32
	buffer         *atomic.Buffer
	header         *logbuffer.Header
	flags          uint8
	frameLength    int32
	termOffset     int32
	sessionID      int32
	reserved       int64
	position       int64
	fragmentedLen  int32
	context        interface{}
	headerBufferAt int32
}

func capture(buffer *atomic.Buffer, offset, length int32, header *logbuffer.Header) delivered {
	return delivered{
		data: buffer.GetBytesArray(offset, length), offset: offset, length: length, buffer: buffer, header: header,
		flags: header.Flags(), frameLength: header.FrameLength(), termOffset: header.TermOffset(),
		sessionID: header.SessionId(), reserved: header.ReservedValue(), position: header.Position(),
		fragmentedLen: header.FragmentedFrameLength(), context: header.Context(), headerBufferAt: header.Offset(),
	}
}

// assemblerUnderTest adapts the four assembler variants to a common shape.
type assemblerUnderTest struct {
	name        string
	perSession  bool
	controlled  bool
	onFragment  func(*atomic.Buffer, int32, int32, *logbuffer.Header) term.ControlledPollAction
	got         *[]delivered
	nextAction  *term.ControlledPollAction
	assembling  func(sessionID int32) bool
	freeSession func(sessionID int32) bool
	unavailable func(Image)
}

func allAssemblers() []func() *assemblerUnderTest {
	mk := func(name string, perSession, controlled bool) func() *assemblerUnderTest {
		return func() *assemblerUnderTest {
			got := &[]delivered{}
			next := new(term.ControlledPollAction)
			*next = term.ControlledPollActionContinue
			a := &assemblerUnderTest{name: name, perSession: perSession, controlled: controlled, got: got, nextAction: next}
			fh := func(buffer *atomic.Buffer, offset, length int32, header *logbuffer.Header) {
				*got = append(*got, capture(buffer, offset, length, header))
			}
			ch := func(buffer *atomic.Buffer, offset, length int32, header *logbuffer.Header) term.ControlledPollAction {
				action := *next
				*next = term.ControlledPollActionContinue
				if action != term.ControlledPollActionAbort {
					*got = append(*got, capture(buffer, offset, length, header))
				}
				return action
			}
			switch {
			case perSession && !controlled:
				fa := NewFragmentAssembler(fh, 0)
				a.onFragment = func(b *atomic.Buffer, o, l int32, h *logbuffer.Header) term.ControlledPollAction {
					fa.OnFragment(b, o, l, h)
					return term.ControlledPollActionContinue
				}
				a.assembling, a.freeSession, a.unavailable = fa.IsSessionAssembling, fa.FreeSessionBuffer, fa.OnUnavailableImage
			case perSession && controlled:
				fa := NewControlledFragmentAssembler(ch, 0)
				a.onFragment = fa.OnFragment
				a.assembling, a.freeSession, a.unavailable = fa.IsSessionAssembling, fa.FreeSessionBuffer, fa.OnUnavailableImage
			case !controlled:
				fa := NewImageFragmentAssembler(fh, 0)
				a.onFragment = func(b *atomic.Buffer, o, l int32, h *logbuffer.Header) term.ControlledPollAction {
					fa.OnFragment(b, o, l, h)
					return term.ControlledPollActionContinue
				}
				a.assembling = func(int32) bool { return fa.IsAssembling() }
			default:
				fa := NewImageControlledFragmentAssembler(ch, 0)
				a.onFragment = fa.OnFragment
				a.assembling = func(int32) bool { return fa.IsAssembling() }
			}
			return a
		}
	}
	return []func() *assemblerUnderTest{
		mk("FragmentAssembler", true, false),
		mk("ControlledFragmentAssembler", true, true),
		mk("ImageFragmentAssembler", false, false),
		mk("ImageControlledFragmentAssembler", false, true),
	}
}

func (a *assemblerUnderTest) deliver(tt *testTerm, frames ...testFrame) []term.ControlledPollAction {
	var actions []term.ControlledPollAction
	for _, fr := range frames {
		actions = append(actions, a.onFragment(tt.buffer, fr.termOffset+hdrLen, fr.length, tt.headerAt(fr.termOffset)))
	}
	return actions
}

func fields(sessionID int32) testFrameFields {
	return testFrameFields{sessionID: sessionID, streamID: testStreamID, termID: testInitialTermID + 2}
}

func forEachAssembler(t *testing.T, fn func(t *testing.T, a *assemblerUnderTest)) {
	for _, mk := range allAssemblers() {
		a := mk()
		t.Run(a.name, func(t *testing.T) { fn(t, a) })
	}
}

func TestAssemblerPassesThroughUnfragmentedMessage(t *testing.T) {
	forEachAssembler(t, func(t *testing.T, a *assemblerUnderTest) {
		tt := newTestTerm(testTermLength, testInitialTermID)
		f := fields(testSessionID)
		f.flags = unfragmented
		f.reserved = 42
		msg := patternBytes(32, 1)
		tt.writeFrame(64, f, msg)
		a.deliver(tt, testFrame{termOffset: 64, length: 32})

		require.Len(t, *a.got, 1)
		d := (*a.got)[0]
		assert.Same(t, tt.buffer, d.buffer)
		assert.Same(t, tt.header, d.header)
		assert.EqualValues(t, 64+hdrLen, d.offset)
		assert.Equal(t, msg, d.data)
		assert.Equal(t, unfragmented, d.flags)
		assert.EqualValues(t, 42, d.reserved)
		assert.EqualValues(t, NullValue, d.fragmentedLen)
		assert.False(t, a.assembling(testSessionID))
	})
}

func TestAssemblerAssemblesTwoPartMessage(t *testing.T) {
	forEachAssembler(t, func(t *testing.T, a *assemblerUnderTest) {
		tt := newTestTerm(testTermLength, testInitialTermID)
		msg := patternBytes(1024, 2)
		frames, next := tt.writeMessage(0, msg, 512, fields(testSessionID), nil)
		require.Len(t, frames, 2)

		a.deliver(tt, frames[0])
		assert.Empty(t, *a.got)
		assert.True(t, a.assembling(testSessionID))
		a.deliver(tt, frames[1])
		assert.False(t, a.assembling(testSessionID))

		require.Len(t, *a.got, 1)
		d := (*a.got)[0]
		assert.EqualValues(t, 0, d.offset)
		assert.EqualValues(t, 1024, d.length)
		assert.Equal(t, msg, d.data)
		assert.Equal(t, testSessionID, d.sessionID)
		assert.Equal(t, unfragmented, d.flags)
		assert.Equal(t, hdrLen+1024, d.frameLength)
		assert.EqualValues(t, 0, d.termOffset)
		assert.NotSame(t, tt.header, d.header)
		assert.Equal(t, logbuffer.ComputeFragmentedFrameLength(1024, 512), d.fragmentedLen)
		assert.Equal(t, int64(2)*int64(testTermLength)+int64(next), d.position)
	})
}

func TestAssemblerAssemblesFourPartMessage(t *testing.T) {
	forEachAssembler(t, func(t *testing.T, a *assemblerUnderTest) {
		tt := newTestTerm(testTermLength, testInitialTermID)
		msg := patternBytes(1024, 3)
		f := fields(testSessionID)
		f.flags = 0x3
		frames, _ := tt.writeMessage(0, msg, 256, f, nil)
		require.Len(t, frames, 4)
		// only the last fragment carries the extra flag bits, as in Java's FragmentAssemblerTest
		for _, fr := range frames[:3] {
			flagsAt := fr.termOffset + logbuffer.DataFrameHeader.FlagsFieldOffset
			tt.buffer.PutUInt8(flagsAt, tt.buffer.GetUInt8(flagsAt)&^0x3)
		}
		a.deliver(tt, frames...)

		require.Len(t, *a.got, 1)
		d := (*a.got)[0]
		assert.EqualValues(t, 1024, d.length)
		assert.Equal(t, msg, d.data)
		assert.Equal(t, testSessionID, d.sessionID)
		assert.Equal(t, unfragmented|0x3, d.flags)
	})
}

func TestAssemblerDeliversHeaderOfFirstFragment(t *testing.T) {
	forEachAssembler(t, func(t *testing.T, a *assemblerUnderTest) {
		tt := newTestTerm(testTermLength, testInitialTermID)
		tt.header.SetContext("ctx")
		start := int32(4096)
		msg := patternBytes(5000, 4)
		frames, next := tt.writeMessage(start, msg, 1376, fields(testSessionID),
			func(termOffset int32) int64 { return int64(termOffset)<<8 | 0x7 })
		require.Len(t, frames, 4)
		a.deliver(tt, frames...)

		require.Len(t, *a.got, 1)
		d := (*a.got)[0]
		assert.Equal(t, msg, d.data)
		assert.Equal(t, start, d.termOffset)
		assert.Equal(t, int64(start)<<8|0x7, d.reserved, "reserved value must be that of the first fragment")
		assert.Equal(t, hdrLen+5000, d.frameLength)
		assert.Equal(t, int64(2)*int64(testTermLength)+int64(next), d.position, "position must be after last fragment")
		assert.Equal(t, next-start, d.fragmentedLen)
		assert.Equal(t, "ctx", d.context)
		assert.EqualValues(t, 0, d.headerBufferAt)
	})
}

func TestAssemblerDoesNothingIfEndArrivesWithoutBegin(t *testing.T) {
	forEachAssembler(t, func(t *testing.T, a *assemblerUnderTest) {
		tt := newTestTerm(testTermLength, testInitialTermID)
		frames, _ := tt.writeMessage(0, patternBytes(1024, 5), 512, fields(testSessionID), nil)
		a.deliver(tt, frames[1])
		assert.Empty(t, *a.got)
	})
}

func TestAssemblerDoesNothingIfMidArrivesWithoutBegin(t *testing.T) {
	forEachAssembler(t, func(t *testing.T, a *assemblerUnderTest) {
		tt := newTestTerm(testTermLength, testInitialTermID)
		frames, _ := tt.writeMessage(0, patternBytes(1536, 5), 512, fields(testSessionID), nil)
		a.deliver(tt, frames[1], frames[2])
		assert.Empty(t, *a.got)
	})
}

func TestAssemblerSkipsOverMessagesWithLoss(t *testing.T) {
	forEachAssembler(t, func(t *testing.T, a *assemblerUnderTest) {
		tt := newTestTerm(testTermLength, testInitialTermID)
		lossy, next := tt.writeMessage(0, patternBytes(768, 6), 256, fields(testSessionID), nil)
		whole := patternBytes(600, 7)
		good, next := tt.writeMessage(next, whole, 256, fields(testSessionID), nil)
		f := fields(testSessionID)
		f.flags = unfragmented
		tt.writeFrame(next, f, []byte("tail"))

		// middle fragment lost
		a.deliver(tt, lossy[0], lossy[2])
		assert.Empty(t, *a.got)
		assert.False(t, a.assembling(testSessionID))
		a.deliver(tt, good...)
		a.deliver(tt, testFrame{termOffset: next, length: 4})

		require.Len(t, *a.got, 2)
		assert.Equal(t, whole, (*a.got)[0].data)
		assert.Equal(t, []byte("tail"), (*a.got)[1].data)
		assert.Equal(t, unfragmented, (*a.got)[1].flags)
	})
}

func TestAssemblerDiscardsPartialMessageOnNewBegin(t *testing.T) {
	forEachAssembler(t, func(t *testing.T, a *assemblerUnderTest) {
		tt := newTestTerm(testTermLength, testInitialTermID)
		first, next := tt.writeMessage(0, patternBytes(1024, 8), 512, fields(testSessionID), nil)
		msg := patternBytes(1000, 9)
		second, _ := tt.writeMessage(next, msg, 512, fields(testSessionID), nil)
		a.deliver(tt, first[0])
		a.deliver(tt, second...)
		require.Len(t, *a.got, 1)
		assert.Equal(t, msg, (*a.got)[0].data)
	})
}

func TestAssemblerReusesBuilderAcrossMessages(t *testing.T) {
	forEachAssembler(t, func(t *testing.T, a *assemblerUnderTest) {
		tt := newTestTerm(testTermLength, testInitialTermID)
		next := int32(0)
		var msgs [][]byte
		for i, n := range []int{20000, 3000, 1377, 1376 * 2, 9} {
			msg := patternBytes(n, i)
			msgs = append(msgs, msg)
			var frames []testFrame
			frames, next = tt.writeMessage(next, msg, 1376, fields(testSessionID), nil)
			a.deliver(tt, frames...)
		}
		require.Len(t, *a.got, len(msgs))
		for i := range msgs {
			assert.Equal(t, msgs[i], (*a.got)[i].data, "message %d", i)
		}
	})
}

func TestPerSessionAssemblersInterleaveSessions(t *testing.T) {
	forEachAssembler(t, func(t *testing.T, a *assemblerUnderTest) {
		if !a.perSession {
			t.Skip("image assemblers handle a single session")
		}
		tt := newTestTerm(testTermLength, testInitialTermID)
		msgA, msgB := patternBytes(3000, 10), patternBytes(2000, 11)
		framesA, next := tt.writeMessage(0, msgA, 1376, fields(1), nil)
		framesB, _ := tt.writeMessage(next, msgB, 1376, fields(2), nil)
		// each session's frames are contiguous in its own image; termOffset bookkeeping is per session
		tt2 := newTestTerm(testTermLength, testInitialTermID)
		framesB2, _ := tt2.writeMessage(0, msgB, 1376, fields(2), nil)
		_ = framesB

		a.deliver(tt, framesA[0])
		a.onFragment(tt2.buffer, framesB2[0].termOffset+hdrLen, framesB2[0].length, tt2.headerAt(framesB2[0].termOffset))
		assert.True(t, a.assembling(1))
		assert.True(t, a.assembling(2))
		a.deliver(tt, framesA[1:]...)
		a.onFragment(tt2.buffer, framesB2[1].termOffset+hdrLen, framesB2[1].length, tt2.headerAt(framesB2[1].termOffset))

		require.Len(t, *a.got, 2)
		assert.Equal(t, msgA, (*a.got)[0].data)
		assert.EqualValues(t, 1, (*a.got)[0].sessionID)
		assert.Equal(t, msgB, (*a.got)[1].data)
		assert.EqualValues(t, 2, (*a.got)[1].sessionID)
	})
}

func TestPerSessionAssemblerFreesSessionBuffer(t *testing.T) {
	forEachAssembler(t, func(t *testing.T, a *assemblerUnderTest) {
		if !a.perSession {
			t.Skip("image assemblers have a single buffer")
		}
		tt := newTestTerm(testTermLength, testInitialTermID)
		frames, _ := tt.writeMessage(0, patternBytes(1024, 12), 512, fields(testSessionID), nil)
		assert.False(t, a.freeSession(testSessionID))
		a.deliver(tt, frames...)
		require.Len(t, *a.got, 1)
		assert.True(t, a.freeSession(testSessionID))
		assert.False(t, a.freeSession(testSessionID))
	})
}

func TestPerSessionAssemblerFreesBufferOnUnavailableImage(t *testing.T) {
	forEachAssembler(t, func(t *testing.T, a *assemblerUnderTest) {
		if !a.perSession {
			t.Skip("image assemblers have a single buffer")
		}
		tt := newTestTerm(testTermLength, testInitialTermID)
		frames, _ := tt.writeMessage(0, patternBytes(1024, 13), 512, fields(testSessionID), nil)
		a.deliver(tt, frames[0])
		assert.True(t, a.assembling(testSessionID))

		image := new(MockImage)
		image.On("SessionID").Return(testSessionID)
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			a.unavailable(image)
		}()
		wg.Wait()

		// the free is applied on the polling goroutine; the end fragment no longer has a builder
		a.deliver(tt, frames[1])
		assert.Empty(t, *a.got)
		assert.False(t, a.assembling(testSessionID))
		assert.False(t, a.freeSession(testSessionID))
	})
}

func TestPerSessionAssemblerUnavailableImageRace(t *testing.T) {
	tt := newTestTerm(testTermLength, testInitialTermID)
	frames, _ := tt.writeMessage(0, patternBytes(4000, 14), 1376, fields(testSessionID), nil)
	count := 0
	fa := NewFragmentAssembler(func(*atomic.Buffer, int32, int32, *logbuffer.Header) { count++ }, 0)
	image := new(MockImage)
	image.On("SessionID").Return(int32(99))
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 1000; i++ {
			fa.OnUnavailableImage(image)
		}
	}()
	for i := 0; i < 1000; i++ {
		tt.deliver(fa.OnFragment, frames...)
	}
	<-done
	assert.Equal(t, 1000, count)
}

func TestControlledAssemblersAbortRedeliversLastFragment(t *testing.T) {
	forEachAssembler(t, func(t *testing.T, a *assemblerUnderTest) {
		if !a.controlled {
			t.Skip("not controlled")
		}
		tt := newTestTerm(testTermLength, testInitialTermID)
		msg := patternBytes(3000, 15)
		frames, next := tt.writeMessage(0, msg, 1376, fields(testSessionID),
			func(termOffset int32) int64 { return int64(termOffset) + 1 })
		require.Len(t, frames, 3)
		a.deliver(tt, frames[:2]...)

		*a.nextAction = term.ControlledPollActionAbort
		actions := a.deliver(tt, frames[2])
		assert.Equal(t, []term.ControlledPollAction{term.ControlledPollActionAbort}, actions)
		assert.Empty(t, *a.got)
		assert.True(t, a.assembling(testSessionID))

		*a.nextAction = term.ControlledPollActionBreak
		actions = a.deliver(tt, frames[2])
		assert.Equal(t, []term.ControlledPollAction{term.ControlledPollActionBreak}, actions)
		require.Len(t, *a.got, 1)
		d := (*a.got)[0]
		assert.Equal(t, msg, d.data)
		assert.Equal(t, hdrLen+3000, d.frameLength)
		assert.Equal(t, unfragmented, d.flags)
		assert.EqualValues(t, 1, d.reserved)
		assert.Equal(t, int64(2)*int64(testTermLength)+int64(next), d.position)
		assert.False(t, a.assembling(testSessionID))
	})
}

func TestControlledAssemblersReturnDelegateActionForUnfragmented(t *testing.T) {
	forEachAssembler(t, func(t *testing.T, a *assemblerUnderTest) {
		if !a.controlled {
			t.Skip("not controlled")
		}
		tt := newTestTerm(testTermLength, testInitialTermID)
		f := fields(testSessionID)
		f.flags = unfragmented
		tt.writeFrame(0, f, []byte("x"))
		*a.nextAction = term.ControlledPollActionCommit
		assert.Equal(t, []term.ControlledPollAction{term.ControlledPollActionCommit},
			a.deliver(tt, testFrame{termOffset: 0, length: 1}))
	})
}

func TestFragmentAssemblerClear(t *testing.T) {
	tt := newTestTerm(testTermLength, testInitialTermID)
	frames, _ := tt.writeMessage(0, patternBytes(1024, 16), 512, fields(testSessionID), nil)
	fa := NewFragmentAssembler(func(*atomic.Buffer, int32, int32, *logbuffer.Header) { t.Fatal("unexpected") }, 128)
	tt.deliver(fa.OnFragment, frames[0])
	assert.True(t, fa.IsSessionAssembling(testSessionID))
	fa.Clear()
	assert.False(t, fa.IsSessionAssembling(testSessionID))
	tt.deliver(fa.OnFragment, frames[1])

	cfa := NewControlledFragmentAssembler(func(*atomic.Buffer, int32, int32, *logbuffer.Header) term.ControlledPollAction {
		t.Fatal("unexpected")
		return term.ControlledPollActionContinue
	}, -5)
	tt.deliver(func(b *atomic.Buffer, o, l int32, h *logbuffer.Header) { cfa.OnFragment(b, o, l, h) }, frames[0])
	assert.True(t, cfa.IsSessionAssembling(testSessionID))
	cfa.Clear()
	assert.False(t, cfa.IsSessionAssembling(testSessionID))
}

func TestImageAssemblersExposeBufferBuilder(t *testing.T) {
	fa := NewImageFragmentAssembler(nil, 1024)
	assert.EqualValues(t, 1024, fa.BufferBuilder().Capacity())
	cfa := NewImageControlledFragmentAssembler(nil, 0)
	assert.EqualValues(t, 0, cfa.BufferBuilder().Capacity())
	assert.Nil(t, cfa.Delegate())
}

func TestAssemblersToleratesInitialBufferLengthAboveMaxCapacity(t *testing.T) {
	for _, initial := range []int32{BufferBuilderMaxCapacity + 1, math.MaxInt32, -1} {
		tt := newTestTerm(testTermLength, testInitialTermID)
		msg := patternBytes(3000, 21)
		frames, _ := tt.writeMessage(0, msg, 1376, fields(testSessionID), nil)
		var got [][]byte
		fh := func(b *atomic.Buffer, o, l int32, _ *logbuffer.Header) { got = append(got, b.GetBytesArray(o, l)) }
		ch := func(b *atomic.Buffer, o, l int32, h *logbuffer.Header) term.ControlledPollAction {
			fh(b, o, l, h)
			return term.ControlledPollActionContinue
		}
		fa := NewFragmentAssembler(fh, initial)
		cfa := NewControlledFragmentAssembler(ch, initial)
		ifa := NewImageFragmentAssembler(fh, initial)
		icfa := NewImageControlledFragmentAssembler(ch, initial)
		require.NotNil(t, ifa.BufferBuilder())
		require.NotNil(t, icfa.BufferBuilder())
		assert.EqualValues(t, 0, ifa.BufferBuilder().Capacity())
		for _, h := range []func(*atomic.Buffer, int32, int32, *logbuffer.Header){
			fa.OnFragment, ifa.OnFragment,
			func(b *atomic.Buffer, o, l int32, hd *logbuffer.Header) { cfa.OnFragment(b, o, l, hd) },
			func(b *atomic.Buffer, o, l int32, hd *logbuffer.Header) { icfa.OnFragment(b, o, l, hd) },
		} {
			tt.deliver(h, frames...)
		}
		require.Len(t, got, 4, "initial=%d", initial)
		for _, g := range got {
			assert.Equal(t, msg, g)
		}
	}
}
