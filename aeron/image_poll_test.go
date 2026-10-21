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
	"path/filepath"
	"testing"
	"unsafe"

	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/logbuffer"
	"github.com/lirm/aeron-go/aeron/logbuffer/term"
	"github.com/lirm/aeron-go/aeron/util"
	"github.com/lirm/aeron-go/aeron/util/memmap"
	"github.com/stretchr/testify/require"
)

const (
	imgTestSessionID     int32 = 77
	imgTestStreamID      int32 = 1001
	imgTestInitialTermID int32 = 5
	imgTestMtu           int32 = 1408
	flagUnfragmented     uint8 = 0xC0
	flagBegin            uint8 = 0x80
)

type testImage struct {
	*image
	term     *atomic.Buffer
	counters *atomic.Buffer
	fileName string
}

func newTestImage(t *testing.T) *testImage {
	termLength := int32(logbuffer.TermMinLength)
	logLength := int(termLength)*logbuffer.PartitionCount + int(logbuffer.LogMetaDataLength)
	fileName := filepath.Join(t.TempDir(), "image.logbuffer")
	mmap, err := memmap.NewFile(fileName, 0, logLength)
	require.NoError(t, err)
	metaBuf := atomic.MakeBuffer(
		unsafe.Pointer(uintptr(mmap.GetMemoryPtr())+uintptr(int(termLength)*logbuffer.PartitionCount)),
		logbuffer.LogMetaDataLength)
	var meta logbuffer.LogBufferMetaData
	meta.Wrap(metaBuf, 0)
	meta.TermLen.Set(termLength)
	meta.InitTermID.Set(imgTestInitialTermID)
	meta.MTULen.Set(imgTestMtu)
	meta.SetPageSize(logbuffer.PageMinSize)
	meta.EndOfStreamPosOff.Set(math.MaxInt64)
	require.NoError(t, mmap.Close())

	counters := atomic.MakeBuffer(make([]byte, 4096))
	img := DefaultImageFactory(imgTestSessionID, 99, fileName, 11, "127.0.0.1:1234", counters, 3).(*image)
	t.Cleanup(func() { _ = img.Close() })
	return &testImage{image: img, term: img.termBuffers[0], counters: counters, fileName: fileName}
}

// appendFrame writes a data frame of payloadLength bytes at offset in term 0 and returns the next aligned offset.
func (ti *testImage) appendFrame(offset int32, payloadLength int32, flags uint8, reserved int64) int32 {
	buf := ti.term
	frameLength := payloadLength + logbuffer.DataFrameHeader.Length
	buf.PutUInt8(offset+logbuffer.DataFrameHeader.VersionFieldOffset, 0)
	buf.PutUInt8(offset+logbuffer.DataFrameHeader.FlagsFieldOffset, flags)
	buf.PutUInt16(offset+logbuffer.DataFrameHeader.TypeFieldOffset, logbuffer.DataFrameHeader.TypeData)
	buf.PutInt32(offset+logbuffer.DataFrameHeader.TermOffsetFieldOffset, offset)
	buf.PutInt32(offset+logbuffer.DataFrameHeader.SessionIDFieldOffset, imgTestSessionID)
	buf.PutInt32(offset+logbuffer.DataFrameHeader.StreamIDFieldOffset, imgTestStreamID)
	buf.PutInt32(offset+logbuffer.DataFrameHeader.TermIDFieldOffset, imgTestInitialTermID)
	buf.PutInt64(offset+logbuffer.DataFrameHeader.ReservedValueFieldOffset, reserved)
	for i := int32(0); i < payloadLength; i++ {
		buf.PutUInt8(offset+logbuffer.DataFrameHeader.Length+i, uint8(i))
	}
	buf.PutInt32Ordered(offset, frameLength)
	return offset + util.AlignInt32(frameLength, logbuffer.FrameAlignment)
}

func (ti *testImage) appendPadding(offset int32, length int32) int32 {
	buf := ti.term
	buf.PutUInt16(offset+logbuffer.DataFrameHeader.TypeFieldOffset, logbuffer.DataFrameHeader.TypePad)
	buf.PutInt32(offset+logbuffer.DataFrameHeader.TermOffsetFieldOffset, offset)
	buf.PutInt32(offset+logbuffer.DataFrameHeader.TermIDFieldOffset, imgTestInitialTermID)
	buf.PutInt32Ordered(offset, length)
	return offset + length
}

func TestImageAccessors(t *testing.T) {
	ti := newTestImage(t)
	require.Equal(t, int64(0), ti.JoinPosition())
	require.Equal(t, int32(3), ti.SubscriberPositionID())
	require.Equal(t, "127.0.0.1:1234", ti.SourceIdentity())
	require.Equal(t, int64(99), ti.CorrelationID())
	require.Equal(t, int64(11), ti.SubscriptionRegistrationID())
	require.Equal(t, imgTestSessionID, ti.SessionID())
	require.Equal(t, int32(logbuffer.TermMinLength), ti.TermBufferLength())
	require.Equal(t, imgTestMtu, ti.MtuLength())
	require.Equal(t, imgTestInitialTermID, ti.InitialTermID())
	require.Equal(t, int32(16), ti.PositionBitsToShift())
	require.Equal(t, int64(math.MaxInt64), ti.EndOfStreamPosition())
	require.False(t, ti.IsEndOfStream())
	require.False(t, ti.IsPublicationRevoked())
	require.False(t, ti.IsClosed())
	require.Nil(t, ti.Subscription())
	require.Error(t, ti.Reject("no subscription"))
}

func TestImagePollAndHeader(t *testing.T) {
	ti := newTestImage(t)
	next := ti.appendFrame(0, 10, flagUnfragmented, 42)
	next = ti.appendFrame(next, 100, flagUnfragmented, 43)
	ti.appendFrame(next, 5, flagUnfragmented, 44)

	var headers []struct {
		offset, termOffset, nextTermOffset, length int32
		position, reserved                         int64
		flags                                      uint8
		context                                    interface{}
	}
	handler := func(buffer *atomic.Buffer, offset int32, length int32, header *logbuffer.Header) {
		require.Equal(t, imgTestSessionID, header.SessionId())
		require.Equal(t, imgTestStreamID, header.StreamId())
		require.Equal(t, imgTestInitialTermID, header.TermId())
		require.Equal(t, imgTestInitialTermID, header.InitialTermId())
		require.Equal(t, logbuffer.DataFrameHeader.TypeData, header.Type())
		require.Equal(t, length+logbuffer.DataFrameHeader.Length, header.FrameLength())
		headers = append(headers, struct {
			offset, termOffset, nextTermOffset, length int32
			position, reserved                         int64
			flags                                      uint8
			context                                    interface{}
		}{offset, header.TermOffset(), header.NextTermOffset(), length, header.Position(), header.ReservedValue(),
			header.Flags(), header.Context()})
	}

	require.Equal(t, 2, ti.Poll(handler, 2))
	require.Equal(t, int64(224), ti.Position())
	require.Len(t, headers, 2)
	require.Equal(t, int32(32), headers[0].offset)
	require.Equal(t, int32(0), headers[0].termOffset)
	require.Equal(t, int32(64), headers[0].nextTermOffset)
	require.Equal(t, int64(64), headers[0].position)
	require.Equal(t, int64(42), headers[0].reserved)
	require.Equal(t, flagUnfragmented, headers[0].flags)
	require.Same(t, ti.image, headers[0].context)
	require.Equal(t, int32(64), headers[1].termOffset)
	require.Equal(t, int32(224), headers[1].nextTermOffset)
	require.Equal(t, int64(224), headers[1].position)
	require.Equal(t, int64(43), headers[1].reserved)

	require.Equal(t, 1, ti.Poll(handler, 10))
	require.Equal(t, int64(288), ti.Position())
	require.Equal(t, 0, ti.Poll(handler, 10))
}

func TestImagePollSkipsPadding(t *testing.T) {
	ti := newTestImage(t)
	next := ti.appendFrame(0, 10, flagUnfragmented, 0)
	ti.appendPadding(next, ti.TermBufferLength()-next)
	count := 0
	handler := func(*atomic.Buffer, int32, int32, *logbuffer.Header) { count++ }
	require.Equal(t, 1, ti.Poll(handler, 10))
	require.Equal(t, int64(ti.TermBufferLength()), ti.Position())
}

func TestImageControlledPollActions(t *testing.T) {
	ti := newTestImage(t)
	offset := int32(0)
	for i := 0; i < 4; i++ {
		offset = ti.appendFrame(offset, 16, flagUnfragmented, int64(i))
	}
	frame := int64(64)

	actions := []term.ControlledPollAction{term.ControlledPollActionContinue, term.ControlledPollActionAbort}
	n := 0
	handler := func(_ *atomic.Buffer, _ int32, _ int32, _ *logbuffer.Header) term.ControlledPollAction {
		a := actions[n%len(actions)]
		n++
		return a
	}
	require.Equal(t, 1, ti.ControlledPoll(handler, 10), "ABORT is not counted and not consumed")
	require.Equal(t, frame, ti.Position())

	n = 0
	actions = []term.ControlledPollAction{term.ControlledPollActionBreak}
	require.Equal(t, 1, ti.ControlledPoll(handler, 10), "BREAK consumes the fragment then stops")
	require.Equal(t, 2*frame, ti.Position())

	var positionsSeen []int64
	n = 0
	actions = []term.ControlledPollAction{term.ControlledPollActionCommit}
	commitHandler := func(b *atomic.Buffer, o int32, l int32, h *logbuffer.Header) term.ControlledPollAction {
		positionsSeen = append(positionsSeen, ti.Position())
		return handler(b, o, l, h)
	}
	require.Equal(t, 2, ti.ControlledPoll(commitHandler, 10))
	require.Equal(t, []int64{2 * frame, 3 * frame}, positionsSeen, "COMMIT publishes position immediately")
	require.Equal(t, 4*frame, ti.Position())
}

func TestImageBoundedPolls(t *testing.T) {
	ti := newTestImage(t)
	offset := int32(0)
	for i := 0; i < 4; i++ {
		offset = ti.appendFrame(offset, 16, flagUnfragmented, int64(i))
	}
	count := 0
	handler := func(*atomic.Buffer, int32, int32, *logbuffer.Header) { count++ }
	require.Equal(t, 1, ti.BoundedPoll(handler, 64, 10))
	require.Equal(t, int64(64), ti.Position())
	require.Equal(t, 0, ti.BoundedPoll(handler, 0, 10), "limit behind position")

	controlled := func(*atomic.Buffer, int32, int32, *logbuffer.Header) term.ControlledPollAction {
		return term.ControlledPollActionContinue
	}
	require.Equal(t, 2, ti.BoundedControlledPoll(controlled, 128+10, 10), "frames starting before the limit")
	require.Equal(t, int64(192), ti.Position())

	// MaxInt64 limit used to overflow int32 and read nothing.
	require.Equal(t, 1, ti.BoundedPoll(handler, math.MaxInt64, 10))
	require.Equal(t, int64(256), ti.Position())
}

func TestImageControlledPeekAndSetPosition(t *testing.T) {
	ti := newTestImage(t)
	next := ti.appendFrame(0, 16, flagUnfragmented, 0)
	next = ti.appendFrame(next, 16, flagBegin, 0)
	ti.appendFrame(next, 16, 0x40, 0)

	count := 0
	handler := func(*atomic.Buffer, int32, int32, *logbuffer.Header) term.ControlledPollAction {
		count++
		return term.ControlledPollActionContinue
	}
	pos, err := ti.ControlledPeek(0, handler, 128)
	require.NoError(t, err)
	require.Equal(t, 2, count)
	require.Equal(t, int64(64), pos, "BEGIN fragment without END is not a complete message")
	require.Equal(t, int64(0), ti.Position(), "peek does not consume")

	count = 0
	pos, err = ti.ControlledPeek(0, handler, math.MaxInt64)
	require.NoError(t, err)
	require.Equal(t, 3, count)
	require.Equal(t, int64(192), pos)

	_, err = ti.ControlledPeek(3, handler, 192)
	require.Error(t, err)

	require.NoError(t, ti.SetPosition(pos))
	require.Equal(t, int64(192), ti.Position())
	require.Error(t, ti.SetPosition(0), "behind current position")
	require.Error(t, ti.SetPosition(200), "not aligned")
	require.Error(t, ti.SetPosition(int64(ti.TermBufferLength())+32), "beyond current term")
	require.NoError(t, ti.SetPosition(int64(ti.TermBufferLength())))
}

func TestImageBlockAndRawPoll(t *testing.T) {
	ti := newTestImage(t)
	next := ti.appendFrame(0, 16, flagUnfragmented, 0)
	next = ti.appendFrame(next, 100, flagUnfragmented, 0)
	ti.appendFrame(next, 16, flagUnfragmented, 0)

	var gotOffset, gotLength, gotSession, gotTerm int32
	block := func(_ *atomic.Buffer, offset int32, length int32, sessionID int32, termID int32) {
		gotOffset, gotLength, gotSession, gotTerm = offset, length, sessionID, termID
	}
	require.Equal(t, int32(64), ti.BlockPoll(block, 100), "only whole frames within the block limit")
	require.Equal(t, []int32{0, 64, imgTestSessionID, imgTestInitialTermID}, []int32{gotOffset, gotLength, gotSession, gotTerm})
	require.Equal(t, int64(64), ti.Position())

	var gotFile string
	var gotFileOffset int64
	raw := func(file string, fileOffset int64, _ *atomic.Buffer, termOffset int32, length int32, _ int32, _ int32) {
		gotFile, gotFileOffset, gotOffset, gotLength = file, fileOffset, termOffset, length
	}
	require.Equal(t, int32(224), ti.RawPoll(raw, 1<<20))
	require.Equal(t, ti.fileName, gotFile)
	require.Equal(t, int64(64), gotFileOffset)
	require.Equal(t, int32(64), gotOffset)
	require.Equal(t, int64(288), ti.Position())
	require.Equal(t, int32(0), ti.BlockPoll(block, 100))
}

func TestImageCloseCapturesState(t *testing.T) {
	ti := newTestImage(t)
	ti.appendFrame(0, 16, flagUnfragmented, 0)
	ti.logBuffers.Meta().EndOfStreamPosOff.Set(64)
	handler := func(*atomic.Buffer, int32, int32, *logbuffer.Header) {}
	require.False(t, ti.IsEndOfStream())
	require.Equal(t, 1, ti.Poll(handler, 10))
	require.True(t, ti.IsEndOfStream())
	require.Equal(t, int64(64), ti.EndOfStreamPosition())

	ti.markClosed()
	require.True(t, ti.IsClosed())
	require.True(t, ti.IsEndOfStream())
	require.Equal(t, int64(64), ti.Position())
	require.Equal(t, int64(64), ti.EndOfStreamPosition())
	require.Equal(t, int32(0), ti.ActiveTransportCount())
	require.Equal(t, 0, ti.Poll(handler, 10))
	require.ErrorIs(t, ti.SetPosition(64), ErrImageClosed)
	require.NoError(t, ti.Close())
	require.NoError(t, ti.Close())
}

func TestImagePollsDoNotAllocate(t *testing.T) {
	ti := newTestImage(t)
	offset := int32(0)
	for offset < ti.TermBufferLength()-1024 {
		offset = ti.appendFrame(offset, 64, flagUnfragmented, 0)
	}
	var sum int64
	handler := func(_ *atomic.Buffer, _ int32, length int32, header *logbuffer.Header) {
		sum += int64(length) + header.Position()
	}
	controlled := func(_ *atomic.Buffer, _ int32, length int32, _ *logbuffer.Header) term.ControlledPollAction {
		sum += int64(length)
		return term.ControlledPollActionCommit
	}
	block := func(_ *atomic.Buffer, _ int32, length int32, _ int32, _ int32) { sum += int64(length) }
	reset := func() { ti.subscriberPosition.setRelease(0) }

	require.Zero(t, testing.AllocsPerRun(100, func() { reset(); ti.Poll(handler, 10) }))
	require.Zero(t, testing.AllocsPerRun(100, func() { reset(); ti.BoundedPoll(handler, 640, 10) }))
	require.Zero(t, testing.AllocsPerRun(100, func() { reset(); ti.ControlledPoll(controlled, 10) }))
	require.Zero(t, testing.AllocsPerRun(100, func() { reset(); ti.BoundedControlledPoll(controlled, 640, 10) }))
	require.Zero(t, testing.AllocsPerRun(100, func() { reset(); _, _ = ti.ControlledPeek(0, controlled, 640) }))
	require.Zero(t, testing.AllocsPerRun(100, func() { reset(); ti.BlockPoll(block, 4096) }))
	require.NotZero(t, sum)
}
