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
	"fmt"
	"math"
	"math/rand"
	"testing"

	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/logbuffer"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func mustBufferBuilder(t *testing.T, capacity int32) *BufferBuilder {
	b, err := NewBufferBuilder(capacity)
	require.NoError(t, err)
	return b
}

func TestBufferBuilderFindSuitableCapacity(t *testing.T) {
	cases := []struct {
		capacity int32
		required int64
		expected int32
	}{
		{0, 0, 4096}, {4095, 1, 4096}, {4096, 7, 4096}, {100, 200, 4096}, {101, 5000, 6144},
		{5000, 8080, 11250}, {6666, 11111, 14998}, {0, 2147483639, 2147483639},
		{2048, 2147483647, 2147483639}, {5, 1000000000000, 2147483639},
	}
	for _, c := range cases {
		assert.Equal(t, c.expected, findSuitableCapacity(c.capacity, c.required), "%+v", c)
	}
}

func TestBufferBuilderInitialisesToDefaultValues(t *testing.T) {
	b := mustBufferBuilder(t, 0)
	assert.EqualValues(t, 0, b.Capacity())
	assert.EqualValues(t, 0, b.Limit())
	assert.EqualValues(t, NullValue, b.NextTermOffset())
	assert.EqualValues(t, 0, b.Buffer().Capacity())
	assert.Empty(t, b.Bytes())
	assert.EqualValues(t, hdrLen, b.headerBuffer.Capacity())
	assert.NotEqual(t, b.headerBuffer.Ptr(), b.Buffer().Ptr())
}

func TestBufferBuilderRejectsInitialCapacityOutOfRange(t *testing.T) {
	for _, c := range []int32{-1, math.MaxInt32, math.MaxInt32 - 7} {
		_, err := NewBufferBuilder(c)
		require.Error(t, err)
		assert.Equal(t, fmt.Sprintf("initialCapacity outside range 0 - 2147483639: initialCapacity=%d", c), err.Error())
	}
}

func TestBufferBuilderRejectsLimitOutOfRange(t *testing.T) {
	b := mustBufferBuilder(t, 0)
	for _, l := range []int32{-1, 65, 1000000} {
		err := b.SetLimit(l)
		require.Error(t, err)
		assert.Equal(t, fmt.Sprintf("limit outside range: capacity=0 limit=%d", l), err.Error())
	}
	b = mustBufferBuilder(t, 64)
	require.NoError(t, b.SetLimit(64))
	require.NoError(t, b.SetLimit(10))
	assert.EqualValues(t, 10, b.Limit())
}

func TestBufferBuilderPanicsIfDataExceedsMaxCapacity(t *testing.T) {
	for _, length := range []int32{math.MaxInt32, math.MaxInt32 - 23} {
		src := atomic.MakeBuffer(make([]byte, 16), 16)
		src.PutInt64(0, math.MaxInt64)
		src.PutInt64(8, math.MinInt64)
		b := mustBufferBuilder(t, 64)
		b.Append(src, 0, src.Capacity())
		assert.EqualValues(t, 16, b.Limit())
		assert.PanicsWithValue(t,
			fmt.Sprintf("insufficient capacity: maxCapacity=2147483639 limit=16 additionalLength=%d", length),
			func() { b.Append(src, 0, length) })
	}
}

func TestBufferBuilderAppendsNothingForZeroLength(t *testing.T) {
	b := mustBufferBuilder(t, 0)
	b.Append(atomic.MakeBuffer(make([]byte, 4096), 4096), 0, 0)
	assert.EqualValues(t, 0, b.Limit())
	assert.EqualValues(t, 0, b.Capacity())
}

func TestBufferBuilderGrowsToMultipleOfInitialCapacity(t *testing.T) {
	b := mustBufferBuilder(t, 0)
	n := BufferBuilderInitMinCapacity * 5
	b.Append(atomic.MakeBuffer(make([]byte, n), n), 0, n)
	assert.Equal(t, n, b.Limit())
	assert.GreaterOrEqual(t, b.Capacity(), n)
}

func TestBufferBuilderAppendThenReset(t *testing.T) {
	b := mustBufferBuilder(t, 0)
	n := BufferBuilderInitMinCapacity
	b.Append(atomic.MakeBuffer(make([]byte, n), n), 0, n)
	b.SetNextTermOffset(1024)
	assert.Equal(t, n, b.Limit())
	assert.EqualValues(t, 1024, b.NextTermOffset())
	b.Reset()
	assert.EqualValues(t, 0, b.Limit())
	assert.EqualValues(t, NullValue, b.NextTermOffset())
}

func TestBufferBuilderAppendOneBufferWithoutResizing(t *testing.T) {
	b := mustBufferBuilder(t, 0)
	msg := []byte("Hello World")
	src := atomic.MakeBuffer(make([]byte, 4096), 4096)
	src.PutBytesSlice(0, msg)
	b.Append(src, 0, int32(len(msg)))
	assert.Equal(t, msg, b.Buffer().GetBytesArray(0, int32(len(msg))))
	assert.Equal(t, msg, b.Bytes())
	assert.EqualValues(t, len(msg), b.Limit())
	assert.Equal(t, BufferBuilderInitMinCapacity, b.Capacity())
}

func TestBufferBuilderAppendTwoBuffersWithoutResizing(t *testing.T) {
	b := mustBufferBuilder(t, 0)
	msg := []byte("1111111122222222")
	src := atomic.MakeBuffer(msg, len(msg))
	b.Append(src, 0, 8)
	b.Append(src, 8, 8)
	assert.Equal(t, msg, b.Bytes())
	assert.Equal(t, BufferBuilderInitMinCapacity, b.Capacity())
}

func TestBufferBuilderFillBufferWithoutResizing(t *testing.T) {
	data := patternBytes(128, 7)
	b := mustBufferBuilder(t, 128)
	b.Append(atomic.MakeBuffer(data, len(data)), 0, 128)
	assert.Equal(t, data, b.Bytes())
	assert.EqualValues(t, 128, b.Capacity())
}

func TestBufferBuilderResizeWhenBufferJustDoesNotFit(t *testing.T) {
	data := patternBytes(129, 7)
	b := mustBufferBuilder(t, 128)
	b.Append(atomic.MakeBuffer(data, len(data)), 0, 129)
	assert.Equal(t, data, b.Bytes())
	assert.Greater(t, b.Capacity(), int32(128))
}

func TestBufferBuilderAppendTwoBuffersAndResize(t *testing.T) {
	data := patternBytes(128, 3)
	src := atomic.MakeBuffer(data, len(data))
	b := mustBufferBuilder(t, 64)
	b.Append(src, 0, 32)
	b.Append(src, 32, 64)
	assert.Equal(t, data[:96], b.Bytes())
	assert.GreaterOrEqual(t, b.Capacity(), int32(96))
}

func TestBufferBuilderAppendBytes(t *testing.T) {
	b := mustBufferBuilder(t, 0)
	data := patternBytes(10000, 1)
	b.AppendBytes(data[:5000]).AppendBytes(data[5000:])
	assert.Equal(t, data, b.Bytes())
	assert.Equal(t, data, b.Buffer().GetBytesArray(0, b.Limit()))
}

func TestBufferBuilderCompactToLowerLimit(t *testing.T) {
	n := BufferBuilderInitMinCapacity / 2
	src := atomic.MakeBuffer(make([]byte, n), n)
	b := mustBufferBuilder(t, 0)
	for i := 0; i < 5; i++ {
		b.Append(src, 0, n)
	}
	assert.Equal(t, 5*n, b.Limit())
	expanded := b.Capacity()
	assert.Greater(t, expanded, 5*n)

	b.Reset()
	b.Append(src, 0, n).Append(src, 0, n).Append(src, 0, n)
	b.Compact()
	assert.Equal(t, 3*n, b.Limit())
	assert.Less(t, b.Capacity(), expanded)
	assert.Equal(t, b.Limit(), b.Capacity())
}

func TestBufferBuilderCompactEmptyIsNoOp(t *testing.T) {
	b := mustBufferBuilder(t, 0)
	assert.Same(t, b, b.Compact())
	assert.EqualValues(t, 0, b.Capacity())
	assert.EqualValues(t, 0, b.Limit())
}

func TestBufferBuilderCompactIsNoOpIfCapacityNotDecreasing(t *testing.T) {
	b := mustBufferBuilder(t, 0)
	b.Append(atomic.MakeBuffer(make([]byte, 5), 5), 0, 3)
	ptr := b.Buffer().Ptr()
	assert.Same(t, b, b.Compact())
	assert.EqualValues(t, 3, b.Limit())
	assert.Equal(t, BufferBuilderInitMinCapacity, b.Capacity())
	assert.Equal(t, ptr, b.Buffer().Ptr())
}

func TestBufferBuilderCaptureHeaderCopiesEntireHeader(t *testing.T) {
	raw := make([]byte, 100)
	buf := atomic.MakeBuffer(raw, len(raw))
	offset := int32(40)
	reserved := int64(math.MaxInt64 - 117)
	buf.PutInt32(offset, 512)
	buf.PutUInt8(offset+logbuffer.DataFrameHeader.VersionFieldOffset, 0xA)
	buf.PutUInt8(offset+logbuffer.DataFrameHeader.FlagsFieldOffset, 0b1110_0111)
	buf.PutUInt16(offset+logbuffer.DataFrameHeader.TypeFieldOffset, 0x05)
	buf.PutInt32(offset+logbuffer.DataFrameHeader.TermOffsetFieldOffset, 384)
	buf.PutInt32(offset+logbuffer.DataFrameHeader.SessionIDFieldOffset, -890)
	buf.PutInt32(offset+logbuffer.DataFrameHeader.StreamIDFieldOffset, 555)
	buf.PutInt32(offset+logbuffer.DataFrameHeader.TermIDFieldOffset, 42)
	buf.PutInt64(offset+logbuffer.DataFrameHeader.ReservedValueFieldOffset, reserved)

	header := logbuffer.NewHeader(5, 3)
	header.Wrap(buf.Ptr(), buf.Capacity()).SetOffset(offset)

	b := mustBufferBuilder(t, 0)
	assert.Same(t, b, b.CaptureHeader(header))
	assert.EqualValues(t, 0, b.Capacity())
	assert.EqualValues(t, 0, b.Limit())
	assert.Equal(t, buf.Ptr(), header.Buffer().Ptr())
	assert.Equal(t, offset, header.Offset())

	ch := &b.completeHeader
	assert.EqualValues(t, 512, ch.FrameLength())
	assert.EqualValues(t, 0xA, b.headerBuffer.GetUInt8(logbuffer.DataFrameHeader.VersionFieldOffset))
	assert.EqualValues(t, 0b1110_0111, ch.Flags())
	assert.EqualValues(t, 0x05, ch.Type())
	assert.EqualValues(t, 384, ch.TermOffset())
	assert.EqualValues(t, -890, ch.SessionID())
	assert.EqualValues(t, 555, ch.StreamID())
	assert.EqualValues(t, 42, ch.TermID())
	assert.Equal(t, reserved, ch.ReservedValue())
	assert.EqualValues(t, 5, ch.InitialTermID())
	assert.EqualValues(t, 3, ch.PositionBitsToShift())
}

func writeRawHeader(raw []byte, offset int32, frameLength int32, version uint8, flags uint8, typ uint16,
	termOffset, sessionID, streamID, termID int32, reserved int64) *atomic.Buffer {
	buf := atomic.MakeBuffer(raw, len(raw))
	buf.PutInt32(offset, frameLength)
	buf.PutUInt8(offset+logbuffer.DataFrameHeader.VersionFieldOffset, version)
	buf.PutUInt8(offset+logbuffer.DataFrameHeader.FlagsFieldOffset, flags)
	buf.PutUInt16(offset+logbuffer.DataFrameHeader.TypeFieldOffset, typ)
	buf.PutInt32(offset+logbuffer.DataFrameHeader.TermOffsetFieldOffset, termOffset)
	buf.PutInt32(offset+logbuffer.DataFrameHeader.SessionIDFieldOffset, sessionID)
	buf.PutInt32(offset+logbuffer.DataFrameHeader.StreamIDFieldOffset, streamID)
	buf.PutInt32(offset+logbuffer.DataFrameHeader.TermIDFieldOffset, termID)
	buf.PutInt64(offset+logbuffer.DataFrameHeader.ReservedValueFieldOffset, reserved)
	return buf
}

func TestBufferBuilderPrepareCompleteHeader(t *testing.T) {
	data := atomic.MakeBuffer(make([]byte, 999), 999)
	frameLength := int32(128)
	reserved := int64(-0x3501454121524111) // 0xCAFEBABEDEADBEEF
	first := writeRawHeader(make([]byte, 44), 1, frameLength, 15, 0b1011_1001, 0x00,
		1024, 87, -9, 10, reserved)
	header := logbuffer.NewHeader(4, 48)
	header.Wrap(first.Ptr(), first.Capacity()).SetOffset(1)
	header.SetContext("image")

	b := mustBufferBuilder(t, 0)
	b.CaptureHeader(header)
	b.Append(data, 0, frameLength-hdrLen).Append(data, 0, frameLength-hdrLen)

	last := writeRawHeader(make([]byte, 88), 19, frameLength, 0xC, 0b0100_0100, 0x01,
		48, 999, 4, 39, math.MinInt64)
	header.Wrap(last.Ptr(), last.Capacity()).SetOffset(19)

	ch := b.CompleteHeader(header)
	assert.NotSame(t, header, ch)
	assert.Equal(t, BufferBuilderInitMinCapacity, b.Capacity())
	assert.Equal(t, 2*(frameLength-hdrLen), b.Limit())
	assert.Equal(t, last.Ptr(), header.Buffer().Ptr())
	assert.EqualValues(t, 19, header.Offset())
	assert.Equal(t, b.headerBuffer.Ptr(), ch.Buffer().Ptr())
	assert.EqualValues(t, 0, ch.Offset())
	assert.EqualValues(t, 4, ch.InitialTermID())
	assert.EqualValues(t, 48, ch.PositionBitsToShift())
	assert.Equal(t, hdrLen+2*(frameLength-hdrLen), ch.FrameLength())
	assert.EqualValues(t, 15, ch.Buffer().GetUInt8(logbuffer.DataFrameHeader.VersionFieldOffset))
	assert.EqualValues(t, 0xFD, ch.Flags())
	assert.EqualValues(t, 0x00, ch.Type())
	assert.EqualValues(t, 1024, ch.TermOffset())
	assert.EqualValues(t, 87, ch.SessionID())
	assert.EqualValues(t, -9, ch.StreamID())
	assert.EqualValues(t, 10, ch.TermID())
	assert.Equal(t, reserved, ch.ReservedValue())
	assert.Equal(t, "image", ch.Context())

	b.Reset()
	assert.Nil(t, ch.Context())
	assert.EqualValues(t, NullValue, ch.FragmentedFrameLength())
}

func TestBufferBuilderCalculatesPositionAndFrameLengthWhenReassembling(t *testing.T) {
	for _, c := range []struct{ total, mtu int32 }{{1024, 1408}, {1024, 128}, {8192, 1408}} {
		maxPayload := c.mtu - hdrLen
		termOffset, termID, initialTermID, bits := int32(1024), int32(10), int32(4), int32(16)
		fragmentedFrameLength := logbuffer.ComputeFragmentedFrameLength(c.total, maxPayload)
		startPosition := int64(termID-initialTermID)<<bits + int64(termOffset)

		first := writeRawHeader(make([]byte, 32), 0, hdrLen+maxPayload, 15, 0b1011_1001, 0x01,
			termOffset, 87, -9, termID, 1)
		header := logbuffer.NewHeader(initialTermID, bits)
		header.Wrap(first.Ptr(), first.Capacity()).SetOffset(0)

		b := mustBufferBuilder(t, 0)
		b.CaptureHeader(header)
		data := atomic.MakeBuffer(make([]byte, c.total), c.total)
		lastPayload := int32(0)
		for pos := int32(0); pos < c.total; pos += maxPayload {
			lastPayload = c.total - pos
			if lastPayload > maxPayload {
				lastPayload = maxPayload
			}
			b.Append(data, pos, lastPayload)
		}
		last := writeRawHeader(make([]byte, 32), 0, hdrLen+lastPayload, 15, 0b1011_1001, 0x01,
			math.MinInt32, 87, -9, termID, 1)
		header.Wrap(last.Ptr(), last.Capacity()).SetOffset(0)

		ch := b.CompleteHeader(header)
		assert.Equal(t, hdrLen+c.total, ch.FrameLength(), "%+v", c)
		assert.Equal(t, fragmentedFrameLength, ch.FragmentedFrameLength(), "%+v", c)
		assert.Equal(t, startPosition+int64(fragmentedFrameLength), ch.Position(), "%+v", c)
	}
}

func TestBufferBuilderHeaderBufferUnchangedUponResize(t *testing.T) {
	b := mustBufferBuilder(t, 0)
	ptr := b.headerBuffer.Ptr()
	for i := int32(0); i < b.headerBuffer.Capacity(); i++ {
		b.headerBuffer.PutUInt8(i, 0xFA)
	}
	src := make([]byte, 100)
	rand.Read(src)
	b.Append(atomic.MakeBuffer(src, len(src)), 0, 100)
	assert.Equal(t, ptr, b.headerBuffer.Ptr())
	for i := int32(0); i < b.headerBuffer.Capacity(); i++ {
		assert.EqualValues(t, 0xFA, b.headerBuffer.GetUInt8(i))
	}
}

func TestComputeFragmentedFrameLength(t *testing.T) {
	assert.EqualValues(t, 0, logbuffer.ComputeFragmentedFrameLength(0, 1376))
	assert.EqualValues(t, 64, logbuffer.ComputeFragmentedFrameLength(1, 1376))
	assert.EqualValues(t, 1408, logbuffer.ComputeFragmentedFrameLength(1376, 1376))
	assert.EqualValues(t, 1408+64, logbuffer.ComputeFragmentedFrameLength(1377, 1376))
	assert.EqualValues(t, 3*1408, logbuffer.ComputeFragmentedFrameLength(3*1376, 1376))
}
