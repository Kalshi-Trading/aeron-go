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
	"os"
	"path/filepath"
	"testing"

	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/logbuffer"
	"github.com/lirm/aeron-go/aeron/protocol"
	"github.com/stretchr/testify/require"
)

const (
	apSessionID int32 = 0x5E55
	apStreamID  int32 = 0x5743
	apTermID    int32 = 7
)

func newAppenderLog(t *testing.T) *logbuffer.LogBuffers {
	t.Helper()
	dir := t.TempDir()
	wd, err := os.Getwd()
	require.NoError(t, err)
	require.NoError(t, os.Chdir(dir))
	lb, err := logbuffer.NewTestingLogbuffer()
	require.NoError(t, os.Chdir(wd))
	require.NoError(t, err)
	t.Cleanup(func() {
		lb.Close()
		os.Remove(filepath.Join(dir, logbuffer.Filename))
	})
	require.NoError(t, lb.Meta().StoreDefaultFrameHeader(protocol.CreateDefaultHeader(apSessionID, apStreamID, 0)))
	return lb
}

func requireDataHeader(t *testing.T, term *atomic.Buffer, offset, frameLength int32, flags uint8, typ uint16) {
	t.Helper()
	require.Equal(t, frameLength, logbuffer.GetFrameLength(term, offset), "frame length")
	require.Equal(t, uint8(protocol.CurrentVersion), logbuffer.FrameVersion(term, offset), "version")
	require.Equal(t, flags, logbuffer.GetFlags(term, offset), "flags")
	require.Equal(t, typ, logbuffer.FrameType(term, offset), "type")
	require.Equal(t, offset, logbuffer.GetFrameTermOffset(term, offset), "term offset")
	require.Equal(t, apSessionID, logbuffer.GetFrameSessionID(term, offset), "session id")
	require.Equal(t, apStreamID, logbuffer.GetFrameStreamID(term, offset), "stream id")
	require.Equal(t, apTermID, logbuffer.GetFrameTermID(term, offset), "term id")
}

func payload(n int32) *atomic.Buffer {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(i + 1)
	}
	return atomic.MakeBuffer(b)
}

func TestAppenderAppendsUnfragmentedFrameAndAdvancesTail(t *testing.T) {
	lb := newAppenderLog(t)
	a := MakeAppender(lb, 0)
	a.SetRawTail(logbuffer.PackTail(apTermID, 64))

	const msgLength int32 = 20
	supplier := func(*atomic.Buffer, int32, int32) int64 { return 0x1234 }
	resultingOffset, termID := a.AppendUnfragmentedMessage(payload(msgLength), 0, msgLength, supplier)

	frameLength := msgLength + logbuffer.DataFrameHeader.Length
	require.Equal(t, apTermID, termID)
	require.Equal(t, int64(64+64), resultingOffset)
	require.Equal(t, logbuffer.PackTail(apTermID, 128), a.RawTail())
	requireDataHeader(t, a.TermBuffer(), 64, frameLength, logbuffer.Unfragmented, logbuffer.DataFrameHeader.TypeData)
	require.Equal(t, int64(0x1234), logbuffer.GetReservedValue(a.TermBuffer(), 64))
	require.Equal(t, payload(msgLength).GetBytesArray(0, msgLength),
		a.TermBuffer().GetBytesArray(64+logbuffer.DataFrameHeader.Length, msgLength))
}

func TestAppenderPadsAndTripsAtEndOfTerm(t *testing.T) {
	lb := newAppenderLog(t)
	a := MakeAppender(lb, 1)
	termLength := a.TermBuffer().Capacity()
	tail := termLength - 64
	a.SetRawTail(logbuffer.PackTail(apTermID, tail))

	resultingOffset, termID := a.AppendUnfragmentedMessage(payload(64), 0, 64, nil)

	require.Equal(t, AppenderTripped, resultingOffset)
	require.Equal(t, apTermID, termID)
	require.Equal(t, logbuffer.PackTail(apTermID, tail+96), a.RawTail(), "tail advanced past the end like Java getAndAdd")
	requireDataHeader(t, a.TermBuffer(), tail, 64, logbuffer.Unfragmented, logbuffer.DataFrameHeader.TypePad)

	// A second append once the tail is past the end trips without writing anything.
	resultingOffset, _ = a.Claim(10, &logbuffer.Claim{})
	require.Equal(t, AppenderTripped, resultingOffset)
	require.Equal(t, int32(64), logbuffer.GetFrameLength(a.TermBuffer(), tail), "padding frame not overwritten")
}

func TestAppenderFragmentsMessage(t *testing.T) {
	lb := newAppenderLog(t)
	a := MakeAppender(lb, 2)
	a.SetRawTail(logbuffer.PackTail(apTermID, 0))

	const maxPayload int32 = 64
	const msgLength int32 = maxPayload*2 + 1
	resultingOffset, _ := a.AppendFragmentedMessage(payload(msgLength), 0, msgLength, maxPayload, nil)

	require.Equal(t, int64(ComputeFragmentedFrameLength(msgLength, maxPayload)), resultingOffset)
	requireDataHeader(t, a.TermBuffer(), 0, 96, logbuffer.BeginFrag, logbuffer.DataFrameHeader.TypeData)
	requireDataHeader(t, a.TermBuffer(), 96, 96, 0, logbuffer.DataFrameHeader.TypeData)
	requireDataHeader(t, a.TermBuffer(), 192, 33, logbuffer.EndFrag, logbuffer.DataFrameHeader.TypeData)
	require.Equal(t, byte(msgLength), a.TermBuffer().GetUInt8(192+logbuffer.DataFrameHeader.Length))
}

func TestAppenderClaimCommitAndAbort(t *testing.T) {
	lb := newAppenderLog(t)
	a := MakeAppender(lb, 0)
	a.SetRawTail(logbuffer.PackTail(apTermID, 0))

	var claim logbuffer.Claim
	resultingOffset, _ := a.Claim(10, &claim)
	require.Equal(t, int64(64), resultingOffset)
	require.Equal(t, int32(-42), logbuffer.GetFrameLength(a.TermBuffer(), 0), "claimed frame length is negative until commit")
	require.Equal(t, int32(10), claim.Length())
	claim.Commit()
	requireDataHeader(t, a.TermBuffer(), 0, 42, logbuffer.Unfragmented, logbuffer.DataFrameHeader.TypeData)

	resultingOffset, _ = a.Claim(10, &claim)
	require.Equal(t, int64(128), resultingOffset)
	claim.Abort()
	requireDataHeader(t, a.TermBuffer(), 64, 42, logbuffer.Unfragmented, logbuffer.DataFrameHeader.TypePad)
}

func TestExclusiveAppenderTripSetsTailToResultingOffsetLikeJava(t *testing.T) {
	lb := newAppenderLog(t)
	a := MakeExclusiveAppender(lb, 0)
	termLength := a.TermBuffer().Capacity()
	termOffset := termLength - 32

	result := a.AppendUnfragmentedMessage(apTermID, termOffset, payload(64), 0, 64, nil)

	// ExclusivePublication.appendUnfragmentedMessage: putLongRelease(tail, packTail(termId, termOffset + alignedLength)).
	require.Equal(t, int32(AppenderTripped), result)
	require.Equal(t, logbuffer.PackTail(apTermID, termOffset+96), a.RawTail())
	requireDataHeader(t, a.TermBuffer(), termOffset, 32, logbuffer.Unfragmented, logbuffer.DataFrameHeader.TypePad)
}

func TestExclusiveAppenderAppendsAndPads(t *testing.T) {
	lb := newAppenderLog(t)
	a := MakeExclusiveAppender(lb, 1)

	result := a.AppendUnfragmentedMessage(apTermID, 0, payload(8), 0, 8, nil)
	require.Equal(t, int32(64), result)
	require.Equal(t, logbuffer.PackTail(apTermID, 64), a.RawTail())
	requireDataHeader(t, a.TermBuffer(), 0, 40, logbuffer.Unfragmented, logbuffer.DataFrameHeader.TypeData)

	result = a.AppendPadding(apTermID, 64, 100)
	require.Equal(t, int32(64+160), result)
	requireDataHeader(t, a.TermBuffer(), 64, 132, logbuffer.Unfragmented, logbuffer.DataFrameHeader.TypePad)

	var claim logbuffer.Claim
	result = a.Claim(apTermID, 224, 1, &claim)
	require.Equal(t, int32(224+64), result)
	require.Equal(t, int32(-33), logbuffer.GetFrameLength(a.TermBuffer(), 224))
}

func TestExclusiveAppenderAppendBlock(t *testing.T) {
	lb := newAppenderLog(t)
	a := MakeExclusiveAppender(lb, 2)
	src := atomic.MakeBuffer(make([]byte, 256))
	src.PutInt32(32, 64)
	src.PutInt32(32+64, 32)

	result := a.AppendBlock(apTermID, 128, src, 32, 96)

	require.Equal(t, int32(128+96), result)
	require.Equal(t, logbuffer.PackTail(apTermID, 224), a.RawTail())
	require.Equal(t, int32(64), logbuffer.GetFrameLength(a.TermBuffer(), 128))
	require.Equal(t, int32(32), logbuffer.GetFrameLength(a.TermBuffer(), 192))
	require.Equal(t, int32(64), src.GetInt32(32), "source first frame length restored")
}

func TestReadSkipsPaddingAndHonoursLimits(t *testing.T) {
	buf := newTerm()
	next := putFrame(buf, 0, 40, logbuffer.DataFrameHeader.TypeData)
	next = putFrame(buf, next, 64, logbuffer.DataFrameHeader.TypePad)
	next = putFrame(buf, next, 33, logbuffer.DataFrameHeader.TypeData)
	end := putFrame(buf, next, 32, logbuffer.DataFrameHeader.TypeData)

	var offsets, lengths []int32
	handler := func(_ *atomic.Buffer, offset, length int32, h *logbuffer.Header) {
		require.Equal(t, offset-logbuffer.DataFrameHeader.Length, h.Offset())
		offsets = append(offsets, offset)
		lengths = append(lengths, length)
	}
	header := logbuffer.NewHeader(0, 16)

	offset, read := Read(buf, 0, handler, 10, header)
	require.Equal(t, end, offset, "stops at the first zero length frame")
	require.Equal(t, 3, read, "padding is not delivered")
	require.Equal(t, []int32{32, 160, 224}, offsets)
	require.Equal(t, []int32{8, 1, 0}, lengths)

	offset, read = Read(buf, 0, handler, 1, header)
	require.Equal(t, int32(64), offset)
	require.Equal(t, 1, read)

	offset, read = Read(buf, 64, handler, 1, header)
	require.Equal(t, int32(192), offset, "padding is consumed without counting toward the fragment limit")
	require.Equal(t, 1, read)

	offset, read = BoundedRead(buf, 0, 128, handler, 10, header)
	require.Equal(t, int32(128), offset)
	require.Equal(t, 1, read)

	offset, read = Read(buf, buf.Capacity(), handler, 10, header)
	require.Equal(t, buf.Capacity(), offset)
	require.Equal(t, 0, read)
}
