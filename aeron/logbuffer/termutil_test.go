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
	"testing"

	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/protocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	testTermID    int32 = 7
	testSessionID int32 = 11
	testStreamID  int32 = 1001
)

func newTerm() *atomic.Buffer { return atomic.MakeBuffer(make([]byte, TermMinLength)) }

func newMetaWithHeader(t *testing.T) *LogBufferMetaData {
	m := newMeta()
	require.NoError(t, m.StoreDefaultFrameHeader(protocol.CreateDefaultHeader(testSessionID, testStreamID, 0)))
	return m
}

func putFrame(term *atomic.Buffer, offset, length int32) {
	term.PutInt32(offset+DataFrameHeader.TermOffsetFieldOffset, offset)
	term.PutInt32(offset, length)
}

func assertPadding(t *testing.T, term *atomic.Buffer, offset, length int32) {
	var d protocol.DataHeaderFlyweight
	d.Wrap(term, offset)
	assert.Equal(t, length, d.FrameLength())
	assert.Equal(t, protocol.HdrTypePad, d.HeaderType())
	assert.Equal(t, offset, d.TermOffset())
	assert.Equal(t, testTermID, d.TermID())
	assert.Equal(t, testSessionID, d.SessionID())
	assert.Equal(t, testStreamID, d.StreamID())
}

// TermRebuilderTest
func TestTermRebuilderInsertsIntoEmptyBufferAndSkipsExisting(t *testing.T) {
	term := newTerm()
	packet := atomic.MakeBuffer(make([]byte, 256))
	hdr := protocol.CreateDefaultHeader(testSessionID, testStreamID, testTermID)
	packet.PutBytes(0, hdr, 0, 32)
	var d protocol.DataHeaderFlyweight
	d.Wrap(packet, 0)
	d.SetFrameLength(256)
	d.SetTermOffset(512)
	for i := int32(32); i < 256; i++ {
		packet.PutUInt8(i, uint8(i))
	}

	TermRebuilderInsert(term, 512, packet, 256)
	assert.Equal(t, packet.GetBytesArray(0, 256), term.GetBytesArray(512, 256))

	other := atomic.MakeBuffer(make([]byte, 256))
	other.PutInt32(0, 256)
	TermRebuilderInsert(term, 512, other, 256)
	assert.Equal(t, packet.GetBytesArray(0, 256), term.GetBytesArray(512, 256))
}

// TermGapScannerTest
func TestTermGapScanner(t *testing.T) {
	type gap struct{ termID, offset, length int32 }
	scan := func(term *atomic.Buffer, from, limit int32) (int32, []gap) {
		var gaps []gap
		r := TermGapScan(term, testTermID, from, limit, func(id, o, l int32) { gaps = append(gaps, gap{id, o, l}) })
		return r, gaps
	}

	t.Run("reportGapAtBeginningOfBuffer", func(t *testing.T) {
		term := newTerm()
		putFrame(term, 128, 32)
		r, gaps := scan(term, 0, 128+32)
		assert.Equal(t, int32(0), r)
		assert.Equal(t, []gap{{testTermID, 0, 128}}, gaps)
	})
	t.Run("reportSingleGapWhenBufferNotFull", func(t *testing.T) {
		term := newTerm()
		putFrame(term, 0, 32)
		putFrame(term, 32, 32)
		putFrame(term, 128, 32)
		r, gaps := scan(term, 0, 160)
		assert.Equal(t, int32(64), r)
		assert.Equal(t, []gap{{testTermID, 64, 64}}, gaps)
	})
	t.Run("reportSingleGapWhenBufferIsFull", func(t *testing.T) {
		term := newTerm()
		tail := TermMinLength - 64
		putFrame(term, tail-32, 32)
		putFrame(term, tail+32, 32)
		r, gaps := scan(term, tail-32, TermMinLength)
		assert.Equal(t, tail, r)
		assert.Equal(t, []gap{{testTermID, tail, 32}}, gaps)
	})
	t.Run("reportNoGapWhenHwmIsInPadding", func(t *testing.T) {
		term := newTerm()
		padding := int32(64)
		tail := TermMinLength - padding
		putFrame(term, tail-32, 32)
		putFrame(term, tail, padding)
		r, gaps := scan(term, tail-32, TermMinLength)
		assert.Equal(t, TermMinLength, r)
		assert.Empty(t, gaps)
	})
}

// TermGapFillerTest
func TestTermGapFiller(t *testing.T) {
	m := newMetaWithHeader(t)
	term := newTerm()
	assert.True(t, TermGapFill(m, term, testTermID, 0, 128))
	assertPadding(t, term, 0, 128)

	term = newTerm()
	putFrame(term, 64, 64)
	assert.False(t, TermGapFill(m, term, testTermID, 0, 128))
	assert.Equal(t, int32(0), term.GetInt32(0))

	term = newTerm()
	putFrame(term, 0, 64)
	assert.True(t, TermGapFill(m, term, testTermID, 64, 128))
	assertPadding(t, term, 64, 128)
}

// TermUnblockerTest
func TestTermUnblocker(t *testing.T) {
	m := newMetaWithHeader(t)
	msgLen := int32(256)

	t.Run("noActionWhenMessageIsComplete", func(t *testing.T) {
		term := newTerm()
		putFrame(term, 0, msgLen)
		assert.Equal(t, TermUnblockNoAction, TermUnblock(m, term, 0, msgLen, testTermID))
	})
	t.Run("noActionWhenNoUnblockedMessage", func(t *testing.T) {
		term := newTerm()
		assert.Equal(t, TermUnblockNoAction, TermUnblock(m, term, 0, msgLen, testTermID))
	})
	t.Run("patchNonCommittedMessage", func(t *testing.T) {
		term := newTerm()
		term.PutInt32(0, -msgLen)
		assert.Equal(t, TermUnblockUnblocked, TermUnblock(m, term, 0, msgLen, testTermID))
		assertPadding(t, term, 0, msgLen)
	})
	t.Run("patchToEndOfPartition", func(t *testing.T) {
		term := newTerm()
		blocked := TermMinLength - msgLen
		assert.Equal(t, TermUnblockUnblockedToEnd, TermUnblock(m, term, blocked, TermMinLength, testTermID))
		assertPadding(t, term, blocked, msgLen)
	})
	t.Run("scanForwardForNextCompleteMessage", func(t *testing.T) {
		term := newTerm()
		putFrame(term, msgLen, msgLen)
		assert.Equal(t, TermUnblockUnblocked, TermUnblock(m, term, 0, msgLen*2, testTermID))
		assertPadding(t, term, 0, msgLen)
	})
	t.Run("scanForwardForNextNonCommittedMessage", func(t *testing.T) {
		term := newTerm()
		term.PutInt32(msgLen, -msgLen)
		assert.Equal(t, TermUnblockUnblocked, TermUnblock(m, term, 0, msgLen*2, testTermID))
		assertPadding(t, term, 0, msgLen)
	})
}

// LogBufferUnblockerTest
func TestLogBufferUnblocker(t *testing.T) {
	newLog := func() (*LogBufferMetaData, []*atomic.Buffer) {
		m := newMetaWithHeader(t)
		m.SetTermLength(TermMinLength)
		m.SetInitialTermID(testTermID)
		m.InitialiseTailWithTermID(0, testTermID)
		for i := int32(1); i < PartitionCount; i++ {
			m.InitialiseTailWithTermID(i, testTermID+i-PartitionCount)
		}
		return m, []*atomic.Buffer{newTerm(), newTerm(), newTerm()}
	}
	shift := PositionBitsToShift(TermMinLength)

	t.Run("notUnblockWhenPositionHasCompleteMessage", func(t *testing.T) {
		m, terms := newLog()
		m.SetRawTail(0, PackTail(testTermID, 64))
		putFrame(terms[0], 0, 64)
		assert.False(t, LogBufferUnblock(terms, m, 0, TermMinLength))
		assert.Equal(t, PackTail(testTermID, 64), m.RawTailVolatile(0))
	})
	t.Run("unblockWhenPositionHasNonCommittedMessageAndTailWithinTerm", func(t *testing.T) {
		m, terms := newLog()
		m.SetRawTail(0, PackTail(testTermID, 256))
		terms[0].PutInt32(0, -128)
		assert.True(t, LogBufferUnblock(terms, m, 0, TermMinLength))
		assertPadding(t, terms[0], 0, 128)
	})
	t.Run("unblockWhenPositionHasNonCommittedMessageAndTailAtEndOfTerm", func(t *testing.T) {
		m, terms := newLog()
		blocked := TermMinLength - 128
		m.SetRawTail(0, PackTail(testTermID, TermMinLength))
		assert.True(t, LogBufferUnblock(terms, m, int64(blocked), TermMinLength))
		assertPadding(t, terms[0], blocked, 128)
		assert.Equal(t, int32(1), m.ActiveTermCount())
		assert.Equal(t, PackTail(testTermID+1, 0), m.RawTailVolatile(1))
	})
	t.Run("unblockWhenBlockedAtStartOfNextTerm", func(t *testing.T) {
		m, terms := newLog()
		m.SetRawTail(0, PackTail(testTermID, TermMinLength))
		blockedPosition := ComputeTermBeginPosition(testTermID+1, shift, testTermID)
		assert.True(t, LogBufferUnblock(terms, m, blockedPosition, TermMinLength))
		assert.Equal(t, int32(1), m.ActiveTermCount())
		assert.Equal(t, PackTail(testTermID+1, 0), m.RawTailVolatile(1))
	})
}
