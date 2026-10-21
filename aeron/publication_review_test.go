package aeron

import (
	"bytes"
	"math/rand"
	"runtime/debug"
	"syscall"
	"testing"
	"unsafe"

	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/logbuffer"
	"github.com/lirm/aeron-go/aeron/logbuffer/term"
	"github.com/lirm/aeron-go/aeron/util"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTryClaimNegativeLengthPanicsRegardlessOfState(t *testing.T) {
	l := newTestLog(t, logbuffer.TermMinLength, 0, 1408)
	var claim BufferClaim
	for _, p := range []Publisher{newTestPublication(t, l, 0), newTestExclusivePublication(t, l, 0)} {
		assert.Panics(t, func() { p.TryClaim(-1, &claim) }, "back pressured")
		require.NoError(t, p.Close())
		assert.Panics(t, func() { p.TryClaim(-1, &claim) }, "closed")
	}
}

func TestOfferVectorsValidatesBeforeClosedCheck(t *testing.T) {
	l := newTestLog(t, logbuffer.TermMinLength, 0, 1408)
	bad := []DirectBufferVector{{Buffer: bufOf(make([]byte, 8)), Offset: 0, Length: 9}}
	for _, p := range []Publisher{newTestPublication(t, l, 1<<20), newTestExclusivePublication(t, l, 1<<20)} {
		require.NoError(t, p.Close())
		assert.Panics(t, func() { p.OfferVectors(bad, nil) })
	}
}

func TestOffer2InvalidLengthReturnsAdminActionDuringRotation(t *testing.T) {
	l := newTestLog(t, logbuffer.TermMinLength, 0, 1408)
	pub := newTestPublication(t, l, 1<<20)
	l.lb.Buffer(logbuffer.LogMetaDataSectionIndex).PutInt32(logActiveTermCountOffset, 1)
	buf := bufOf(make([]byte, 8))
	assert.Equal(t, AdminAction, pub.Offer2(buf, 0, -1, buf, 0, 1, nil))
}

func TestAppendPaddingNegativeLengthWhenBackPressured(t *testing.T) {
	l := newTestLog(t, logbuffer.TermMinLength, 0, 1408)
	ex := newTestExclusivePublication(t, l, 0)
	assert.Equal(t, BackPressured, ex.AppendPadding(-1))
	ex.pubLimit.set(1 << 20)
	assert.Panics(t, func() { ex.AppendPadding(-1) })
}

func TestExclusiveTrippedTailPacksResultingOffset(t *testing.T) {
	termLength := logbuffer.TermMinLength
	l := newTestLog(t, termLength, 7, 1408)
	l.setPositionInTerm(0, 7, termLength-64)
	ex := newTestExclusivePublication(t, l, 1<<40)
	assert.Equal(t, AdminAction, ex.Offer(bufOf(make([]byte, 100)), 0, 100, nil))
	rawTail := l.lb.Buffer(logbuffer.LogMetaDataSectionIndex).GetInt64(termTailCountersOffset)
	assert.Equal(t, int32(7), logbuffer.TermID(rawTail))
	assert.Equal(t, int64(termLength-64)+aligned(100), rawTail&0xFFFFFFFF)
}

func TestOfferBlockDoesNotWriteSourceBuffer(t *testing.T) {
	l := newTestLog(t, logbuffer.TermMinLength, 0, 1408)
	ex := newTestExclusivePublication(t, l, 1<<20)
	block := buildBlock(ex, pattern(100, 1), pattern(50, 2))

	pageSize := syscall.Getpagesize()
	mem, err := syscall.Mmap(-1, 0, pageSize, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_ANON|syscall.MAP_PRIVATE)
	require.NoError(t, err)
	defer func() { _ = syscall.Munmap(mem) }()
	copy(mem, block)
	require.NoError(t, syscall.Mprotect(mem, syscall.PROT_READ))

	src := atomic.MakeBuffer(unsafe.Pointer(&mem[0]), len(block))
	defer debug.SetPanicOnFault(debug.SetPanicOnFault(true))
	var pos int64
	require.NotPanics(t, func() { pos = ex.OfferBlock(src, 0, int32(len(block))) })
	assert.Equal(t, int64(len(block)), pos)
	frames := dataFrames(scanTerm(l.lb, 0))
	require.Len(t, frames, 2)
	assert.Equal(t, pattern(100, 1), frames[0].payload)
	assert.Equal(t, pattern(50, 2), frames[1].payload)
}

func TestPublicationErrorStringUnknownMatchesJava(t *testing.T) {
	assert.Equal(t, "UNKNOWN", PublicationErrorString(-6))
	assert.Equal(t, "UNKNOWN", PublicationErrorString(-1000))
	assert.Equal(t, "NONE", PublicationErrorString(0))
	assert.Equal(t, "MAX_POSITION_EXCEEDED", PublicationErrorString(MaxPositionExceeded))
}

func TestIsRevokedAfterCloseDoesNotReadLog(t *testing.T) {
	l := newTestLog(t, logbuffer.TermMinLength, 0, 1408)
	pub := newTestPublication(t, l, 1<<20)
	l.lb.Buffer(logbuffer.LogMetaDataSectionIndex).PutUInt8(logIsPublicationRevokedOffset, 1)
	assert.True(t, pub.IsRevoked())
	require.NoError(t, pub.Close())
	pub.logMetaData = nil
	assert.False(t, pub.IsRevoked())
}

// termModel is the Java 1.53.3 Concurrent/ExclusivePublication position arithmetic for a single writer.
type termModel struct {
	termLength, maxPayload, maxMessage int32
	termBegin, limit, maxPossible      int64
	termOffset                         int32
}

func (m *termModel) step(length, framed int32) (expected int64, wrote bool) {
	position := m.termBegin + int64(m.termOffset)
	if position >= m.limit {
		if position+int64(util.AlignInt32(length+logbuffer.DataFrameHeader.Length, logbuffer.FrameAlignment)) >=
			m.maxPossible {
			return MaxPositionExceeded, false
		}
		return BackPressured, false
	}
	resulting := int64(m.termOffset) + int64(framed)
	if resulting > int64(m.termLength) {
		if m.termBegin+int64(m.termLength) >= m.maxPossible {
			return MaxPositionExceeded, false
		}
		m.termBegin += int64(m.termLength)
		m.termOffset = 0
		return AdminAction, false
	}
	m.termOffset = int32(resulting)
	return m.termBegin + resulting, true
}

func runTermBoundaryFuzz(t *testing.T, seed int64, initialTermID int32, countSel uint8, startUnits uint16,
	mtuSel uint8, exclusive bool) {
	termLength := logbuffer.TermMinLength
	mtu := []int32{1408, 4096, 8192, 32}[mtuSel%4]
	if mtu == 32 {
		mtu = 64
	}
	var termCount int32
	if countSel&0x80 != 0 {
		termCount = int32(1<<31-1) - int32(countSel%3)
	} else {
		termCount = int32(countSel % 6)
	}
	startOffset := (int32(startUnits) * logbuffer.FrameAlignment) % termLength
	l := newTestLog(t, termLength, initialTermID, mtu)
	meta := l.lb.Buffer(logbuffer.LogMetaDataSectionIndex)
	for k := int32(0); k < logbuffer.PartitionCount; k++ {
		c := termCount - k
		idx := ((c % 3) + 3) % 3
		raw := int64(initialTermID+c) << 32
		if k == 0 {
			raw |= int64(startOffset)
		}
		meta.PutInt64(termTailCountersOffset+idx*8, raw)
	}
	meta.PutInt32(logActiveTermCountOffset, termCount)

	rng := rand.New(rand.NewSource(seed))
	termBegin := int64(termCount) << util.NumberOfTrailingZeroes(uint32(termLength))
	m := &termModel{
		termLength: termLength, maxPayload: mtu - logbuffer.DataFrameHeader.Length,
		maxMessage: logbuffer.ComputeMaxMessageLength(termLength),
		termBegin:  termBegin, termOffset: startOffset,
		limit:       termBegin + int64(startOffset) + rng.Int63n(4*int64(termLength)),
		maxPossible: int64(termLength) << 31,
	}

	var pub Publisher
	var ex *ExclusivePublication
	if exclusive {
		ex = newTestExclusivePublication(t, l, m.limit)
		pub = ex
	} else {
		pub = newTestPublication(t, l, m.limit)
	}
	require.Equal(t, m.maxPossible, pub.MaxPossiblePosition())
	require.Equal(t, m.termBegin+int64(m.termOffset), pub.Position())

	var claim BufferClaim
	src := pattern(int(m.maxMessage), byte(seed))
	srcBuf := bufOf(src)
	hdrLen := logbuffer.DataFrameHeader.Length
	for i := 0; i < 400; i++ {
		kinds := 4
		if exclusive {
			kinds = 5
		}
		kind := rng.Intn(kinds)
		var length int32
		switch {
		case kind == 3:
			length = rng.Int31n(m.maxPayload + 1)
		case rng.Intn(4) == 0:
			length = rng.Int31n(m.maxMessage + 1)
		default:
			length = rng.Int31n(m.maxPayload*2 + 1)
		}
		if length > m.maxMessage {
			length = m.maxMessage
		}
		if kind == 4 {
			length %= term.ComputeFragmentedFrameLength(m.maxMessage, m.maxPayload) + 1
		}
		framed := util.AlignInt32(length+hdrLen, logbuffer.FrameAlignment)
		if kind <= 2 && length > m.maxPayload {
			framed = term.ComputeFragmentedFrameLength(length, m.maxPayload)
		}
		prevBegin, prevOffset := m.termBegin, m.termOffset
		partition := int32(((prevBegin >> util.NumberOfTrailingZeroes(uint32(termLength))) % 3))
		termID := initialTermID + int32(prevBegin>>util.NumberOfTrailingZeroes(uint32(termLength)))
		expected, wrote := m.step(length, framed)

		var got int64
		switch kind {
		case 0:
			got = pub.Offer(srcBuf, 0, length, nil)
		case 1:
			split := rng.Int31n(length + 1)
			got = pub.Offer2(srcBuf, 0, split, srcBuf, split, length-split, nil)
		case 2:
			split := rng.Int31n(length + 1)
			got = pub.OfferVectors([]DirectBufferVector{
				{Buffer: srcBuf, Offset: 0, Length: split},
				{Buffer: srcBuf, Offset: split, Length: length - split},
			}, nil)
		case 3:
			got = pub.TryClaim(length, &claim)
			if got > 0 {
				claim.PutBytes(srcBuf, 0, length)
				claim.Commit()
			}
		case 4:
			got = ex.AppendPadding(length)
		}
		require.Equalf(t, expected, got, "op %d kind %d length %d at begin %d offset %d", i, kind, length,
			prevBegin, prevOffset)

		termBuf := l.lb.Buffer(int(partition))
		switch {
		case wrote:
			require.Equal(t, termID, termBuf.GetInt32(prevOffset+logbuffer.DataFrameHeader.TermIDFieldOffset))
			require.Equal(t, prevOffset, termBuf.GetInt32(prevOffset+logbuffer.DataFrameHeader.TermOffsetFieldOffset))
			require.Equal(t, pubTestSessionID, termBuf.GetInt32(prevOffset+logbuffer.DataFrameHeader.SessionIDFieldOffset))
			if kind != 4 {
				var payload []byte
				for off := prevOffset; off < prevOffset+framed; {
					frameLength := termBuf.GetInt32(off)
					require.Equal(t, termID, termBuf.GetInt32(off+logbuffer.DataFrameHeader.TermIDFieldOffset))
					payload = append(payload, termBuf.GetBytesArray(off+hdrLen, frameLength-hdrLen)...)
					off += util.AlignInt32(frameLength, logbuffer.FrameAlignment)
				}
				require.True(t, bytes.Equal(src[:length], payload), "payload mismatch op %d", i)
			} else {
				require.Equal(t, logbuffer.DataFrameHeader.TypePad,
					termBuf.GetUInt16(prevOffset+logbuffer.DataFrameHeader.TypeFieldOffset))
			}
		case expected == AdminAction && prevOffset < termLength:
			require.Equal(t, termLength-prevOffset, termBuf.GetInt32(prevOffset))
			require.Equal(t, logbuffer.DataFrameHeader.TypePad,
				termBuf.GetUInt16(prevOffset+logbuffer.DataFrameHeader.TypeFieldOffset))
		}
		if expected == MaxPositionExceeded && !wrote && prevBegin+int64(prevOffset) < m.limit {
			return
		}
		require.Equal(t, m.termBegin+int64(m.termOffset), pub.Position())
		require.Equal(t, m.limit-pub.Position(), pub.AvailableWindow())
		if expected == AdminAction || rng.Intn(16) == 0 {
			m.limit = m.termBegin + int64(m.termOffset) + rng.Int63n(2*int64(termLength))
			l.counters.PutInt64(0, m.limit)
		}
	}
}

func FuzzPublicationTermBoundary(f *testing.F) {
	for i, initial := range []int32{0, 1, -7, 1<<31 - 2, -1 << 31} {
		for _, sel := range []uint8{0, 2, 5, 0x80, 0x81, 0x82} {
			f.Add(int64(i)*31+int64(sel), initial, sel, uint16(i*517+int(sel)), uint8(i), i%2 == 0)
		}
	}
	f.Fuzz(func(t *testing.T, seed int64, initialTermID int32, countSel uint8, startUnits uint16, mtuSel uint8,
		exclusive bool) {
		runTermBoundaryFuzz(t, seed, initialTermID, countSel, startUnits, mtuSel, exclusive)
	})
}
