package aeron

import (
	"path/filepath"
	"sync"
	"testing"
	"unsafe"

	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/logbuffer"
	"github.com/lirm/aeron-go/aeron/logbuffer/term"
	"github.com/lirm/aeron-go/aeron/util"
	"github.com/lirm/aeron-go/aeron/util/memmap"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	pubTestSessionID = int32(0x1234)
	pubTestStreamID  = int32(1001)
)

type testLog struct {
	lb       *logbuffer.LogBuffers
	counters *atomic.Buffer
}

// newTestLog creates a log buffer laid out as the Java driver would: tails initialised per
// LogBufferDescriptor.initialiseTailWithTermId, default header filled with session/stream.
func newTestLog(t testing.TB, termLength, initialTermID, mtu int32) *testLog {
	name := filepath.Join(t.TempDir(), "pub.logbuffer")
	logLength := int(termLength)*logbuffer.PartitionCount + int(logbuffer.LogMetaDataLength)
	mmap, err := memmap.NewFile(name, 0, logLength)
	require.NoError(t, err)
	ptr := unsafe.Pointer(uintptr(mmap.GetMemoryPtr()) + uintptr(logLength-int(logbuffer.LogMetaDataLength)))
	buf := atomic.MakeBuffer(ptr, logbuffer.LogMetaDataLength)
	var meta logbuffer.LogBufferMetaData
	meta.Wrap(buf, 0)
	meta.SetTermLength(termLength)
	meta.SetMtuLength(mtu)
	meta.SetInitialTermID(initialTermID)
	meta.SetPageSize(4096)
	meta.SetIsConnected(true)
	meta.SetRawTail(0, int64(initialTermID)<<32)
	for i := 1; i < logbuffer.PartitionCount; i++ {
		meta.SetRawTail(int32(i), int64(initialTermID+int32(i)-logbuffer.PartitionCount)<<32)
	}
	hdr := meta.DefaultFrameHeader.Get()
	hdr.PutUInt8(logbuffer.DataFrameHeader.VersionFieldOffset, 0)
	hdr.PutUInt8(logbuffer.DataFrameHeader.FlagsFieldOffset, 0xC0)
	hdr.PutUInt16(logbuffer.DataFrameHeader.TypeFieldOffset, logbuffer.DataFrameHeader.TypeData)
	hdr.PutInt32(logbuffer.DataFrameHeader.SessionIDFieldOffset, pubTestSessionID)
	hdr.PutInt32(logbuffer.DataFrameHeader.StreamIDFieldOffset, pubTestStreamID)
	require.NoError(t, mmap.Close())

	lb := logbuffer.Wrap(name)
	t.Cleanup(func() { _ = lb.Close() })
	return &testLog{lb: lb, counters: atomic.MakeBuffer(make([]byte, 4096))}
}

func (l *testLog) setPositionInTerm(termCount, termID, termOffset int32) {
	meta := l.lb.Buffer(logbuffer.LogMetaDataSectionIndex)
	meta.PutInt64(termTailCountersOffset+(termCount%logbuffer.PartitionCount)*8, int64(termID)<<32|int64(termOffset))
	meta.PutInt32(logActiveTermCountOffset, termCount)
}

func wire(base *publicationBase, l *testLog, limit int64) {
	base.sessionID = pubTestSessionID
	base.streamID = pubTestStreamID
	base.channelStatusIndicatorID = ChannelStatusNoIdAllocated
	base.pubLimit = NewPosition(l.counters, 0)
	base.pubLimit.set(limit)
}

func newTestPublication(t testing.TB, l *testLog, limit int64) *Publication {
	pub := NewPublication(l.lb)
	wire(&pub.publicationBase, l, limit)
	return pub
}

func newTestExclusivePublication(t testing.TB, l *testLog, limit int64) *ExclusivePublication {
	pub := NewExclusivePublication(l.lb)
	wire(&pub.publicationBase, l, limit)
	return pub
}

type frame struct {
	partition  int
	termOffset int32
	length     int32
	flags      uint8
	typ        uint16
	termID     int32
	sessionID  int32
	streamID   int32
	reserved   int64
	payload    []byte
}

func scanTerm(lb *logbuffer.LogBuffers, partition int) []frame {
	return scanTermFrom(lb, partition, 0)
}

func scanTermFrom(lb *logbuffer.LogBuffers, partition int, start int32) []frame {
	buf := lb.Buffer(partition)
	var frames []frame
	for offset := start; offset < buf.Capacity(); {
		length := buf.GetInt32Volatile(offset)
		if length <= 0 {
			break
		}
		hdr := &logbuffer.DataFrameHeader
		f := frame{
			partition:  partition,
			termOffset: buf.GetInt32(offset + hdr.TermOffsetFieldOffset),
			length:     length,
			flags:      buf.GetUInt8(offset + hdr.FlagsFieldOffset),
			typ:        buf.GetUInt16(offset + hdr.TypeFieldOffset),
			termID:     buf.GetInt32(offset + hdr.TermIDFieldOffset),
			sessionID:  buf.GetInt32(offset + hdr.SessionIDFieldOffset),
			streamID:   buf.GetInt32(offset + hdr.StreamIDFieldOffset),
			reserved:   buf.GetInt64(offset + hdr.ReservedValueFieldOffset),
			payload:    buf.GetBytesArray(offset+hdr.Length, length-hdr.Length),
		}
		frames = append(frames, f)
		offset += util.AlignInt32(length, logbuffer.FrameAlignment)
	}
	return frames
}

func dataFrames(frames []frame) []frame {
	var out []frame
	for _, f := range frames {
		if f.typ == logbuffer.DataFrameHeader.TypeData {
			out = append(out, f)
		}
	}
	return out
}

func reassemble(t *testing.T, frames []frame) [][]byte {
	var msgs [][]byte
	var cur []byte
	for _, f := range dataFrames(frames) {
		switch {
		case f.flags == 0xC0:
			msgs = append(msgs, f.payload)
		case f.flags == 0x80:
			cur = append([]byte{}, f.payload...)
		case f.flags == 0x40:
			msgs = append(msgs, append(cur, f.payload...))
			cur = nil
		case f.flags == 0:
			cur = append(cur, f.payload...)
		default:
			t.Fatalf("unexpected flags %x", f.flags)
		}
	}
	return msgs
}

func pattern(n int, seed byte) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = seed + byte(i*7)
	}
	return b
}

func bufOf(b []byte) *atomic.Buffer { return atomic.MakeBuffer(b) }

func aligned(payload int32) int64 {
	return int64(util.AlignInt32(payload+logbuffer.DataFrameHeader.Length, logbuffer.FrameAlignment))
}

func TestMetaDataLayoutMatchesJava(t *testing.T) {
	l := newTestLog(t, logbuffer.TermMinLength, 0, 1408)
	meta := l.lb.Buffer(logbuffer.LogMetaDataSectionIndex)
	// LogBufferDescriptor: LOG_DEFAULT_FRAME_HEADER_OFFSET = 320, LOG_IS_CONNECTED_OFFSET = 136.
	assert.Equal(t, pubTestSessionID, meta.GetInt32(320+logbuffer.DataFrameHeader.SessionIDFieldOffset))
	assert.Equal(t, int32(1), meta.GetInt32(logIsConnectedOffset))
	assert.Equal(t, int32(1408), meta.GetInt32(272))
}

func TestPublicationAccessors(t *testing.T) {
	l := newTestLog(t, logbuffer.TermMinLength, 7, 1408)
	pub := newTestPublication(t, l, 1024)
	assert.Equal(t, logbuffer.TermMinLength, pub.TermBufferLength())
	assert.Equal(t, int32(7), pub.InitialTermID())
	assert.Equal(t, int32(1408-32), pub.MaxPayloadLength())
	assert.Equal(t, logbuffer.TermMinLength/8, pub.MaxMessageLength())
	assert.Equal(t, int64(logbuffer.TermMinLength)<<31, pub.MaxPossiblePosition())
	assert.Equal(t, int32(16), pub.PositionBitsToShift())
	assert.Equal(t, int64(0), pub.Position())
	assert.Equal(t, int64(1024), pub.PositionLimit())
	assert.Equal(t, int64(1024), pub.AvailableWindow())
	assert.Equal(t, int32(0), pub.PositionLimitID())
	assert.True(t, pub.IsConnected())
	assert.False(t, pub.IsRevoked())
	assert.Equal(t, int64(ChannelStatusActive), pub.ChannelStatus())
	assert.Nil(t, pub.LocalSocketAddresses())

	l.lb.Buffer(logbuffer.LogMetaDataSectionIndex).PutUInt8(logIsPublicationRevokedOffset, 1)
	assert.True(t, pub.IsRevoked())

	require.NoError(t, pub.Close())
	assert.True(t, pub.IsClosed())
	assert.False(t, pub.IsConnected())
	assert.Equal(t, PublicationClosed, pub.Position())
	assert.Equal(t, PublicationClosed, pub.PositionLimit())
	assert.Equal(t, PublicationClosed, pub.AvailableWindow())
	assert.Equal(t, int64(ChannelStatusNoIdAllocated), pub.ChannelStatus())
	assert.Equal(t, PublicationClosed, pub.Offer(bufOf(make([]byte, 8)), 0, 8, nil))
	assert.Error(t, pub.AddDestination("aeron:udp?endpoint=localhost:1"))
}

func TestResultCodesMatchJava(t *testing.T) {
	assert.Equal(t, int64(-1), NotConnected)
	assert.Equal(t, int64(-2), BackPressured)
	assert.Equal(t, int64(-3), AdminAction)
	assert.Equal(t, int64(-4), PublicationClosed)
	assert.Equal(t, int64(-5), MaxPositionExceeded)
	assert.Equal(t, "CLOSED", PublicationErrorString(PublicationClosed))
	assert.Equal(t, "MAX_POSITION_EXCEEDED", PublicationErrorString(-5))
	assert.Equal(t, "NONE", PublicationErrorString(64))
}

func TestOfferUnfragmentedWritesJavaCompatibleFrame(t *testing.T) {
	l := newTestLog(t, logbuffer.TermMinLength, 3, 1408)
	pub := newTestPublication(t, l, 1<<20)
	msg := pattern(100, 1)
	supplier := func(termBuffer *atomic.Buffer, termOffset int32, length int32) int64 {
		return int64(termOffset)<<32 | int64(length)
	}

	pos := pub.Offer(bufOf(msg), 0, int32(len(msg)), supplier)
	assert.Equal(t, aligned(100), pos)
	assert.Equal(t, pos, pub.Position())
	pos2 := pub.Offer(bufOf(msg), 10, 50, nil)
	assert.Equal(t, aligned(100)+aligned(50), pos2)

	frames := scanTerm(l.lb, 0)
	require.Len(t, frames, 2)
	f := frames[0]
	assert.Equal(t, int32(132), f.length)
	assert.Equal(t, uint8(0xC0), f.flags)
	assert.Equal(t, logbuffer.DataFrameHeader.TypeData, f.typ)
	assert.Equal(t, int32(3), f.termID)
	assert.Equal(t, pubTestSessionID, f.sessionID)
	assert.Equal(t, pubTestStreamID, f.streamID)
	assert.Equal(t, int64(132), f.reserved)
	assert.Equal(t, msg, f.payload)
	assert.Equal(t, int32(aligned(100)), frames[1].termOffset)
	assert.Equal(t, int64(0), frames[1].reserved)
	assert.Equal(t, msg[10:60], frames[1].payload)
}

func TestOffer2AndVectorsConcatenate(t *testing.T) {
	for _, exclusive := range []bool{false, true} {
		l := newTestLog(t, logbuffer.TermMinLength, 0, 1408)
		var p Publisher
		if exclusive {
			p = newTestExclusivePublication(t, l, 1<<20)
		} else {
			p = newTestPublication(t, l, 1<<20)
		}
		a, b, c := pattern(30, 1), pattern(1500, 2), pattern(2000, 3)
		var want [][]byte
		require.Positive(t, p.Offer2(bufOf(a), 0, 30, bufOf(b), 5, 40, nil))
		want = append(want, append(append([]byte{}, a...), b[5:45]...))
		require.Positive(t, p.Offer2(bufOf(b), 0, 1500, bufOf(c), 0, 2000, nil))
		want = append(want, append(append([]byte{}, b...), c...))
		vectors := []DirectBufferVector{
			term.NewDirectBufferVector(bufOf(a), 0, 30),
			term.NewDirectBufferVector(bufOf(c), 100, 0),
			term.NewDirectBufferVector(bufOf(b), 0, 1500),
			term.NewDirectBufferVector(bufOf(c), 0, 2000),
		}
		require.Positive(t, p.OfferVectors(vectors[:1], nil))
		want = append(want, a)
		pos := p.OfferVectors(vectors, func(*atomic.Buffer, int32, int32) int64 { return 42 })
		require.Positive(t, pos)
		want = append(want, append(append(append([]byte{}, a...), b...), c...))
		assert.Equal(t, pos, p.Position())

		frames := scanTerm(l.lb, 0)
		assert.Equal(t, want, reassemble(t, frames))
		assert.Equal(t, int64(42), frames[len(frames)-1].reserved)
		assert.Equal(t, int64(42), frames[len(frames)-2].reserved)
		var end int64
		for _, f := range frames {
			end = int64(f.termOffset) + aligned(f.length-32)
		}
		assert.Equal(t, pos, end, "exclusive=%v", exclusive)
	}
}

func TestOfferFragmentsLargeMessage(t *testing.T) {
	l := newTestLog(t, logbuffer.TermMinLength, 0, 1408)
	pub := newTestPublication(t, l, 1<<20)
	msg := pattern(int(pub.MaxMessageLength()), 9)
	pos := pub.Offer(bufOf(msg), 0, int32(len(msg)), nil)
	require.Positive(t, pos)
	assert.Equal(t, int64(term.ComputeFragmentedFrameLength(int32(len(msg)), pub.MaxPayloadLength())), pos)
	frames := scanTerm(l.lb, 0)
	assert.Equal(t, uint8(0x80), frames[0].flags)
	assert.Equal(t, uint8(0x40), frames[len(frames)-1].flags)
	assert.Equal(t, [][]byte{msg}, reassemble(t, frames))
	assert.Panics(t, func() { pub.Offer(bufOf(make([]byte, len(msg)+1)), 0, int32(len(msg)+1), nil) })
	assert.Panics(t, func() { pub.Offer(bufOf(msg), 0, -1, nil) })
	assert.Panics(t, func() { pub.Offer2(bufOf(msg), 0, -1, bufOf(msg), 0, 1, nil) })
}

func TestTryClaimCommitAbortFlagsAndHeaderType(t *testing.T) {
	for _, exclusive := range []bool{false, true} {
		l := newTestLog(t, logbuffer.TermMinLength, 0, 1408)
		var p Publisher
		if exclusive {
			p = newTestExclusivePublication(t, l, 1<<20)
		} else {
			p = newTestPublication(t, l, 1<<20)
		}
		var claim BufferClaim
		pos := p.TryClaim(64, &claim)
		require.Equal(t, aligned(64), pos)
		assert.Equal(t, int32(64), claim.Length())
		assert.Equal(t, uint8(0xC0), claim.Flags())
		assert.Equal(t, logbuffer.DataFrameHeader.TypeData, claim.HeaderType())
		payload := pattern(64, 5)
		claim.PutBytes(bufOf(payload), 0, 64).PutReservedValue(77).SetFlags(0xC0)
		assert.Equal(t, int64(77), claim.ReservedValue())
		assert.Empty(t, scanTerm(l.lb, 0), "frame must not be visible before commit")
		claim.Commit()

		require.Positive(t, p.TryClaim(10, &claim))
		claim.Abort()
		require.Positive(t, p.TryClaim(10, &claim))
		claim.SetHeaderType(logbuffer.DataFrameHeader.TypeData)
		claim.Commit()

		frames := scanTerm(l.lb, 0)
		require.Len(t, frames, 3)
		assert.Equal(t, payload, frames[0].payload)
		assert.Equal(t, int64(77), frames[0].reserved)
		assert.Equal(t, logbuffer.DataFrameHeader.TypePad, frames[1].typ)
		assert.Equal(t, logbuffer.DataFrameHeader.TypeData, frames[2].typ)
		assert.Panics(t, func() { p.TryClaim(p.MaxPayloadLength()+1, &claim) })
	}
}

func TestBackPressureAndNotConnected(t *testing.T) {
	l := newTestLog(t, logbuffer.TermMinLength, 0, 1408)
	pub := newTestPublication(t, l, 0)
	msg := bufOf(make([]byte, 64))
	assert.Equal(t, BackPressured, pub.Offer(msg, 0, 64, nil))
	l.lb.Meta().SetIsConnected(false)
	assert.Equal(t, NotConnected, pub.Offer(msg, 0, 64, nil))
	var claim BufferClaim
	assert.Equal(t, NotConnected, pub.TryClaim(10, &claim))

	ex := newTestExclusivePublication(t, l, 0)
	assert.Equal(t, NotConnected, ex.Offer(msg, 0, 64, nil))
	assert.Equal(t, NotConnected, ex.AppendPadding(64))
	l.lb.Meta().SetIsConnected(true)
	assert.Equal(t, BackPressured, ex.Offer(msg, 0, 64, nil))
}

func TestConcurrentTermRotation(t *testing.T) {
	termLength := logbuffer.TermMinLength
	l := newTestLog(t, termLength, 10, 1408)
	pub := newTestPublication(t, l, 1<<40)
	msg := bufOf(pattern(1000, 0))
	var last int64
	var sawAdmin bool
	for i := 0; i < 100; i++ {
		pos := pub.Offer(msg, 0, 1000, nil)
		if pos == AdminAction {
			sawAdmin = true
			pos = pub.Offer(msg, 0, 1000, nil)
		}
		require.Positive(t, pos)
		require.Greater(t, pos, last)
		last = pos
	}
	assert.True(t, sawAdmin)
	assert.Equal(t, int32(1), pub.activeTermCount())
	frames := scanTerm(l.lb, 0)
	pad := frames[len(frames)-1]
	assert.Equal(t, logbuffer.DataFrameHeader.TypePad, pad.typ)
	assert.Equal(t, termLength, pad.termOffset+pad.length)
	next := scanTerm(l.lb, 1)
	assert.Equal(t, int32(11), next[0].termID)
	assert.Equal(t, int64(termLength)+int64(len(next))*aligned(1000), last)
}

func TestConcurrentOffersFromManyGoroutines(t *testing.T) {
	l := newTestLog(t, logbuffer.TermMinLength, 0, 1408)
	pub := newTestPublication(t, l, 1<<40)
	const writers, perWriter = 8, 100
	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			buf := bufOf(make([]byte, 64))
			var claim BufferClaim
			for i := 0; i < perWriter; i++ {
				buf.PutInt32(0, int32(w))
				buf.PutInt32(4, int32(i))
				for {
					var pos int64
					if i%2 == 0 {
						pos = pub.Offer(buf, 0, 64, nil)
					} else if pos = pub.TryClaim(64, &claim); pos > 0 {
						claim.PutBytes(buf, 0, 64)
						claim.Commit()
					}
					if pos > 0 {
						break
					}
					require.Equal(t, AdminAction, pos)
				}
			}
		}(w)
	}
	wg.Wait()

	seen := map[[2]int32]bool{}
	for p := 0; p < 2; p++ {
		for _, f := range dataFrames(scanTerm(l.lb, p)) {
			key := [2]int32{int32(f.payload[0]), int32(f.payload[4])}
			require.False(t, seen[key], "duplicate %v", key)
			seen[key] = true
		}
	}
	assert.Len(t, seen, writers*perWriter)
	assert.Equal(t, int64(writers*perWriter)*aligned(64)+paddingBytes(l.lb), pub.Position())
}

func paddingBytes(lb *logbuffer.LogBuffers) int64 {
	var n int64
	for _, f := range scanTerm(lb, 0) {
		if f.typ == logbuffer.DataFrameHeader.TypePad {
			n += int64(f.length)
		}
	}
	return n
}

func TestExclusiveInitialPositionAndRotation(t *testing.T) {
	termLength := logbuffer.TermMinLength
	l := newTestLog(t, termLength, 100, 1408)
	l.setPositionInTerm(5, 105, termLength-256)
	pub := newTestExclusivePublication(t, l, 1<<40)
	assert.Equal(t, int32(105), pub.TermID())
	assert.Equal(t, termLength-256, pub.TermOffset())
	start := 5*int64(termLength) + int64(termLength-256)
	assert.Equal(t, start, pub.Position())

	msg := bufOf(pattern(100, 4))
	pos := pub.Offer(msg, 0, 100, nil)
	assert.Equal(t, start+aligned(100), pos)
	assert.Equal(t, AdminAction, pub.Offer(msg, 0, 100, nil))
	assert.Equal(t, int32(106), pub.TermID())
	assert.Equal(t, int32(6), pub.activeTermCount())
	assert.Equal(t, 6*int64(termLength), pub.Position())
	pos = pub.Offer(msg, 0, 100, nil)
	assert.Equal(t, 6*int64(termLength)+aligned(100), pos)

	old := scanTermFrom(l.lb, 5%3, termLength-256)
	require.Len(t, old, 2)
	assert.Equal(t, termLength-256, old[0].termOffset)
	assert.Equal(t, logbuffer.DataFrameHeader.TypePad, old[1].typ)
	assert.Equal(t, termLength, old[1].termOffset+old[1].length)
	cur := scanTerm(l.lb, 6%3)
	require.Len(t, cur, 1)
	assert.Equal(t, int32(106), cur[0].termID)
	assert.Equal(t, int64(106)<<32|aligned(100), l.lb.Buffer(logbuffer.LogMetaDataSectionIndex).GetInt64(0))
}

func TestExclusiveAppendPadding(t *testing.T) {
	l := newTestLog(t, logbuffer.TermMinLength, 0, 1408)
	pub := newTestExclusivePublication(t, l, 1<<40)
	pos := pub.AppendPadding(200)
	assert.Equal(t, aligned(200), pos)
	pos = pub.Offer(bufOf(pattern(10, 0)), 0, 10, nil)
	assert.Equal(t, aligned(200)+aligned(10), pos)
	frames := scanTerm(l.lb, 0)
	require.Len(t, frames, 2)
	assert.Equal(t, logbuffer.DataFrameHeader.TypePad, frames[0].typ)
	assert.Equal(t, int32(232), frames[0].length)
	assert.Panics(t, func() { pub.AppendPadding(logbuffer.TermMinLength) })
}

func buildBlock(pub *ExclusivePublication, payloads ...[]byte) []byte {
	var block []byte
	offset := pub.TermOffset()
	for _, p := range payloads {
		fl := int32(len(p)) + logbuffer.DataFrameHeader.Length
		frameBytes := make([]byte, aligned(int32(len(p))))
		b := bufOf(frameBytes)
		hdr := &logbuffer.DataFrameHeader
		b.PutInt32(hdr.FrameLengthFieldOffset, fl)
		b.PutUInt8(hdr.FlagsFieldOffset, 0xC0)
		b.PutUInt16(hdr.TypeFieldOffset, hdr.TypeData)
		b.PutInt32(hdr.TermOffsetFieldOffset, offset)
		b.PutInt32(hdr.SessionIDFieldOffset, pub.SessionID())
		b.PutInt32(hdr.StreamIDFieldOffset, pub.StreamID())
		b.PutInt32(hdr.TermIDFieldOffset, pub.TermID())
		copy(frameBytes[hdr.Length:], p)
		block = append(block, frameBytes...)
		offset += int32(len(frameBytes))
	}
	return block
}

func TestExclusiveOfferBlock(t *testing.T) {
	l := newTestLog(t, logbuffer.TermMinLength, 0, 1408)
	pub := newTestExclusivePublication(t, l, 1<<40)
	require.Positive(t, pub.Offer(bufOf(pattern(40, 1)), 0, 40, nil))
	a, b := pattern(100, 2), pattern(7, 3)
	block := buildBlock(pub, a, b)
	blockBuf := bufOf(block)
	before := pub.Position()
	pos := pub.OfferBlock(blockBuf, 0, int32(len(block)))
	assert.Equal(t, before+int64(len(block)), pos)
	assert.Equal(t, int32(132), blockBuf.GetInt32(0), "source first frame length restored")
	frames := scanTerm(l.lb, 0)
	require.Len(t, frames, 3)
	assert.Equal(t, a, frames[1].payload)
	assert.Equal(t, b, frames[2].payload)

	bad := buildBlock(pub, a)
	bufOf(bad).PutInt32(logbuffer.DataFrameHeader.SessionIDFieldOffset, 999)
	assert.Panics(t, func() { pub.OfferBlock(bufOf(bad), 0, int32(len(bad))) })
	tooLong := make([]byte, logbuffer.TermMinLength)
	copy(tooLong, buildBlock(pub, a))
	assert.Panics(t, func() { pub.OfferBlock(bufOf(tooLong), 0, int32(len(tooLong))) })
}

func TestMaxPositionExceeded(t *testing.T) {
	termLength := logbuffer.TermMinLength
	l := newTestLog(t, termLength, 0, 1408)
	lastTermID := int32(1<<31 - 1)
	l.setPositionInTerm(lastTermID, lastTermID, termLength-64)
	ex := newTestExclusivePublication(t, l, 1<<62)
	msg := bufOf(make([]byte, 100))
	assert.Equal(t, MaxPositionExceeded, ex.Offer(msg, 0, 100, nil))

	pub := newTestPublication(t, l, 1<<62)
	assert.Equal(t, MaxPositionExceeded, pub.Offer(msg, 0, 100, nil))
}

func TestRevokeOnClose(t *testing.T) {
	l := newTestLog(t, logbuffer.TermMinLength, 0, 1408)
	ex := newTestExclusivePublication(t, l, 1<<20)
	ex.RevokeOnClose()
	assert.True(t, ex.revokeOnClose)
	require.NoError(t, ex.Revoke())
	assert.True(t, ex.IsClosed())
	assert.NoError(t, ex.Revoke())
}

func TestOfferDoesNotAllocate(t *testing.T) {
	l := newTestLog(t, logbuffer.TermMinLength, 0, 1408)
	pub := newTestPublication(t, l, 1<<62)
	ex := newTestExclusivePublication(t, newTestLog(t, logbuffer.TermMinLength, 0, 1408), 1<<62)
	small := bufOf(make([]byte, 256))
	large := bufOf(make([]byte, 4000))
	vectors := []DirectBufferVector{term.NewDirectBufferVector(small, 0, 100), term.NewDirectBufferVector(large, 0, 3000)}
	var claim BufferClaim
	cases := map[string]func(){
		"Offer":            func() { pub.Offer(small, 0, 100, nil) },
		"OfferFragmented":  func() { pub.Offer(large, 0, 4000, nil) },
		"Offer2":           func() { pub.Offer2(small, 0, 100, small, 0, 50, nil) },
		"OfferVectors":     func() { pub.OfferVectors(vectors, nil) },
		"TryClaim":         func() { tryClaimCommit(pub, &claim) },
		"Position":         func() { pub.Position() },
		"ExclusiveOffer":   func() { ex.Offer(small, 0, 100, nil) },
		"ExclusiveVectors": func() { ex.OfferVectors(vectors, nil) },
		"ExclusiveClaim":   func() { tryClaimCommit(ex, &claim) },
	}
	for name, fn := range cases {
		assert.Zero(t, testing.AllocsPerRun(500, fn), name)
	}
}

func tryClaimCommit(p Publisher, claim *BufferClaim) {
	if p.TryClaim(64, claim) > 0 {
		claim.Commit()
	}
}

func BenchmarkPublicationOffer(b *testing.B) {
	l := newTestLog(b, logbuffer.TermMinLength, 0, 1408)
	pub := newTestPublication(b, l, 1<<62)
	src := bufOf(make([]byte, 64))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pub.Offer(src, 0, 64, nil)
	}
}

func BenchmarkExclusivePublicationOffer(b *testing.B) {
	l := newTestLog(b, logbuffer.TermMinLength, 0, 1408)
	pub := newTestExclusivePublication(b, l, 1<<62)
	src := bufOf(make([]byte, 64))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pub.Offer(src, 0, 64, nil)
	}
}

func BenchmarkPublicationTryClaim(b *testing.B) {
	l := newTestLog(b, logbuffer.TermMinLength, 0, 1408)
	pub := newTestPublication(b, l, 1<<62)
	var claim BufferClaim
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tryClaimCommit(pub, &claim)
	}
}
