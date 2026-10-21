package status

import (
	"math"
	"strings"
	"testing"

	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/counters"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func label(buf *atomic.Buffer, offset, length int32) string {
	return string(buf.GetBytesArray(offset, length))
}

func TestEncodeRecordingPosLayout(t *testing.T) {
	buf := atomic.MakeBuffer(make([]byte, TempBufferLength))
	keyLength, labelOffset, labelLength, err := EncodeRecordingPos(
		buf, 5, 42, -7, 1001, "aeron:udp?endpoint=localhost:8010", "127.0.0.1:4000")
	require.NoError(t, err)
	// 8 recordingId + 4 sessionId + 4 length + 14 identity + 8 archiveId.
	assert.Equal(t, int32(38), keyLength)
	assert.Equal(t, int32(40), labelOffset)
	assert.Equal(t, int64(42), buf.GetInt64(0))
	assert.Equal(t, int32(-7), buf.GetInt32(8))
	assert.Equal(t, int32(14), buf.GetInt32(12))
	assert.Equal(t, "127.0.0.1:4000", label(buf, 16, 14))
	assert.Equal(t, int64(5), buf.GetInt64(30))
	assert.Equal(t, "rec-pos: 42 -7 1001 aeron:udp?endpoint=localhost:8010 - archiveId=5",
		label(buf, labelOffset, labelLength))

	// The encoded key is what the lookups decode.
	f := newFakeCounters(2)
	id := f.allocate(RecordingPositionTypeId, func(k *atomic.Buffer) { k.PutBytes(0, buf, 0, keyLength) })
	r := f.reader()
	assert.Equal(t, id, FindCounterIdByRecording(r, 42, 5))
	assert.Equal(t, id, FindCounterIdBySession(r, -7, 5))
	src, ok := GetSourceIdentity(r, id)
	assert.True(t, ok)
	assert.Equal(t, "127.0.0.1:4000", src)
	assert.Equal(t, int64(5), GetArchiveId(r, id))
}

func TestEncodeRecordingPosTruncation(t *testing.T) {
	buf := atomic.MakeBuffer(make([]byte, TempBufferLength))
	longIdentity := strings.Repeat("i", 200)
	longChannel := "aeron:udp?endpoint=localhost:8010|alias=" + strings.Repeat("c", 500)
	keyLength, labelOffset, labelLength, err := EncodeRecordingPos(
		buf, math.MinInt64, math.MaxInt64, math.MinInt32, math.MaxInt32, longChannel, longIdentity)
	require.NoError(t, err)
	assert.Equal(t, int32(counters.MaxKeyLength), keyLength)
	assert.Equal(t, MaxSourceIdentityLength, buf.GetInt32(SourceIdentityLengthOffset))
	assert.Equal(t, int64(math.MinInt64), buf.GetInt64(SourceIdentityOffset+MaxSourceIdentityLength))
	assert.Equal(t, int32(counters.MaxKeyLength), labelOffset)
	assert.Equal(t, int32(counters.MaxLabelLength), labelLength)
	l := label(buf, labelOffset, labelLength)
	assert.True(t, strings.HasPrefix(l, "rec-pos: 9223372036854775807 -2147483648 2147483647 aeron:udp?"), l)
	assert.True(t, strings.HasSuffix(l, "c - archiveId=-9223372036854775808"), l)
}

func TestEncodeRecordingPosNonASCII(t *testing.T) {
	buf := atomic.MakeBuffer(make([]byte, TempBufferLength))
	// Agrona putStringWithoutLengthAscii writes '?' per UTF-16 code unit above 127; "😀" is a surrogate pair.
	keyLength, labelOffset, labelLength, err := EncodeRecordingPos(buf, 1, 2, 3, 4, "aeron:ipc?alias=é", "h€st😀")
	require.NoError(t, err)
	assert.Equal(t, int32(6), buf.GetInt32(SourceIdentityLengthOffset))
	assert.Equal(t, "h?st??", label(buf, SourceIdentityOffset, 6))
	assert.Equal(t, int32(SourceIdentityOffset+6+8), keyLength)
	assert.Equal(t, "rec-pos: 2 3 4 aeron:ipc?alias=? - archiveId=1", label(buf, labelOffset, labelLength))
}

func TestEncodeRejectsSmallBuffer(t *testing.T) {
	small := atomic.MakeBuffer(make([]byte, TempBufferLength-1))
	_, _, _, err := EncodeRecordingPos(small, 1, 2, 3, 4, "aeron:ipc", "x")
	assert.Error(t, err)
	_, _, _, err = EncodeRecordingPos(nil, 1, 2, 3, 4, "aeron:ipc", "x")
	assert.Error(t, err)
	_, _, err = EncodeArchiveCounter(small, "n", 1)
	assert.Error(t, err)
}

func TestEncodeArchiveCounter(t *testing.T) {
	buf := atomic.MakeBuffer(make([]byte, TempBufferLength))
	keyLength, labelLength, err := EncodeArchiveCounter(buf, "Archive Recording Sessions", -23462384)
	require.NoError(t, err)
	assert.Equal(t, int32(8), keyLength)
	assert.Equal(t, int64(-23462384), buf.GetInt64(0))
	assert.Equal(t, "Archive Recording Sessions - archiveId=-23462384", label(buf, keyLength, labelLength))

	f := newFakeCounters(1)
	id := f.allocate(ArchiveRecordingSessionCountTypeId, func(k *atomic.Buffer) { k.PutBytes(0, buf, 0, keyLength) })
	assert.Equal(t, id, FindArchiveCounter(f.reader(), ArchiveRecordingSessionCountTypeId, -23462384))

	keyLength, labelLength, err = EncodeArchiveCounter(buf, strings.Repeat("n", 1000), 7)
	require.NoError(t, err)
	assert.Equal(t, int32(counters.MaxLabelLength), labelLength)
	assert.True(t, strings.HasSuffix(label(buf, keyLength, labelLength), "n - archiveId=7"))
}

// The type ids must match io.aeron.AeronCounters 1.53.3.
func TestArchiveTypeIds(t *testing.T) {
	ids := []int32{ArchiveRecordingPositionTypeId, ArchiveErrorCountTypeId, ArchiveControlSessionsTypeId,
		ArchiveMaxCycleTimeTypeId, ArchiveCycleTimeThresholdExceededTypeId, ArchiveRecorderMaxWriteTimeTypeId,
		ArchiveRecorderTotalWriteBytesTypeId, ArchiveRecorderTotalWriteTimeTypeId, ArchiveReplayerMaxReadTimeTypeId,
		ArchiveReplayerTotalReadBytesTypeId, ArchiveReplayerTotalReadTimeTypeId, ArchiveRecordingSessionCountTypeId,
		ArchiveReplaySessionCountTypeId, ArchiveControlSessionTypeId}
	for i, id := range ids {
		assert.Equal(t, int32(100+i), id)
	}
	assert.Equal(t, int32(100), RecordingPositionTypeId)
	assert.Equal(t, int32(113), ControlSessionTypeId)
	assert.Equal(t, int32(88), MaxSourceIdentityLength)
}
