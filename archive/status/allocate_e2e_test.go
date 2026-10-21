package status_test

import (
	"fmt"
	"math/rand"
	"strings"
	"testing"
	"time"

	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/counters"
	"github.com/lirm/aeron-go/archive/status"
	"github.com/stretchr/testify/require"
)

// The Go encoder produces the same key bytes and label as the Java archive's RecordingPos.allocate, and counters
// allocated by the Go client are found by the lookups.
func TestE2EAllocateMatchesJavaArchive(t *testing.T) {
	arch := e2eArchive(t)
	a := arch.Aeron()
	reader := a.CounterReader()

	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	stream := int32(7000 + r.Intn(1000))
	port := 23000 + r.Intn(1000)
	channel := fmt.Sprintf("aeron:udp?endpoint=localhost:%d|alias=status-e2e", port)
	_, err := arch.StartRecording(channel, stream, false, true)
	require.NoError(t, err)
	t.Cleanup(func() { _ = arch.StopRecording(channel, stream) })
	pub, err := arch.AddPublication(channel, stream)
	require.NoError(t, err)
	defer pub.Close()
	await(t, "publication connected", 10*time.Second, pub.IsConnected)

	var javaId int32
	await(t, "RecordingPos counter", 10*time.Second, func() bool {
		javaId = status.FindCounterIdBySession(reader, pub.SessionID(), e2eArchiveId)
		return javaId != counters.NullCounterId
	})
	recordingId := status.GetRecordingId(reader, javaId)
	src, ok := status.GetSourceIdentity(reader, javaId)
	require.True(t, ok)
	javaLabel := labelOf(reader, javaId)
	prefix := fmt.Sprintf("rec-pos: %d %d %d ", recordingId, pub.SessionID(), stream)
	suffix := status.ArchiveIdLabel(e2eArchiveId)
	require.True(t, strings.HasPrefix(javaLabel, prefix) && strings.HasSuffix(javaLabel, suffix), javaLabel)
	strippedChannel := javaLabel[len(prefix) : len(javaLabel)-len(suffix)]

	buf := atomic.MakeBuffer(make([]byte, status.TempBufferLength))
	keyLength, labelOffset, labelLength, err := status.EncodeRecordingPos(
		buf, e2eArchiveId, recordingId, pub.SessionID(), stream, strippedChannel, src)
	require.NoError(t, err)
	javaKey := reader.MetaDataBuffer().GetBytesArray(counters.MetaDataOffset(javaId)+counters.KeyOffset, keyLength)
	require.Equal(t, javaKey, buf.GetBytesArray(0, keyLength), "key bytes")
	require.Equal(t, javaLabel, string(buf.GetBytesArray(labelOffset, labelLength)), "label")

	// Counters allocated through the Go client under an unused archive id.
	fakeArchiveId := int64(1_000_000 + r.Intn(1_000_000))
	fakeRecordingId := int64(r.Int31())
	recPos, err := status.AllocateRecordingPos(a, nil, fakeArchiveId, fakeRecordingId, 4242, 77, "aeron:ipc", "go-src")
	require.NoError(t, err)
	defer recPos.Close()
	require.Equal(t, recPos.Id(), status.FindCounterIdByRecording(reader, fakeRecordingId, fakeArchiveId))
	require.Equal(t, recPos.Id(), status.FindCounterIdBySession(reader, 4242, fakeArchiveId))
	require.Equal(t, fakeArchiveId, status.GetArchiveId(reader, recPos.Id()))
	goSrc, ok := status.GetSourceIdentity(reader, recPos.Id())
	require.True(t, ok)
	require.Equal(t, "go-src", goSrc)
	require.Equal(t, fmt.Sprintf("rec-pos: %d 4242 77 aeron:ipc - archiveId=%d", fakeRecordingId, fakeArchiveId),
		labelOf(reader, recPos.Id()))
	require.True(t, status.IsActive(reader, recPos.Id(), fakeRecordingId))

	sessions, err := status.AllocateArchiveCounter(a, nil, status.ArchiveRecordingSessionCountTypeId,
		"Archive Recording Sessions", fakeArchiveId)
	require.NoError(t, err)
	defer sessions.Close()
	require.Equal(t, sessions.Id(), status.FindArchiveCounter(reader, status.ArchiveRecordingSessionCountTypeId, fakeArchiveId))
	require.Equal(t, fmt.Sprintf("Archive Recording Sessions - archiveId=%d", fakeArchiveId), labelOf(reader, sessions.Id()))
	javaSessions := status.FindArchiveCounter(reader, status.ArchiveRecordingSessionCountTypeId, e2eArchiveId)
	require.Equal(t, "Archive Recording Sessions"+suffix, labelOf(reader, javaSessions))

	require.NoError(t, recPos.Close())
	await(t, "go rec-pos freed", 5*time.Second, func() bool { return !status.IsActive(reader, recPos.Id(), fakeRecordingId) })
	require.Equal(t, counters.NullCounterId, status.FindCounterIdByRecording(reader, fakeRecordingId, fakeArchiveId))
}
