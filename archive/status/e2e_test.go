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

package status_test

import (
	"fmt"
	"math"
	"math/rand"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/lirm/aeron-go/aeron"
	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/counters"
	"github.com/lirm/aeron-go/aeron/logbuffer"
	"github.com/lirm/aeron-go/archive"
	"github.com/lirm/aeron-go/archive/status"
	"github.com/stretchr/testify/require"
)

// Runs against the Java 1.53.3 ArchivingMediaDriver from docker/docker-compose.yml (archive id 1):
//
//	docker compose -f docker/docker-compose.yml up -d --build archiving-driver
//	AERON_E2E=1 AERON_DIR=/dev/shm/aeron-compose go test ./archive/status -run E2E -v
const e2eArchiveId = int64(1)

func e2eArchive(t *testing.T) *archive.Archive {
	if os.Getenv("AERON_E2E") != "1" {
		t.Skip("set AERON_E2E=1 to run against a live archiving media driver")
	}
	dir := os.Getenv("AERON_DIR")
	if dir == "" {
		dir = "/dev/shm/aeron-compose"
	}
	ctx := aeron.NewContext().AeronDir(dir)
	arch, err := archive.NewArchive(archive.DefaultOptions(), ctx)
	require.NoError(t, err)
	t.Cleanup(func() { _ = arch.Close() })
	return arch
}

func await(t *testing.T, what string, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

func offerAll(t *testing.T, pub *aeron.Publication, from, count int) {
	t.Helper()
	buf := atomic.MakeBuffer(make([]byte, 64))
	for i := from; i < from+count; i++ {
		msg := []byte(fmt.Sprintf("msg-%06d", i))
		buf.PutBytesArray(0, &msg, 0, int32(len(msg)))
		await(t, "offer", 5*time.Second, func() bool { return pub.Offer(buf, 0, int32(len(msg)), nil) > 0 })
	}
}

func TestE2ERecordingPosAndBoundedReplay(t *testing.T) {
	arch := e2eArchive(t)
	reader := arch.Aeron().CounterReader()

	// Archive-wide counters keyed by archive id.
	controlSessions := status.FindArchiveCounter(reader, status.ArchiveControlSessionsTypeId, e2eArchiveId)
	require.NotEqual(t, counters.NullCounterId, controlSessions, "control sessions counter")
	require.GreaterOrEqual(t, reader.GetCounterValue(controlSessions), int64(1))
	// ArchivingMediaDriver shares the driver's error counter, so no ArchiveErrorCountTypeId counter exists here.
	recSessions := status.FindArchiveCounter(reader, status.ArchiveRecordingSessionCountTypeId, e2eArchiveId)
	require.NotEqual(t, counters.NullCounterId, recSessions, "recording sessions counter")
	require.Equal(t, e2eArchiveId, status.GetArchiveCounterArchiveId(reader, recSessions, status.ArchiveRecordingSessionCountTypeId))
	require.Equal(t, counters.NullCounterId, status.FindArchiveCounter(reader, status.ArchiveRecordingSessionCountTypeId, e2eArchiveId+1))
	recSessionsBefore := reader.GetCounterValue(recSessions)

	// Our own control session's per-client counter (ControlSessionCounter, since 1.49).
	sessCounter := status.FindControlSessionCounter(reader, e2eArchiveId, arch.SessionID)
	require.NotEqual(t, counters.NullCounterId, sessCounter, "control session counter for %d", arch.SessionID)
	csid, ok := status.GetControlSessionId(reader, sessCounter)
	require.True(t, ok)
	require.Equal(t, arch.SessionID, csid)
	t.Logf("archive counters: control-sessions=%d (value %d) recording-sessions=%d control-session=%d label=%q",
		controlSessions, reader.GetCounterValue(controlSessions), recSessions, sessCounter, labelOf(reader, sessCounter))

	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	stream := int32(5000 + r.Intn(1000))
	port := 21000 + r.Intn(1000)
	channel := fmt.Sprintf("aeron:udp?endpoint=localhost:%d", port)

	_, err := arch.StartRecording(channel, stream, false, true)
	require.NoError(t, err)
	t.Cleanup(func() { _ = arch.StopRecording(channel, stream) })

	pub, err := arch.AddPublication(channel, stream)
	require.NoError(t, err)
	defer pub.Close()
	await(t, "publication connected", 10*time.Second, pub.IsConnected)

	// The archive allocates the RecordingPos counter once the recorded image is available.
	var counterId int32
	await(t, "RecordingPos counter", 10*time.Second, func() bool {
		counterId = status.FindCounterIdBySession(reader, pub.SessionID(), e2eArchiveId)
		return counterId != counters.NullCounterId
	})
	recordingId := status.GetRecordingId(reader, counterId)
	require.NotEqual(t, status.NullRecordingId, recordingId)
	require.Equal(t, counterId, status.FindCounterIdByRecording(reader, recordingId, e2eArchiveId))
	require.Equal(t, counterId, status.FindCounterIdByRecording(reader, recordingId, status.NullValue))
	require.Equal(t, counters.NullCounterId, status.FindCounterIdByRecording(reader, recordingId, e2eArchiveId+1))
	require.Equal(t, counterId, archive.FindCounterIdBySession(reader, pub.SessionID()))
	require.Equal(t, e2eArchiveId, status.GetArchiveId(reader, counterId))
	sid, ok := status.GetSessionId(reader, counterId)
	require.True(t, ok)
	require.Equal(t, pub.SessionID(), sid)
	src, ok := status.GetSourceIdentity(reader, counterId)
	require.True(t, ok)
	require.True(t, strings.HasPrefix(src, "127.0.0.1:"), "source identity %q", src)
	require.True(t, status.IsActive(reader, counterId, recordingId))
	require.Equal(t, recSessionsBefore+1, reader.GetCounterValue(recSessions))
	label := labelOf(reader, counterId)
	require.True(t, strings.HasPrefix(label, fmt.Sprintf("%s: %d %d %d ", status.RecordingPosName, recordingId, pub.SessionID(), stream)), label)
	require.True(t, strings.HasSuffix(label, status.ArchiveIdLabel(e2eArchiveId)), label)
	t.Logf("rec-pos counter %d: recordingId=%d sourceIdentity=%s label=%q", counterId, recordingId, src, label)

	// Watch the counter advance as data is published.
	const batch = 100
	prev := reader.GetCounterValue(counterId)
	for i := 0; i < 3; i++ {
		offerAll(t, pub, i*batch, batch)
		target := pub.Position()
		await(t, "rec-pos to reach publication position", 10*time.Second, func() bool {
			return reader.GetCounterValue(counterId) >= target
		})
		v := reader.GetCounterValue(counterId)
		require.Greater(t, v, prev)
		t.Logf("after %d msgs rec-pos=%d (publication position %d)", (i+1)*batch, v, target)
		prev = v
	}

	// Bounded replay limited by the live RecordingPos counter follows the recording as it grows.
	replayStream := stream + 1
	replayChannel := fmt.Sprintf("aeron:udp?endpoint=localhost:%d", port+1)
	sub, err := arch.AddSubscription(replayChannel, replayStream)
	require.NoError(t, err)
	defer sub.Close()

	received := 0
	var lastMsg string
	handler := func(buffer *atomic.Buffer, offset int32, length int32, header *logbuffer.Header) {
		lastMsg = string(buffer.GetBytesArray(offset, length))
		require.Equal(t, fmt.Sprintf("msg-%06d", received), lastMsg)
		received++
	}
	replaySessionId, err := arch.BoundedReplay(recordingId, 0, math.MaxInt64, counterId, replayStream, replayChannel)
	require.NoError(t, err)
	defer func() { _ = arch.StopReplay(replaySessionId) }()

	pollUntil := func(n int) {
		await(t, fmt.Sprintf("%d replayed messages (have %d)", n, received), 10*time.Second, func() bool {
			sub.Poll(handler, 100)
			return received >= n
		})
	}
	pollUntil(3 * batch)
	offerAll(t, pub, 3*batch, batch)
	pollUntil(4 * batch)
	t.Logf("bounded replay (limit=rec-pos %d) delivered %d msgs, last %q", counterId, received, lastMsg)

	// Recording stops -> counter freed -> no longer active.
	require.NoError(t, arch.StopRecording(channel, stream))
	await(t, "rec-pos counter freed", 10*time.Second, func() bool { return !status.IsActive(reader, counterId, recordingId) })
	require.Equal(t, counters.NullCounterId, status.FindCounterIdByRecording(reader, recordingId, e2eArchiveId))
	stopPos, err := arch.GetStopPosition(recordingId)
	require.NoError(t, err)
	require.Equal(t, int64(pub.Position()), stopPos)
}

// A bounded replay limited by a client-owned counter only delivers data up to the counter's value.
func TestE2EBoundedReplayStopsAtLimitCounter(t *testing.T) {
	arch := e2eArchive(t)
	a := arch.Aeron()
	reader := a.CounterReader()

	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	stream := int32(6000 + r.Intn(1000))
	port := 22000 + r.Intn(1000)
	channel := fmt.Sprintf("aeron:udp?endpoint=localhost:%d", port)
	_, err := arch.StartRecording(channel, stream, false, true)
	require.NoError(t, err)
	t.Cleanup(func() { _ = arch.StopRecording(channel, stream) })
	pub, err := arch.AddPublication(channel, stream)
	require.NoError(t, err)
	defer pub.Close()
	await(t, "publication connected", 10*time.Second, pub.IsConnected)

	var recCounter int32
	await(t, "RecordingPos counter", 10*time.Second, func() bool {
		recCounter = status.FindCounterIdBySession(reader, pub.SessionID(), e2eArchiveId)
		return recCounter != counters.NullCounterId
	})
	recordingId := status.GetRecordingId(reader, recCounter)

	const total = 200
	offerAll(t, pub, 0, 100)
	halfway := pub.Position()
	offerAll(t, pub, 100, 100)
	end := pub.Position()
	await(t, "recorded", 10*time.Second, func() bool { return reader.GetCounterValue(recCounter) >= end })

	regId, err := a.AsyncAddCounterByLabel(1001, "go-e2e replay limit")
	require.NoError(t, err)
	var limit *aeron.Counter
	await(t, "limit counter", 5*time.Second, func() bool {
		limit, err = a.FindCounter(regId)
		require.NoError(t, err)
		return limit != nil
	})
	defer limit.Close()
	limit.Counter().Set(int64(halfway))

	replayStream := stream + 1
	replayChannel := fmt.Sprintf("aeron:udp?endpoint=localhost:%d", port+1)
	sub, err := arch.AddSubscription(replayChannel, replayStream)
	require.NoError(t, err)
	defer sub.Close()
	received := 0
	handler := func(buffer *atomic.Buffer, offset int32, length int32, header *logbuffer.Header) {
		require.Equal(t, fmt.Sprintf("msg-%06d", received), string(buffer.GetBytesArray(offset, length)))
		received++
	}
	replaySessionId, err := arch.BoundedReplay(recordingId, 0, math.MaxInt64, limit.Id(), replayStream, replayChannel)
	require.NoError(t, err)
	defer func() { _ = arch.StopReplay(replaySessionId) }()

	await(t, "first half", 10*time.Second, func() bool { sub.Poll(handler, 100); return received >= 100 })
	settle := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(settle) {
		sub.Poll(handler, 100)
	}
	require.Equal(t, 100, received, "replay must not pass the limit counter")

	limit.Counter().Set(int64(end))
	await(t, "second half", 10*time.Second, func() bool { sub.Poll(handler, 100); return received >= total })
	t.Logf("limit counter %d gated replay at %d msgs (pos %d) then released to %d (pos %d)", limit.Id(), 100, halfway, received, end)
}

func labelOf(reader *counters.Reader, counterId int32) string {
	var label string
	reader.Scan(func(c counters.Counter) {
		if c.Id == counterId {
			label = c.Label
		}
	})
	return label
}
