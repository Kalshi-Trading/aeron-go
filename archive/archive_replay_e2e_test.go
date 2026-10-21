package archive

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/lirm/aeron-go/aeron"
	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/idlestrategy"
	"github.com/lirm/aeron-go/aeron/logbuffer"
	"github.com/lirm/aeron-go/archive/codecs"
)

// E2E tests against a real Java ArchivingMediaDriver, see docker/README.md:
//
//	docker compose -f docker/docker-compose.yml up -d --build archiving-driver
//	AERON_E2E=1 AERON_DIR=/dev/shm/aeron-compose go test ./archive -run E2E -v

const (
	e2ePayloadLength = 64
	e2eFrameLength   = 96 // 32 byte header + 64 byte payload, 32 byte aligned
)

func e2eArchive(t *testing.T) *Archive {
	t.Helper()
	if os.Getenv("AERON_E2E") != "1" {
		t.Skip("set AERON_E2E=1 to run against a live archive")
	}
	dir := os.Getenv("AERON_DIR")
	if dir == "" {
		dir = "/dev/shm/aeron-compose"
	}
	ctx := aeron.NewContext()
	ctx.AeronDir(dir)
	options := DefaultOptions()
	options.IdleStrategy = idlestrategy.Sleeping{SleepFor: time.Millisecond}
	a, err := NewArchive(options, ctx)
	if err != nil {
		t.Fatalf("NewArchive: %v", err)
	}
	t.Cleanup(func() { a.Close() })
	return a
}

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func publishN(t *testing.T, pub *aeron.Publication, from, n int) {
	t.Helper()
	buf := atomic.MakeBuffer(make([]byte, e2ePayloadLength))
	for i := from; i < from+n; i++ {
		buf.PutInt32(0, int32(i))
		eventually(t, fmt.Sprintf("offer %d", i), func() bool {
			return pub.Offer(buf, 0, e2ePayloadLength, nil) > 0
		})
	}
}

func collect(t *testing.T, sub *aeron.Subscription, n int) []int {
	t.Helper()
	var got []int
	handler := func(buffer *atomic.Buffer, offset int32, length int32, header *logbuffer.Header) {
		b := make([]byte, 4)
		buffer.GetBytes(offset, b)
		got = append(got, int(int32(binary.LittleEndian.Uint32(b))))
	}
	eventually(t, fmt.Sprintf("%d messages (got %d)", n, len(got)), func() bool {
		sub.Poll(handler, 10)
		return len(got) >= n
	})
	return got
}

func expectSequence(t *testing.T, got []int, from, n int) {
	t.Helper()
	if len(got) != n {
		t.Fatalf("expected %d messages, got %d", n, len(got))
	}
	for i, v := range got {
		if v != from+i {
			t.Fatalf("message %d: expected %d got %d", i, from+i, v)
		}
	}
}

func expectNothing(t *testing.T, sub *aeron.Subscription, wait time.Duration) {
	t.Helper()
	count := 0
	deadline := time.Now().Add(wait)
	for time.Now().Before(deadline) {
		count += sub.Poll(func(*atomic.Buffer, int32, int32, *logbuffer.Header) {}, 10)
		time.Sleep(5 * time.Millisecond)
	}
	if count != 0 {
		t.Fatalf("expected no messages, got %d", count)
	}
}

func expectArchiveError(t *testing.T, err error, code ArchiveErrorCode) {
	t.Helper()
	var archiveErr *ArchiveError
	if !errors.As(err, &archiveErr) || archiveErr.ErrorCode != code {
		t.Fatalf("expected ArchiveError code %d, got %v", code, err)
	}
}

func findRecording(t *testing.T, a *Archive, endpoint string, stream int32, sessionID int32) *codecs.RecordingDescriptor {
	t.Helper()
	var found *codecs.RecordingDescriptor
	eventually(t, "recording descriptor", func() bool {
		descriptors, err := a.ListRecordingsForUri(0, 1000, endpoint, stream)
		if err != nil {
			return false
		}
		for _, d := range descriptors {
			if d.SessionId == sessionID {
				found = d
				return true
			}
		}
		return false
	})
	return found
}

func TestE2ERecordingAndReplay(t *testing.T) {
	a := e2eArchive(t)
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	port := 21000 + r.Intn(5000)
	stream := int32(5000 + r.Intn(1000))
	endpoint := fmt.Sprintf("localhost:%d", port)
	liveChannel := "aeron:udp?endpoint=" + endpoint
	replayChannel := fmt.Sprintf("aeron:udp?endpoint=localhost:%d", port+1)
	responseChannel := fmt.Sprintf("aeron:udp?control-mode=response|control=localhost:%d", port+2)
	replayStream := stream + 1

	if id := a.ArchiveID(); id != 1 {
		t.Fatalf("ArchiveID() = %d; docker archive uses aeron.archive.id=1", id)
	}

	subscriptionID, err := a.StartRecording(liveChannel, stream, false, false)
	if err != nil {
		t.Fatalf("StartRecording: %v", err)
	}
	pub, err := a.AddPublication(liveChannel, stream)
	if err != nil {
		t.Fatal(err)
	}
	defer pub.Close()
	eventually(t, "publication connected", pub.IsConnected)
	descriptor := findRecording(t, a, endpoint, stream, pub.SessionID())
	recordingID := descriptor.RecordingId

	publishN(t, pub, 0, 100)
	end := int64(100 * e2eFrameLength)
	eventually(t, "recording position", func() bool {
		pos, err := a.GetRecordingPosition(recordingID)
		return err == nil && pos == end
	})
	if pos, err := a.GetStartPosition(recordingID); err != nil || pos != 0 {
		t.Fatalf("GetStartPosition = %d, %v", pos, err)
	}
	if pos, err := a.GetStopPosition(recordingID); err != nil || pos != NullPosition {
		t.Fatalf("GetStopPosition of active recording = %d, %v", pos, err)
	}
	if pos, err := a.GetMaxRecordedPosition(recordingID); err != nil || pos != end {
		t.Fatalf("GetMaxRecordedPosition = %d, %v", pos, err)
	}

	t.Run("BoundedReplayWithCounter", func(t *testing.T) {
		counterRegistrationID, err := a.aeron.AddCounterByLabel(1001, "e2e replay bound")
		if err != nil {
			t.Fatal(err)
		}
		var counter *aeron.Counter
		eventually(t, "counter", func() bool {
			counter, err = a.aeron.FindCounter(counterRegistrationID)
			return err == nil && counter != nil
		})
		defer counter.Close()
		counter.Counter().Set(30 * e2eFrameLength)

		params := NewReplayParams()
		params.Position = 0
		params.BoundingLimitCounterID = counter.Id()
		replaySessionID, err := a.StartReplayWithParams(recordingID, replayChannel, replayStream, params)
		if err != nil {
			t.Fatalf("bounded StartReplayWithParams: %v", err)
		}
		uri, _ := aeron.ParseChannelUri(replayChannel)
		uri.SetSessionID(int32(replaySessionID))
		sub, err := a.AddSubscription(uri.String(), replayStream)
		if err != nil {
			t.Fatal(err)
		}
		defer sub.Close()
		expectSequence(t, collect(t, sub, 30), 0, 30)
		expectNothing(t, sub, 300*time.Millisecond)
		counter.Counter().Set(end)
		expectSequence(t, collect(t, sub, 70), 30, 70)

		if err := a.StopReplay(replaySessionID); err != nil {
			t.Fatalf("StopReplay: %v", err)
		}
	})

	t.Run("StopUnknownReplay", func(t *testing.T) {
		// Java ArchiveConductor.stopReplay answers OK even if the replay session is unknown
		if err := a.StopReplay(0x7fffffff); err != nil {
			t.Fatalf("StopReplay unknown: %v", err)
		}
		if err := a.StopAllReplays(recordingID); err != nil {
			t.Fatalf("StopAllReplays: %v", err)
		}
	})

	t.Run("PingKeepalive", func(t *testing.T) {
		// The archive pings idle control sessions every second; they must be consumed without upsetting requests
		time.Sleep(2500 * time.Millisecond)
		if err := a.KeepAlive(); err != nil {
			t.Fatal(err)
		}
		if pos, err := a.GetMaxRecordedPosition(recordingID); err != nil || pos != end {
			t.Fatalf("GetMaxRecordedPosition after idle = %d, %v", pos, err)
		}
	})

	t.Run("TryStopRecording", func(t *testing.T) {
		if stopped, err := a.TryStopRecording("aeron:udp?endpoint=localhost:1", 9999); err != nil || stopped {
			t.Fatalf("TryStopRecording unknown = %v, %v", stopped, err)
		}
		if err := a.StopRecordingBySubscriptionId(-12345); err == nil {
			t.Fatal("StopRecordingBySubscriptionId unknown should fail")
		}
		if stopped, err := a.TryStopRecordingBySubscriptionId(subscriptionID); err != nil || !stopped {
			t.Fatalf("TryStopRecordingBySubscriptionId = %v, %v", stopped, err)
		}
		if stopped, err := a.TryStopRecordingBySubscriptionId(subscriptionID); err != nil || stopped {
			t.Fatalf("second TryStopRecordingBySubscriptionId = %v, %v", stopped, err)
		}
		eventually(t, "stop position", func() bool {
			pos, err := a.GetStopPosition(recordingID)
			return err == nil && pos == end
		})
		if pos, err := a.GetRecordingPosition(recordingID); err != nil || pos != NullPosition {
			t.Fatalf("GetRecordingPosition of stopped recording = %d, %v", pos, err)
		}
		if stopped, err := a.TryStopRecordingByIdentity(recordingID); err != nil || stopped {
			t.Fatalf("TryStopRecordingByIdentity of stopped = %v, %v", stopped, err)
		}
		_, err := a.TryStopRecordingByIdentity(1 << 40)
		expectArchiveError(t, err, ErrorCodeUnknownRecording)
		_, err = a.GetMaxRecordedPosition(1 << 40)
		expectArchiveError(t, err, ErrorCodeUnknownRecording)
	})

	t.Run("Replay", func(t *testing.T) {
		sub, err := a.Replay(recordingID, 0, ReplayAllAndStop, replayChannel, replayStream)
		if err != nil {
			t.Fatalf("Replay: %v", err)
		}
		expectSequence(t, collect(t, sub, 100), 0, 100)
		sub.Close()

		sub, err = a.Replay(recordingID, 50*e2eFrameLength, 20*e2eFrameLength, replayChannel, replayStream)
		if err != nil {
			t.Fatalf("Replay range: %v", err)
		}
		expectSequence(t, collect(t, sub, 20), 50, 20)
		expectNothing(t, sub, 200*time.Millisecond)
		sub.Close()

		params := NewReplayParams()
		params.Position = 10 * e2eFrameLength
		params.Length = ReplayAllAndStop
		params.FileIoMaxLength = 4096
		sub, err = a.ReplayWithParams(recordingID, replayChannel, replayStream, params)
		if err != nil {
			t.Fatalf("ReplayWithParams fileIoMaxLength: %v", err)
		}
		expectSequence(t, collect(t, sub, 90), 10, 90)
		sub.Close()

		params.Position = 3 // not frame aligned
		_, err = a.StartReplayWithParams(recordingID, replayChannel, replayStream, params)
		if err == nil {
			t.Fatal("expected error replaying from an unaligned position")
		}
	})

	t.Run("ReplayViaResponseChannel", func(t *testing.T) {
		token, err := a.RequestReplayToken(recordingID)
		if err != nil || token == NullValue {
			t.Fatalf("RequestReplayToken = %d, %v", token, err)
		}

		params := NewReplayParams()
		params.Position = 0
		params.Length = ReplayAllAndStop
		sub, err := a.ReplayWithParams(recordingID, responseChannel, replayStream, params)
		if err != nil {
			t.Fatalf("ReplayWithParams response channel: %v", err)
		}
		if params.ReplayToken == NullValue {
			t.Fatal("ReplayToken not set")
		}
		expectSequence(t, collect(t, sub, 100), 0, 100)
		sub.Close()

		params = NewReplayParams()
		params.Position = 40 * e2eFrameLength
		params.Length = 10 * e2eFrameLength
		if _, err := a.StartReplayWithParams(recordingID, responseChannel, replayStream, params); err == nil {
			t.Fatal("expected error without SubscriptionRegistrationID")
		}
		sub, err = a.AddSubscription(responseChannel, replayStream)
		if err != nil {
			t.Fatal(err)
		}
		defer sub.Close()
		params.SubscriptionRegistrationID = sub.RegistrationID()
		if _, err := a.StartReplayWithParams(recordingID, responseChannel, replayStream, params); err != nil {
			t.Fatalf("StartReplayWithParams response channel: %v", err)
		}
		expectSequence(t, collect(t, sub, 10), 40, 10)
	})

	t.Run("ExtendRecording", func(t *testing.T) {
		d, err := a.ListRecording(recordingID)
		if err != nil {
			t.Fatal(err)
		}
		extendChannel := fmt.Sprintf("aeron:udp?endpoint=localhost:%d", port+3)
		termLength := int64(d.TermBufferLength)
		initialChannel := fmt.Sprintf("%s|init-term-id=%d|term-id=%d|term-offset=%d|term-length=%d", extendChannel,
			d.InitialTermId, int64(d.InitialTermId)+d.StopPosition/termLength, d.StopPosition%termLength, termLength)

		extendSubscriptionID, err := a.ExtendRecording(recordingID, stream, codecs.SourceLocation.REMOTE, false, extendChannel)
		if err != nil {
			t.Fatalf("ExtendRecording: %v", err)
		}
		extendPub, err := a.aeron.AddExclusivePublication(initialChannel, stream)
		if err != nil {
			t.Fatal(err)
		}
		defer extendPub.Close()
		eventually(t, "extend publication connected", extendPub.IsConnected)
		publishN(t, extendPub, 100, 50)
		newEnd := end + 50*e2eFrameLength
		eventually(t, "extended recording position", func() bool {
			pos, err := a.GetRecordingPosition(recordingID)
			return err == nil && pos == newEnd
		})
		if stopped, err := a.TryStopRecordingBySubscriptionId(extendSubscriptionID); err != nil || !stopped {
			t.Fatalf("stop extend = %v, %v", stopped, err)
		}
		eventually(t, "extended stop position", func() bool {
			pos, err := a.GetStopPosition(recordingID)
			return err == nil && pos == newEnd
		})
		sub, err := a.Replay(recordingID, 0, ReplayAllAndStop, replayChannel, replayStream)
		if err != nil {
			t.Fatal(err)
		}
		defer sub.Close()
		expectSequence(t, collect(t, sub, 150), 0, 150)

		_, err = a.ExtendRecording(recordingID, stream, codecs.SourceLocation.REMOTE, false, extendChannel+"|session-id=12345")
		if err != nil {
			// a session mismatch is only detected when an image arrives; the request itself is accepted
			t.Logf("ExtendRecording with session-id: %v", err)
		}
		a.TryStopRecording(extendChannel+"|session-id=12345", stream)
	})
}

func TestE2EAutoStopAndRecordedExclusivePublication(t *testing.T) {
	a := e2eArchive(t)
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	port := 26000 + r.Intn(3000)
	stream := int32(7000 + r.Intn(1000))
	endpoint := fmt.Sprintf("localhost:%d", port)
	channel := "aeron:udp?endpoint=" + endpoint

	subscriptionID, err := a.StartRecording(channel, stream, false, true)
	if err != nil {
		t.Fatalf("StartRecording autoStop: %v", err)
	}
	pub, err := a.AddPublication(channel, stream)
	if err != nil {
		t.Fatal(err)
	}
	eventually(t, "publication connected", pub.IsConnected)
	d := findRecording(t, a, endpoint, stream, pub.SessionID())
	publishN(t, pub, 0, 10)
	eventually(t, "recording position", func() bool {
		pos, err := a.GetRecordingPosition(d.RecordingId)
		return err == nil && pos == 10*e2eFrameLength
	})
	pub.Close()
	eventually(t, "autoStop to stop the recording", func() bool {
		pos, err := a.GetStopPosition(d.RecordingId)
		return err == nil && pos == 10*e2eFrameLength
	})
	eventually(t, "autoStop to remove the recording subscription", func() bool {
		stopped, err := a.TryStopRecordingBySubscriptionId(subscriptionID)
		return err == nil && !stopped
	})

	// local (spy) recording: ssc makes the publication connected without a network subscriber
	exclusiveChannel := fmt.Sprintf("aeron:udp?endpoint=localhost:%d|ssc=true", port+1)
	exclusivePub, err := a.AddRecordedExclusivePublication(exclusiveChannel, stream)
	if err != nil {
		t.Fatalf("AddRecordedExclusivePublication: %v", err)
	}
	defer exclusivePub.Close()
	eventually(t, "exclusive publication connected", exclusivePub.IsConnected)
	ed := findRecording(t, a, fmt.Sprintf("localhost:%d", port+1), stream, exclusivePub.SessionID())
	publishN(t, exclusivePub, 0, 5)
	eventually(t, "exclusive recording position", func() bool {
		pos, err := a.GetRecordingPosition(ed.RecordingId)
		return err == nil && pos == 5*e2eFrameLength
	})
	if stopped, err := a.TryStopRecordingByIdentity(ed.RecordingId); err != nil || !stopped {
		t.Fatalf("TryStopRecordingByIdentity = %v, %v", stopped, err)
	}
	if stopped, err := a.StopRecordingByIdentity(ed.RecordingId); err != nil || stopped {
		t.Fatalf("StopRecordingByIdentity of stopped = %v, %v", stopped, err)
	}
}
