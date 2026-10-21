package archive

import (
	"fmt"
	"math/rand"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/lirm/aeron-go/aeron"
	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/archive/codecs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestE2E_RecordingEventsAndSignals runs against the docker Java 1.53.3 ArchivingMediaDriver:
//
//	docker compose -f docker/docker-compose.yml up -d --build archiving-driver
//	AERON_E2E=1 AERON_DIR=/dev/shm/aeron-compose go test ./archive -run E2E -v
func TestE2E_RecordingEventsAndSignals(t *testing.T) {
	if os.Getenv("AERON_E2E") != "1" {
		t.Skip("set AERON_E2E=1 to run against the docker archive")
	}
	aeronDir := os.Getenv("AERON_DIR")
	if aeronDir == "" {
		aeronDir = "/dev/shm/aeron-compose"
	}

	var mu sync.Mutex
	type sig struct {
		recordingId int64
		signal      codecs.RecordingSignalEnum
	}
	var signals []sig
	var listenerErrors []error

	options := DefaultOptions()
	options.Timeout = 10 * time.Second
	ctx := aeron.NewContext().AeronDir(aeronDir)
	arch, err := NewArchive(options, ctx)
	require.NoError(t, err)
	defer arch.Close()
	arch.Listeners.RecordingSignalListener = func(e *codecs.RecordingSignalEvent) {
		mu.Lock()
		defer mu.Unlock()
		t.Logf("signal %s recordingId=%d subscriptionId=%d position=%d", signalName(e.Signal), e.RecordingId, e.SubscriptionId, e.Position)
		signals = append(signals, sig{e.RecordingId, e.Signal})
	}
	arch.Listeners.ErrorListener = func(err error) { listenerErrors = append(listenerErrors, err) }

	// Recording events: the Archive-owned adapter with a Java-style listener plus a standalone poller.
	events := map[string][]int64{}
	require.NoError(t, arch.EnableRecordingEvents())
	arch.Events.SetListener(RecordingEventsListenerFuncs{
		Start: func(recordingId, startPosition int64, sessionId, streamId int32, channel, sourceIdentity string) {
			t.Logf("event start recordingId=%d stream=%d channel=%s source=%s", recordingId, streamId, channel, sourceIdentity)
			events["start"] = append(events["start"], recordingId)
		},
		Progress: func(recordingId, startPosition, position int64) {
			events["progress"] = append(events["progress"], recordingId)
		},
		Stop: func(recordingId, startPosition, stopPosition int64) {
			t.Logf("event stop recordingId=%d start=%d stop=%d", recordingId, startPosition, stopPosition)
			events["stop"] = append(events["stop"], recordingId)
		},
	})
	eventsSub, err := arch.Aeron().AddSubscription(options.RecordingEventsChannel, options.RecordingEventsStream)
	require.NoError(t, err)
	defer eventsSub.Close()
	eventsPoller := NewRecordingEventsPoller(eventsSub)
	polledEvents := map[string][]int64{}

	pollAll := func() {
		_, _ = arch.PollForErrorResponse()
		arch.Events.PollWithContext(nil, 10)
		for i := 0; i < 10; i++ {
			if _, err := eventsPoller.Poll(); err != nil || !eventsPoller.IsPollComplete() {
				break
			}
			switch {
			case eventsPoller.IsRecordingStarted():
				polledEvents["start"] = append(polledEvents["start"], eventsPoller.RecordingId())
			case eventsPoller.IsRecordingProgress():
				polledEvents["progress"] = append(polledEvents["progress"], eventsPoller.RecordingId())
			case eventsPoller.IsRecordingStopped():
				polledEvents["stop"] = append(polledEvents["stop"], eventsPoller.RecordingId())
			}
		}
	}
	waitFor := func(what string, cond func() bool) {
		t.Helper()
		deadline := time.Now().Add(15 * time.Second)
		for !cond() {
			if time.Now().After(deadline) {
				t.Fatalf("timed out waiting for %s", what)
			}
			pollAll()
			time.Sleep(time.Millisecond)
		}
	}
	// pollControlUntil polls only through cond: pollAll's PollForErrorResponse would consume the
	// responses meant for the standalone pollers on the same control subscription.
	pollControlUntil := func(what string, cond func() bool) {
		t.Helper()
		deadline := time.Now().Add(15 * time.Second)
		for !cond() {
			if time.Now().After(deadline) {
				t.Fatalf("timed out waiting for %s", what)
			}
			time.Sleep(time.Millisecond)
		}
	}
	hasSignal := func(recordingId int64, s codecs.RecordingSignalEnum) func() bool {
		return func() bool {
			mu.Lock()
			defer mu.Unlock()
			for _, e := range signals {
				if e.signal == s && (recordingId == aeron.NullValue || e.recordingId == recordingId) {
					return true
				}
			}
			return false
		}
	}
	signalsFor := func(recordingId int64) []string {
		mu.Lock()
		defer mu.Unlock()
		var names []string
		for _, e := range signals {
			if e.recordingId == recordingId {
				names = append(names, signalName(e.signal))
			}
		}
		return names
	}
	contains := func(ids []int64, id int64) bool {
		for _, v := range ids {
			if v == id {
				return true
			}
		}
		return false
	}
	buffer := atomic.MakeBuffer(make([]byte, 256))
	publish := func(pub *aeron.Publication, recordingId int64, count int) {
		t.Helper()
		waitFor("publication connected", pub.IsConnected)
		for i := 0; i < count; i++ {
			for pub.Offer(buffer, 0, buffer.Capacity(), nil) < 0 {
				pollAll()
			}
		}
		waitFor("recording to catch up", func() bool {
			pos, err := arch.GetRecordingPosition(recordingId)
			return err == nil && pos == pub.Position()
		})
	}

	stream := int32(20000 + rand.Intn(10000))
	const channel = "aeron:ipc?term-length=65536"

	// START + recording started/progress events
	subscriptionId, err := arch.StartRecording("aeron:ipc", stream, true, false)
	require.NoError(t, err)
	pub, err := arch.Aeron().AddExclusivePublication(channel, stream)
	require.NoError(t, err)
	var recordingId int64 = aeron.NullValue
	waitFor("recording started event", func() bool {
		if ids := events["start"]; len(ids) > 0 {
			recordingId = ids[len(ids)-1]
			return true
		}
		return false
	})
	waitFor("START signal", hasSignal(recordingId, codecs.RecordingSignal.START))
	publish(pub, recordingId, 64)
	waitFor("progress event", func() bool { return contains(events["progress"], recordingId) })

	// STOP + recording stopped event
	require.NoError(t, arch.StopRecordingBySubscriptionId(subscriptionId))
	waitFor("STOP signal", hasSignal(recordingId, codecs.RecordingSignal.STOP))
	waitFor("stopped event", func() bool { return contains(events["stop"], recordingId) })
	stopPosition := pub.Position()
	require.NoError(t, pub.Close())

	// Errors come back as ArchiveException with the Java error code
	err = arch.StopRecordingBySubscriptionId(subscriptionId)
	code, ok := ArchiveErrorCodeOf(err)
	require.True(t, ok, "expected ArchiveException, got %v", err)
	assert.Equal(t, ErrorCodeUnknownSubscription, code)
	t.Logf("expected error: %v", err)

	// EXTEND
	desc, err := arch.ListRecording(recordingId)
	require.NoError(t, err)
	termLength := int64(desc.TermBufferLength)
	extendChannel := fmt.Sprintf("aeron:ipc?init-term-id=%d|term-id=%d|term-offset=%d|term-length=%d",
		desc.InitialTermId, int64(desc.InitialTermId)+stopPosition/termLength, stopPosition%termLength, termLength)
	extendSubscriptionId, err := arch.ExtendRecording(recordingId, stream, codecs.SourceLocation.LOCAL, false, "aeron:ipc")
	require.NoError(t, err)
	pub2, err := arch.Aeron().AddExclusivePublication(extendChannel, stream)
	require.NoError(t, err)
	waitFor("EXTEND signal", hasSignal(recordingId, codecs.RecordingSignal.EXTEND))
	publish(pub2, recordingId, 16)
	require.NoError(t, arch.StopRecordingBySubscriptionId(extendSubscriptionId))
	require.NoError(t, pub2.Close())
	waitFor("second stopped event", func() bool {
		n := 0
		for _, id := range events["stop"] {
			if id == recordingId {
				n++
			}
		}
		return n >= 2
	})

	// REPLICATE + REPLICATE_END (archive replicating from itself)
	controlChannel := options.RequestChannel
	_, err = arch.Replicate(recordingId, aeron.NullValue, options.RequestStream, controlChannel, "")
	require.NoError(t, err)
	var replicaId int64 = aeron.NullValue
	waitFor("REPLICATE signal", func() bool {
		mu.Lock()
		defer mu.Unlock()
		for _, e := range signals {
			if e.signal == codecs.RecordingSignal.REPLICATE {
				replicaId = e.recordingId
				return true
			}
		}
		return false
	})
	waitFor("REPLICATE_END signal", hasSignal(replicaId, codecs.RecordingSignal.REPLICATE_END))
	t.Logf("replica %d signals: %v", replicaId, signalsFor(replicaId))

	// SYNC: replicating into an existing destination that is already caught up
	_, err = arch.Replicate(recordingId, replicaId, options.RequestStream, controlChannel, "")
	require.NoError(t, err)
	waitFor("second REPLICATE_END", func() bool {
		n := 0
		for _, s := range signalsFor(replicaId) {
			if s == "REPLICATE_END" {
				n++
			}
		}
		return n >= 2
	})
	t.Logf("replica %d signals after re-sync: %v", replicaId, signalsFor(replicaId))
	assert.Contains(t, signalsFor(replicaId), "SYNC")

	// Standalone RecordingDescriptorPoller on the live control session
	descriptorPoller := NewRecordingDescriptorPoller(arch.Control.Subscription, nil, nil, arch.SessionID, 10)
	listCorrelationId := nextCorrelationID()
	var listed []int64
	descriptorPoller.Reset(listCorrelationId, 1000, RecordingDescriptorConsumer(func(d *codecs.RecordingDescriptor) {
		id := d.RecordingId
		listed = append(listed, id)
	}))
	require.NoError(t, arch.Proxy.ListRecordingsRequest(listCorrelationId, 0, 1000))
	pollControlUntil("descriptor poller completion", func() bool {
		_, err := descriptorPoller.Poll()
		require.NoError(t, err)
		return descriptorPoller.IsDispatchComplete()
	})
	assert.Contains(t, listed, recordingId)
	assert.Contains(t, listed, replicaId)

	// Standalone ControlResponsePoller
	responsePoller := NewControlResponsePoller(arch.Control.Subscription, 0)
	positionCorrelationId := nextCorrelationID()
	require.NoError(t, arch.Proxy.RecordingPositionRequest(positionCorrelationId, recordingId))
	pollControlUntil("control response poller", func() bool {
		_, err := responsePoller.Poll()
		require.NoError(t, err)
		return responsePoller.IsControlResponse() && responsePoller.CorrelationId() == positionCorrelationId
	})
	assert.EqualValues(t, aeron.NullValue, responsePoller.RelevantId(), "stopped recording has no active position")

	// DELETE
	_, err = arch.PurgeRecording(replicaId)
	require.NoError(t, err)
	waitFor("DELETE signal", hasSignal(replicaId, codecs.RecordingSignal.DELETE))
	t.Logf("recording %d signals: %v", recordingId, signalsFor(recordingId))
	t.Logf("polled events: %v", polledEvents)
	assert.Contains(t, polledEvents["start"], recordingId)
	assert.Contains(t, polledEvents["stop"], recordingId)
	assert.Empty(t, listenerErrors)
}

func signalName(s codecs.RecordingSignalEnum) string {
	names := []string{"START", "STOP", "EXTEND", "REPLICATE", "MERGE", "SYNC", "DELETE", "REPLICATE_END"}
	if s >= 0 && int(s) < len(names) {
		return names[s]
	}
	return fmt.Sprintf("UNKNOWN(%d)", s)
}
