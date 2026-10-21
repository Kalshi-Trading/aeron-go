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

// End-to-end tests against the Java 1.53.3 archives from docker/docker-compose.yml (archive 1) and
// docker/replication.compose.yml (archive 2, 256KiB segment files). Gated by AERON_E2E=1.

const (
	e2eTermLength   = 64 * 1024
	e2eFrameLength  = 4096 // payload + 32 byte header: frames tile terms and segments exactly
	e2ePayload      = e2eFrameLength - 32
	e2eArchive1Ctrl = "aeron:udp?endpoint=localhost:8010"
	e2eArchive2Ctrl = "aeron:udp?endpoint=localhost:8011"
)

func e2eEnv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func requireE2E(t *testing.T) {
	if os.Getenv("AERON_E2E") != "1" {
		t.Skip("set AERON_E2E=1 to run against the docker Java archive(s)")
	}
}

func e2eConnect(t *testing.T, aeronDir, controlChannel, eventsChannel string) *Archive {
	t.Helper()
	ctx := aeron.NewContext()
	ctx.AeronDir(aeronDir)
	opts := DefaultOptions()
	opts.RequestChannel = controlChannel
	opts.RecordingEventsChannel = eventsChannel
	opts.Timeout = 10 * time.Second
	a, err := NewArchive(opts, ctx)
	require.NoError(t, err, "connect to archive at %s (%s)", aeronDir, controlChannel)
	t.Cleanup(func() { a.Close() })
	return a
}

func e2eArchive1(t *testing.T) *Archive {
	return e2eConnect(t, e2eEnv("AERON_DIR", "/dev/shm/aeron-compose"), e2eArchive1Ctrl, "aeron:udp?control-mode=dynamic|control=localhost:8030")
}

func e2eArchive2(t *testing.T) *Archive {
	return e2eConnect(t, e2eEnv("AERON_DIR_2", "/dev/shm/aeron-compose-2"), e2eArchive2Ctrl, "aeron:udp?control-mode=dynamic|control=localhost:8031")
}

func e2eStream() int32 { return 5000 + rand.Int31n(100000) }

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

type e2eRecording struct {
	recordingID    int64
	subscriptionID int64
	pub            *aeron.Publication
}

// startRecording starts a local recording of channel/stream and an exclusive publication on it.
func startRecording(t *testing.T, a *Archive, channel string, stream int32) *e2eRecording {
	t.Helper()
	subID, err := a.StartRecording(channel, stream, true, false)
	require.NoError(t, err)
	pub, err := a.AddExclusivePublication(channel, stream)
	require.NoError(t, err)
	waitFor(t, "publication connected", pub.IsConnected)
	r := &e2eRecording{recordingID: -1, subscriptionID: subID, pub: pub}
	waitFor(t, "recording to appear in catalog", func() bool {
		id, err := a.FindLastMatchingRecording(0, pub.SessionID(), stream, "aeron:ipc")
		require.NoError(t, err)
		r.recordingID = id
		return id != RecordingIdNullValue
	})
	return r
}

func (r *e2eRecording) publish(t *testing.T, a *Archive, messages int) int64 {
	t.Helper()
	payload := make([]byte, e2ePayload)
	buf := atomic.MakeBuffer(payload, len(payload))
	for i := 0; i < messages; i++ {
		waitFor(t, "offer", func() bool { return r.pub.Offer(buf, 0, int32(len(payload)), nil) > 0 })
	}
	position := r.pub.Position()
	waitFor(t, "recording to catch up", func() bool {
		p, err := a.GetMaxRecordedPosition(r.recordingID)
		require.NoError(t, err)
		return p == position
	})
	return position
}

func (r *e2eRecording) stop(t *testing.T, a *Archive) {
	t.Helper()
	require.NoError(t, a.StopRecordingBySubscriptionId(r.subscriptionID))
	r.pub.Close()
	waitFor(t, "recording stopped", func() bool {
		p, err := a.GetRecordingPosition(r.recordingID)
		return err != nil || p == RecordingPositionNull
	})
}

func recordMessages(t *testing.T, a *Archive, channel string, stream int32, messages int) (int64, int64) {
	r := startRecording(t, a, channel, stream)
	pos := r.publish(t, a, messages)
	r.stop(t, a)
	return r.recordingID, pos
}

func listOne(t *testing.T, a *Archive, recordingID int64) *codecs.RecordingDescriptor {
	t.Helper()
	d, err := a.ListRecording(recordingID)
	require.NoError(t, err)
	require.NotNil(t, d, "recording %d", recordingID)
	return d
}

func TestE2ECatalog(t *testing.T) {
	requireE2E(t)
	a := e2eArchive1(t)
	channel := fmt.Sprintf("aeron:ipc?term-length=%d|mtu=%d", e2eTermLength, e2eFrameLength)
	stream := e2eStream()

	r := startRecording(t, a, channel, stream)
	stopPos := r.publish(t, a, 20)
	assert.Equal(t, int64(20*e2eFrameLength), stopPos)

	// Active recording subscriptions, filtered by stream and channel fragment.
	var subs []*codecs.RecordingSubscriptionDescriptor
	n, err := a.ListRecordingSubscriptionsWithConsumer(0, 10, true, stream, "aeron:ipc", func(d *codecs.RecordingSubscriptionDescriptor) {
		subs = append(subs, d)
	})
	require.NoError(t, err)
	require.Equal(t, int32(1), n)
	assert.Equal(t, r.subscriptionID, subs[0].SubscriptionId)
	assert.Equal(t, stream, subs[0].StreamId)

	r.stop(t, a)

	start := time.Now()
	subs2, err := a.ListRecordingSubscriptions(0, 10, true, stream, "aeron:ipc")
	require.NoError(t, err)
	assert.Empty(t, subs2)
	assert.Less(t, time.Since(start), time.Second, "SUBSCRIPTION_UNKNOWN must end the listing, not a timeout")

	var desc *codecs.RecordingDescriptor
	n, err = a.ListRecordingWithConsumer(r.recordingID, func(d *codecs.RecordingDescriptor) { desc = d })
	require.NoError(t, err)
	require.Equal(t, int32(1), n)
	assert.Equal(t, r.recordingID, desc.RecordingId)
	assert.Equal(t, stream, desc.StreamId)
	assert.Equal(t, int64(0), desc.StartPosition)
	assert.Equal(t, stopPos, desc.StopPosition)
	assert.Equal(t, int32(e2eTermLength), desc.TermBufferLength)
	assert.Equal(t, channel, string(desc.OriginalChannel))

	n, err = a.ListRecordingWithConsumer(1<<40, func(*codecs.RecordingDescriptor) { t.Fatal("unexpected descriptor") })
	require.NoError(t, err)
	assert.Zero(t, n)

	var ids []int64
	n, err = a.ListRecordingsForUriWithConsumer(0, 1000, "aeron:ipc", stream, func(d *codecs.RecordingDescriptor) { ids = append(ids, d.RecordingId) })
	require.NoError(t, err)
	assert.Equal(t, []int64{r.recordingID}, ids)
	assert.Equal(t, int32(1), n)

	n, err = a.ListRecordingsWithConsumer(r.recordingID, 10, func(d *codecs.RecordingDescriptor) {})
	require.NoError(t, err)
	assert.GreaterOrEqual(t, n, int32(1))
	all, err := a.ListRecordings(r.recordingID, 10)
	require.NoError(t, err)
	require.NotEmpty(t, all)
	assert.Equal(t, r.recordingID, all[0].RecordingId)

	id, err := a.FindLastMatchingRecording(0, r.pub.SessionID(), stream, "aeron:ipc")
	require.NoError(t, err)
	assert.Equal(t, r.recordingID, id)
	id, err = a.FindLastMatchingRecording(0, r.pub.SessionID(), stream+1, "aeron:ipc")
	require.NoError(t, err)
	assert.Equal(t, int64(RecordingIdNullValue), id)

	pos, err := a.GetStartPosition(r.recordingID)
	require.NoError(t, err)
	assert.Equal(t, int64(0), pos)
	pos, err = a.GetStopPosition(r.recordingID)
	require.NoError(t, err)
	assert.Equal(t, stopPos, pos)
	pos, err = a.GetMaxRecordedPosition(r.recordingID)
	require.NoError(t, err)
	assert.Equal(t, stopPos, pos)

	deleted, err := a.TruncateRecording(r.recordingID, 4*e2eFrameLength)
	require.NoError(t, err)
	assert.Equal(t, int64(0), deleted, "truncating within the first segment deletes no segment files")
	assert.Equal(t, int64(4*e2eFrameLength), listOne(t, a, r.recordingID).StopPosition)

	deleted, err = a.PurgeRecording(r.recordingID)
	require.NoError(t, err)
	assert.Equal(t, int64(1), deleted)
	d, err := a.ListRecording(r.recordingID)
	require.NoError(t, err)
	assert.Nil(t, d, "purged recording is no longer listed")
}

func TestE2ESegments(t *testing.T) {
	requireE2E(t)
	a := e2eArchive2(t)
	channel := fmt.Sprintf("aeron:ipc?term-length=%d|mtu=%d", e2eTermLength, e2eFrameLength)
	stream := e2eStream()

	recordingID, stopPos := recordMessages(t, a, channel, stream, 5*64+10) // 5 full 256KiB segments + 10 frames
	desc := listOne(t, a, recordingID)
	segLen := int64(desc.SegmentFileLength)
	require.Equal(t, int64(256*1024), segLen, "archive 2 must run with -Daeron.archive.segment.file.length=262144")
	require.Equal(t, 5*segLen+10*e2eFrameLength, stopPos)

	base := SegmentFileBasePosition(desc.StartPosition, 2*segLen+100, desc.TermBufferLength, desc.SegmentFileLength)
	assert.Equal(t, 2*segLen, base)

	startPos := func() int64 {
		p, err := a.GetStartPosition(recordingID)
		require.NoError(t, err)
		return p
	}

	require.NoError(t, a.DetachSegments(recordingID, base))
	assert.Equal(t, base, startPos())
	attached, err := a.AttachSegments(recordingID)
	require.NoError(t, err)
	assert.Equal(t, int64(2), attached)
	assert.Equal(t, int64(0), startPos())

	require.NoError(t, a.DetachSegments(recordingID, segLen))
	deleted, err := a.DeleteDetachedSegments(recordingID)
	require.NoError(t, err)
	assert.Equal(t, int64(1), deleted)
	assert.Equal(t, segLen, startPos())
	attached, err = a.AttachSegments(recordingID)
	require.NoError(t, err)
	assert.Zero(t, attached, "deleted segments cannot be reattached")

	purged, err := a.PurgeSegments(recordingID, 3*segLen)
	require.NoError(t, err)
	assert.Equal(t, int64(2), purged)
	assert.Equal(t, 3*segLen, startPos())

	// MigrateSegments: src ends on a segment boundary, dst starts exactly there.
	srcID, _ := recordMessages(t, a, channel, stream, 3*64+5)
	srcDesc := listOne(t, a, srcID)
	_, err = a.TruncateRecording(srcID, 2*segLen)
	require.NoError(t, err)
	termID := srcDesc.InitialTermId + int32(2*segLen/e2eTermLength)
	dstChannel := fmt.Sprintf("%s|init-term-id=%d|term-id=%d|term-offset=0", channel, srcDesc.InitialTermId, termID)
	dstID, dstStop := recordMessages(t, a, dstChannel, stream, 5)
	assert.Equal(t, int64(2*segLen), listOne(t, a, dstID).StartPosition)

	migrated, err := a.MigrateSegments(srcID, dstID)
	require.NoError(t, err)
	assert.Equal(t, int64(2), migrated)
	dst := listOne(t, a, dstID)
	assert.Equal(t, int64(0), dst.StartPosition)
	assert.Equal(t, dstStop, dst.StopPosition)
	src := listOne(t, a, srcID)
	assert.Equal(t, src.StartPosition, src.StopPosition, "migrated source is left empty")
}

type signalCollector struct {
	mu      sync.Mutex
	signals []codecs.RecordingSignalEvent
}

func (c *signalCollector) listen(e *codecs.RecordingSignalEvent) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.signals = append(c.signals, *e)
}

func (c *signalCollector) find(signal codecs.RecordingSignalEnum) *codecs.RecordingSignalEvent {
	c.mu.Lock()
	defer c.mu.Unlock()
	for i := range c.signals {
		if c.signals[i].Signal == signal {
			s := c.signals[i]
			return &s
		}
	}
	return nil
}

func (c *signalCollector) reset() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.signals = nil
}

func awaitSignal(t *testing.T, a *Archive, c *signalCollector, signal codecs.RecordingSignalEnum) *codecs.RecordingSignalEvent {
	t.Helper()
	var found *codecs.RecordingSignalEvent
	waitFor(t, fmt.Sprintf("recording signal %d", signal), func() bool {
		_, err := a.PollForErrorResponse()
		require.NoError(t, err)
		found = c.find(signal)
		return found != nil
	})
	return found
}

func TestE2EReplication(t *testing.T) {
	requireE2E(t)
	src := e2eArchive1(t)
	dst := e2eArchive2(t)
	signals := &signalCollector{}
	dst.Listeners.RecordingSignalListener = signals.listen

	channel := fmt.Sprintf("aeron:ipc?term-length=%d|mtu=%d", e2eTermLength, e2eFrameLength)
	stream := e2eStream()
	srcID, srcStop := recordMessages(t, src, channel, stream, 50)

	// Full replication of a stopped recording using every ReplicationParams field the archive accepts
	// without auth / response-channel setup.
	params := NewReplicationParams()
	params.ReplicationChannel = "aeron:udp?endpoint=localhost:0"
	params.FileIoMaxLength = 64 * 1024
	params.ReplicationSessionID = 0x1234
	params.EncodedCredentials = []byte("ignored-by-default-authenticator")
	replicationID, err := dst.ReplicateWithParams(srcID, 10, e2eArchive1Ctrl, params)
	require.NoError(t, err)

	replicate := awaitSignal(t, dst, signals, codecs.RecordingSignal.REPLICATE)
	end := awaitSignal(t, dst, signals, codecs.RecordingSignal.REPLICATE_END)
	assert.Equal(t, replicate.RecordingId, end.RecordingId)
	dstDesc := listOne(t, dst, replicate.RecordingId)
	assert.Equal(t, srcStop, dstDesc.StopPosition)
	assert.Equal(t, stream, dstDesc.StreamId)
	assert.Equal(t, int32(0x1234), dstDesc.SessionId, "replicationSessionId is used for the replicated image")
	stopped, err := dst.TryStopReplication(replicationID)
	require.NoError(t, err)
	assert.False(t, stopped, "replication already ended")

	// Bounded replication via StopPosition.
	signals.reset()
	params.Reset()
	params.StopPosition = 10 * e2eFrameLength
	_, err = dst.ReplicateWithParams(srcID, 10, e2eArchive1Ctrl, params)
	require.NoError(t, err)
	replicate = awaitSignal(t, dst, signals, codecs.RecordingSignal.REPLICATE)
	awaitSignal(t, dst, signals, codecs.RecordingSignal.REPLICATE_END)
	assert.Equal(t, int64(10*e2eFrameLength), listOne(t, dst, replicate.RecordingId).StopPosition)

	// Extend the full copy with the rest of a recording via DstRecordingID (tagged variant).
	signals.reset()
	_, err = dst.TaggedReplicate(srcID, replicate.RecordingId, 99, 100, 10, e2eArchive1Ctrl, "")
	require.NoError(t, err)
	awaitSignal(t, dst, signals, codecs.RecordingSignal.EXTEND)
	awaitSignal(t, dst, signals, codecs.RecordingSignal.REPLICATE_END)
	assert.Equal(t, srcStop, listOne(t, dst, replicate.RecordingId).StopPosition)

	// Replicating a live recording follows it until StopReplication.
	signals.reset()
	live := startRecording(t, src, channel, e2eStream())
	livePos := live.publish(t, src, 5)
	replicationID, err = dst.Replicate(live.recordingID, RecordingIdNullValue, 10, e2eArchive1Ctrl, "")
	require.NoError(t, err)
	replicate = awaitSignal(t, dst, signals, codecs.RecordingSignal.REPLICATE)
	waitFor(t, "live replication to catch up", func() bool {
		p, err := dst.GetMaxRecordedPosition(replicate.RecordingId)
		require.NoError(t, err)
		return p == livePos
	})
	assert.Nil(t, signals.find(codecs.RecordingSignal.REPLICATE_END))
	require.NoError(t, dst.StopReplication(replicationID))
	awaitSignal(t, dst, signals, codecs.RecordingSignal.REPLICATE_END)
	stopped, err = dst.TryStopReplication(replicationID)
	require.NoError(t, err)
	assert.False(t, stopped)
	live.stop(t, src)
}
