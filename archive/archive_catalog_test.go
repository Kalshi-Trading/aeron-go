package archive

import (
	"bytes"
	"io"
	"testing"

	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/logbuffer/term"
	"github.com/lirm/aeron-go/archive/codecs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type sbeMessage interface {
	Encode(*codecs.SbeGoMarshaller, io.Writer, bool) error
	SbeBlockLength() uint16
	SbeTemplateId() uint16
	SbeSchemaId() uint16
	SbeSchemaVersion() uint16
}

func encodeMessage(t *testing.T, msg sbeMessage) *atomic.Buffer {
	t.Helper()
	m := codecs.NewSbeGoMarshaller()
	buf := new(bytes.Buffer)
	hdr := codecs.MessageHeader{BlockLength: msg.SbeBlockLength(), TemplateId: msg.SbeTemplateId(), SchemaId: msg.SbeSchemaId(), Version: msg.SbeSchemaVersion()}
	require.NoError(t, hdr.Encode(m, buf))
	require.NoError(t, msg.Encode(m, buf, false))
	b := buf.Bytes()
	return atomic.MakeBuffer(b, len(b))
}

func decodeReplicateRequest2(t *testing.T, packet []byte) *codecs.ReplicateRequest2 {
	t.Helper()
	m := codecs.NewSbeGoMarshaller()
	r := bytes.NewReader(packet)
	var hdr codecs.MessageHeader
	require.NoError(t, hdr.Decode(m, r, 0))
	var req codecs.ReplicateRequest2
	assert.Equal(t, req.SbeTemplateId(), hdr.TemplateId)
	require.NoError(t, req.Decode(m, r, hdr.Version, hdr.BlockLength, true))
	return &req
}

func TestSegmentFileBasePosition(t *testing.T) {
	const termLength, segmentLength = 64 * 1024, 256 * 1024
	cases := []struct{ start, position, want int64 }{
		{0, 0, 0},
		{0, segmentLength - 32, 0},
		{0, segmentLength, segmentLength},
		{0, 300000, segmentLength},
		{70000, 70000, 65536},
		{70000, 70000 + segmentLength, 65536 + segmentLength},
		{65536, 65536 + 2*segmentLength + 100, 65536 + 2*segmentLength},
	}
	for _, c := range cases {
		assert.Equal(t, c.want, SegmentFileBasePosition(c.start, c.position, termLength, segmentLength), "start=%d position=%d", c.start, c.position)
	}
}

func TestReplicationParamsDefaults(t *testing.T) {
	p := NewReplicationParams()
	assert.Equal(t, RecordingPositionNull, p.StopPosition)
	assert.Equal(t, int64(RecordingIdNullValue), p.DstRecordingID)
	assert.Equal(t, int64(-1), p.ChannelTagID)
	assert.Equal(t, int64(-1), p.SubscriptionTagID)
	assert.Equal(t, int32(-1), p.FileIoMaxLength)
	assert.Equal(t, int32(-1), p.ReplicationSessionID)
	assert.Empty(t, p.LiveDestination)
	assert.Empty(t, p.ReplicationChannel)
	assert.Empty(t, p.SrcResponseChannel)
	assert.Nil(t, p.EncodedCredentials)

	p.StopPosition = 7
	p.EncodedCredentials = []byte("x")
	assert.Equal(t, NewReplicationParams(), p.Reset())
}

func TestReplicateRequest2FullPacketEncodesAllFields(t *testing.T) {
	p := NewReplicationParams()
	p.StopPosition = 4096
	p.DstRecordingID = 3
	p.LiveDestination = "aeron:udp?endpoint=localhost:9999"
	p.ReplicationChannel = "aeron:udp?endpoint=localhost:0"
	p.ChannelTagID = 11
	p.SubscriptionTagID = 12
	p.FileIoMaxLength = 65536
	p.ReplicationSessionID = 77
	p.EncodedCredentials = []byte("admin:admin")
	p.SrcResponseChannel = "aeron:udp?control-mode=response|control=localhost:10002"

	packet, err := codecs.ReplicateRequest2FullPacket(codecs.NewSbeGoMarshaller(), true, 5, 6, p.toCodec(1, 10, "aeron:udp?endpoint=localhost:8010"))
	require.NoError(t, err)
	req := decodeReplicateRequest2(t, packet)
	assert.Equal(t, codecs.ReplicateRequest2{
		ControlSessionId:     5,
		CorrelationId:        6,
		SrcRecordingId:       1,
		DstRecordingId:       3,
		StopPosition:         4096,
		ChannelTagId:         11,
		SubscriptionTagId:    12,
		SrcControlStreamId:   10,
		FileIoMaxLength:      65536,
		ReplicationSessionId: 77,
		SrcControlChannel:    []uint8("aeron:udp?endpoint=localhost:8010"),
		LiveDestination:      []uint8(p.LiveDestination),
		ReplicationChannel:   []uint8(p.ReplicationChannel),
		EncodedCredentials:   []uint8("admin:admin"),
		SrcResponseChannel:   []uint8(p.SrcResponseChannel),
	}, *req)
}

func TestReplicateRequest2PacketUsesNullForUnsetFields(t *testing.T) {
	packet, err := codecs.ReplicateRequest2Packet(codecs.NewSbeGoMarshaller(), true, 5, 6, 1, -1, -1, 9, 10, "src", "", "")
	require.NoError(t, err)
	req := decodeReplicateRequest2(t, packet)
	assert.Equal(t, int64(9), req.ChannelTagId)
	assert.Equal(t, int64(-1), req.SubscriptionTagId)
	assert.Equal(t, int32(-1), req.FileIoMaxLength)
	assert.Equal(t, int32(-1), req.ReplicationSessionId)
}

func TestMaxRecordedPositionRequestPacket(t *testing.T) {
	packet, err := codecs.MaxRecordedPositionRequestPacket(codecs.NewSbeGoMarshaller(), true, 5, 6, 42)
	require.NoError(t, err)
	m := codecs.NewSbeGoMarshaller()
	r := bytes.NewReader(packet)
	var hdr codecs.MessageHeader
	require.NoError(t, hdr.Decode(m, r, 0))
	var req codecs.MaxRecordedPositionRequest
	assert.Equal(t, req.SbeTemplateId(), hdr.TemplateId)
	require.NoError(t, req.Decode(m, r, hdr.Version, hdr.BlockLength, true))
	assert.Equal(t, codecs.MaxRecordedPositionRequest{ControlSessionId: 5, CorrelationId: 6, RecordingId: 42}, req)
}

func newPollerTestArchive() (*Archive, *[]*codecs.RecordingSignalEvent, *[]error) {
	signals := &[]*codecs.RecordingSignalEvent{}
	errs := &[]error{}
	a := &Archive{SessionID: 5, Options: DefaultOptions(), Control: &Control{}}
	a.Listeners = &ArchiveListeners{
		RecordingSignalListener: func(e *codecs.RecordingSignalEvent) { *signals = append(*signals, e) },
		ErrorListener:           func(err error) { *errs = append(*errs, err) },
	}
	return a, signals, errs
}

func feed(t *testing.T, p *descriptorPoller, msg sbeMessage) term.ControlledPollAction {
	b := encodeMessage(t, msg)
	return p.onFragment(b, 0, b.Capacity(), nil)
}

func TestDescriptorPollerRecordingDescriptors(t *testing.T) {
	a, signals, errs := newPollerTestArchive()
	var got []int64
	p := &descriptorPoller{archive: a, correlationID: 9, remaining: 3, unknownTerminalCode: codecs.ControlResponseCode.RECORDING_UNKNOWN,
		recordingConsumer: func(d *codecs.RecordingDescriptor) { got = append(got, d.RecordingId) }}

	assert.Equal(t, term.ControlledPollAction(term.ControlledPollActionContinue), feed(t, p, &codecs.RecordingDescriptor{ControlSessionId: 5, CorrelationId: 9, RecordingId: 1}))
	// Other session / other correlation are ignored.
	feed(t, p, &codecs.RecordingDescriptor{ControlSessionId: 6, CorrelationId: 9, RecordingId: 100})
	feed(t, p, &codecs.RecordingDescriptor{ControlSessionId: 5, CorrelationId: 8, RecordingId: 101})
	// Signals are dispatched only for our session.
	feed(t, p, &codecs.RecordingSignalEvent{ControlSessionId: 5, RecordingId: 1, Signal: codecs.RecordingSignal.REPLICATE_END})
	feed(t, p, &codecs.RecordingSignalEvent{ControlSessionId: 6, RecordingId: 1, Signal: codecs.RecordingSignal.STOP})
	// Uncorrelated errors go to the error listener without terminating.
	feed(t, p, &codecs.ControlResponse{ControlSessionId: 5, CorrelationId: 1, Code: codecs.ControlResponseCode.ERROR})
	feed(t, p, &codecs.RecordingDescriptor{ControlSessionId: 5, CorrelationId: 9, RecordingId: 2})
	assert.False(t, p.isDispatchComplete)
	// RECORDING_UNKNOWN for our correlation ends the listing early.
	assert.Equal(t, term.ControlledPollAction(term.ControlledPollActionBreak), feed(t, p, &codecs.ControlResponse{ControlSessionId: 5, CorrelationId: 9, Code: codecs.ControlResponseCode.RECORDING_UNKNOWN, RelevantId: 3}))
	assert.True(t, p.isDispatchComplete)
	assert.NoError(t, p.err)
	assert.Equal(t, int32(1), p.remaining)
	assert.Equal(t, []int64{1, 2}, got)
	require.Len(t, *signals, 1)
	assert.Equal(t, codecs.RecordingSignal.REPLICATE_END, (*signals)[0].Signal)
	assert.Len(t, *errs, 1)
	assert.Equal(t, term.ControlledPollAction(term.ControlledPollActionAbort), feed(t, p, &codecs.RecordingDescriptor{ControlSessionId: 5, CorrelationId: 9, RecordingId: 3}))
}

func TestDescriptorPollerCompletesOnCount(t *testing.T) {
	a, _, _ := newPollerTestArchive()
	n := 0
	p := &descriptorPoller{archive: a, correlationID: 9, remaining: 2, unknownTerminalCode: codecs.ControlResponseCode.RECORDING_UNKNOWN,
		recordingConsumer: func(*codecs.RecordingDescriptor) { n++ }}
	feed(t, p, &codecs.RecordingDescriptor{ControlSessionId: 5, CorrelationId: 9})
	assert.Equal(t, term.ControlledPollAction(term.ControlledPollActionBreak), feed(t, p, &codecs.RecordingDescriptor{ControlSessionId: 5, CorrelationId: 9}))
	assert.True(t, p.isDispatchComplete)
	assert.Equal(t, 2, n)
}

func TestDescriptorPollerSubscriptionDescriptors(t *testing.T) {
	a, _, _ := newPollerTestArchive()
	var got []int64
	p := &descriptorPoller{archive: a, correlationID: 9, remaining: 5, unknownTerminalCode: codecs.ControlResponseCode.SUBSCRIPTION_UNKNOWN,
		subscriptionConsumer: func(d *codecs.RecordingSubscriptionDescriptor) { got = append(got, d.SubscriptionId) }}
	feed(t, p, &codecs.RecordingSubscriptionDescriptor{ControlSessionId: 5, CorrelationId: 9, SubscriptionId: 77})
	// RECORDING_UNKNOWN does not terminate a subscription listing; SUBSCRIPTION_UNKNOWN does.
	feed(t, p, &codecs.ControlResponse{ControlSessionId: 5, CorrelationId: 9, Code: codecs.ControlResponseCode.RECORDING_UNKNOWN})
	assert.False(t, p.isDispatchComplete)
	feed(t, p, &codecs.ControlResponse{ControlSessionId: 5, CorrelationId: 9, Code: codecs.ControlResponseCode.SUBSCRIPTION_UNKNOWN})
	assert.True(t, p.isDispatchComplete)
	assert.Equal(t, []int64{77}, got)
}

func TestDescriptorPollerCorrelatedError(t *testing.T) {
	a, _, errs := newPollerTestArchive()
	p := &descriptorPoller{archive: a, correlationID: 9, remaining: 5, unknownTerminalCode: codecs.ControlResponseCode.RECORDING_UNKNOWN,
		recordingConsumer: func(*codecs.RecordingDescriptor) {}}
	assert.Equal(t, term.ControlledPollAction(term.ControlledPollActionBreak), feed(t, p, &codecs.ControlResponse{ControlSessionId: 5, CorrelationId: 9, Code: codecs.ControlResponseCode.ERROR, ErrorMessage: []byte("boom")}))
	assert.ErrorContains(t, p.err, "boom")
	assert.Empty(t, *errs)
}
