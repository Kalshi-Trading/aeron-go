package archive

import (
	"bytes"
	"errors"
	"io"
	"sync"
	"testing"

	"github.com/lirm/aeron-go/aeron"
	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/logbuffer/term"
	"github.com/lirm/aeron-go/archive/codecs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type capturePublisher struct {
	aeron.Publisher
	offers [][]byte
	result int64
}

func (p *capturePublisher) Offer(buffer *atomic.Buffer, offset int32, length int32, _ term.ReservedValueSupplier) int64 {
	if p.result < 0 {
		return p.result
	}
	p.offers = append(p.offers, append([]byte(nil), buffer.GetBytesArray(offset, length)...))
	return int64(len(p.offers)) * 64
}

func (p *capturePublisher) IsConnected() bool { return true }

func newWireArchive(t *testing.T, frames []testFrame) (*Archive, *capturePublisher) {
	control := newFrameControl(t, frames, &ArchiveListeners{})
	a := control.archive
	a.mtx = &sync.Mutex{}
	a.Control = control
	pub := &capturePublisher{}
	a.Proxy = &Proxy{Publication: pub, archive: a, marshaller: codecs.NewSbeGoMarshaller()}
	return a, pub
}

func decodeRequest(t *testing.T, packet []byte, msg interface {
	Decode(*codecs.SbeGoMarshaller, io.Reader, uint16, uint16, bool) error
	SbeTemplateId() uint16
}) {
	t.Helper()
	m := codecs.NewSbeGoMarshaller()
	r := bytes.NewReader(packet)
	var hdr codecs.MessageHeader
	require.NoError(t, hdr.Decode(m, r, 0))
	require.Equal(t, msg.SbeTemplateId(), hdr.TemplateId)
	require.NoError(t, msg.Decode(m, r, hdr.Version, hdr.BlockLength, true))
}

func TestProxyUpdateChannelRequestEncoding(t *testing.T) {
	a, pub := newWireArchive(t, nil)
	require.NoError(t, a.Proxy.UpdateChannelRequest(11, 42, "aeron:udp?endpoint=localhost:5000"))
	require.Len(t, pub.offers, 1)
	var req codecs.UpdateChannelRequest
	decodeRequest(t, pub.offers[0], &req)
	assert.Equal(t, int64(testSession), req.ControlSessionId)
	assert.Equal(t, int64(11), req.CorrelationId)
	assert.Equal(t, int64(42), req.RecordingId)
	assert.Equal(t, "aeron:udp?endpoint=localhost:5000", string(req.Channel))
}

func TestProxyRefusesRequestsWhenClosed(t *testing.T) {
	a, pub := newWireArchive(t, nil)
	a.state.Store(int32(ArchiveStateClosed))

	_, err := a.StartRecording("aeron:ipc", 1, true, false)
	var ae *ArchiveException
	require.True(t, errors.As(err, &ae), "%v", err)
	assert.Equal(t, "client is closed", ae.Message)
	_, err = a.ListRecordings(0, 10)
	require.True(t, errors.As(err, &ae), "%v", err)
	assert.Empty(t, pub.offers, "nothing may be sent by a closed client")

	require.NoError(t, a.Proxy.CloseSessionRequest(), "Close must still be able to send CloseSession")
	assert.Len(t, pub.offers, 1)
}

func TestProxyOfferFailureMapsToArchiveException(t *testing.T) {
	cases := map[int64]string{
		aeron.PublicationClosed:   "connection to the archive has been closed",
		aeron.MaxPositionExceeded: "offer failed due to max position being reached",
	}
	for result, msg := range cases {
		a, pub := newWireArchive(t, nil)
		pub.result = result
		err := a.Proxy.KeepAliveRequest(1)
		var ae *ArchiveException
		require.True(t, errors.As(err, &ae), "%v", err)
		assert.Equal(t, msg, ae.Message)
	}
}

func TestProxyReplicateRejectsLiveDestinationWithSessionId(t *testing.T) {
	a, pub := newWireArchive(t, nil)
	params := NewReplicationParams()
	params.LiveDestination = "aeron:udp?endpoint=localhost:5001"
	params.ReplicationSessionID = 7
	require.Error(t, a.Proxy.ReplicateRequestWithParams(1, 2, 3, "aeron:udp?endpoint=localhost:8010", params))
	assert.Empty(t, pub.offers)

	params.ReplicationSessionID = -1
	require.NoError(t, a.Proxy.ReplicateRequestWithParams(1, 2, 3, "aeron:udp?endpoint=localhost:8010", params))
	assert.Len(t, pub.offers, 1)
}

func TestListDescriptorsErrorIsArchiveException(t *testing.T) {
	a, _ := newWireArchive(t, nil)
	_, err := a.listDescriptors(5, func(*codecs.RecordingDescriptor) {}, nil, func(correlationID int64) error {
		a.Control.Subscription = newFrameSub(t, whole(t,
			response(testSession, correlationID, int64(ErrorCodeActiveListing), codecs.ControlResponseCode.ERROR, "busy")))
		return nil
	})
	var ae *ArchiveException
	require.True(t, errors.As(err, &ae), "%v", err)
	assert.Equal(t, ErrorCodeActiveListing, ae.ErrorCode)
}

var _ func(aeron.Publisher) error = (*Archive)(nil).StopRecordingByPublisher
