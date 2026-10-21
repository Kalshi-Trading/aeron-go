package client

import (
	"errors"
	"testing"

	"github.com/lirm/aeron-go/aeron"
	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/logbuffer"
	"github.com/lirm/aeron-go/aeron/logbuffer/term"
	"github.com/lirm/aeron-go/cluster/codecs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func encode(t *testing.T, msg sbeEncoder) *atomic.Buffer {
	t.Helper()
	data, err := encodeMessage(msg)
	require.NoError(t, err)
	return atomic.MakeBuffer(data)
}

func TestProtocolSemanticVersion(t *testing.T) {
	assert.Equal(t, int32(0x000300), ProtocolSemanticVersion)
	assert.Equal(t, uint16(111), schemaId)
	assert.Equal(t, uint16(17), schemaVersion)
}

func TestParseIngressEndpoints(t *testing.T) {
	m, err := ParseIngressEndpoints("0=localhost:9002,1=host1:9102,2=10.0.0.1:9202")
	require.NoError(t, err)
	assert.Equal(t, map[int32]string{0: "localhost:9002", 1: "host1:9102", 2: "10.0.0.1:9202"}, m)

	m, err = ParseIngressEndpoints("")
	require.NoError(t, err)
	assert.Empty(t, m)

	_, err = ParseIngressEndpoints("0=localhost:9002,localhost:9102")
	assert.True(t, errors.Is(err, ErrConfiguration))
	_, err = ParseIngressEndpoints("x=localhost:9002")
	assert.True(t, errors.Is(err, ErrConfiguration))
}

func TestIngressChannelForEndpoint(t *testing.T) {
	ctx := NewContext()
	ctx.IngressChannel = "aeron:udp?term-length=64k"
	ch, err := ingressChannelForEndpoint(ctx, "localhost:9002")
	require.NoError(t, err)
	uri, err := aeron.ParseChannelUri(ch)
	require.NoError(t, err)
	assert.Equal(t, "localhost:9002", uri.Get("endpoint"))
	assert.Equal(t, "64k", uri.Get("term-length"))

	ctx.IngressChannel = "aeron:ipc"
	ch, err = ingressChannelForEndpoint(ctx, "localhost:9002")
	require.NoError(t, err)
	assert.Equal(t, "aeron:ipc", ch)
}

func concludeWithoutDriver(ctx *Context) error {
	ctx.Aeron = &aeron.Aeron{} // never used by Conclude when non-nil
	return ctx.Conclude()
}

func TestContextConclude(t *testing.T) {
	ctx := NewContext()
	assert.True(t, ctx.IsIngressExclusive)
	assert.Equal(t, DefaultIngressStreamId, ctx.IngressStreamId)
	assert.Equal(t, DefaultEgressStreamId, ctx.EgressStreamId)
	assert.True(t, errors.Is(concludeWithoutDriver(ctx), ErrConfiguration), "ingress channel required")

	ctx = NewContext()
	ctx.IngressChannel = "aeron:ipc"
	ctx.IngressEndpoints = "0=localhost:9002"
	assert.True(t, errors.Is(concludeWithoutDriver(ctx), ErrConfiguration))

	ctx = NewContext()
	ctx.IngressChannel = "aeron:udp"
	ctx.EgressChannel = ""
	assert.True(t, errors.Is(concludeWithoutDriver(ctx), ErrConfiguration))

	ctx = NewContext()
	ctx.IngressChannel = "aeron:udp"
	ctx.ClientName = string(make([]byte, MaxClientNameLength+1))
	assert.True(t, errors.Is(concludeWithoutDriver(ctx), ErrConfiguration))

	ctx = NewContext()
	ctx.IngressChannel = "aeron:udp"
	require.NoError(t, concludeWithoutDriver(ctx))
	assert.True(t, ctx.IsConcluded())
	uri, err := aeron.ParseChannelUri(ctx.EgressChannel)
	require.NoError(t, err)
	assert.Equal(t, "false", uri.Get("rejoin"))
	assert.NotNil(t, ctx.IdleStrategy)
	assert.NotNil(t, ctx.CredentialsSupplier)
	assert.NotNil(t, ctx.EgressListener)
	assert.NotNil(t, ctx.ControlledEgressListener)
	assert.False(t, ctx.OwnsAeronClient)
	assert.Equal(t, 2*DefaultLeaderHeartbeatTimeout, ctx.newLeaderTimeout())
	assert.Error(t, ctx.Conclude(), "double conclude")
}

func TestCredentialsSuppliers(t *testing.T) {
	assert.Nil(t, NullCredentialsSupplier{}.EncodedCredentials())
	s := StaticCredentialsSupplier{Credentials: []byte("user"), ChallengeResponse: []byte("resp")}
	assert.Equal(t, []byte("user"), s.EncodedCredentials())
	assert.Equal(t, []byte("resp"), s.OnChallenge([]byte("c")))
	f := CredentialsSupplierFuncs{Challenge: func(c []byte) []byte { return append([]byte("re:"), c...) }}
	assert.Nil(t, f.EncodedCredentials())
	assert.Equal(t, []byte("re:c"), f.OnChallenge([]byte("c")))
}

func TestAsyncConnectStateNames(t *testing.T) {
	assert.Equal(t, -1, int(AsyncConnectCreateEgressSubscription))
	assert.Equal(t, 5, int(AsyncConnectDone))
	assert.Equal(t, "CREATE_EGRESS_SUBSCRIPTION", StepName(-1))
	assert.Equal(t, "POLL_RESPONSE", StepName(3))
	assert.Equal(t, "DONE", StepName(5))
	assert.Equal(t, "<unknown>", StepName(6))
}

func TestIngressSessionDecoratorHeader(t *testing.T) {
	d := NewIngressSessionDecorator(42, 7)
	h := d.headerBuffer
	assert.Equal(t, uint16(24), h.GetUInt16(0))
	assert.Equal(t, uint16(1), h.GetUInt16(2))
	assert.Equal(t, uint16(111), h.GetUInt16(4))
	assert.Equal(t, uint16(17), h.GetUInt16(6))
	assert.Equal(t, int64(7), h.GetInt64(8))
	assert.Equal(t, int64(42), h.GetInt64(16))
	assert.Equal(t, int64(-1), h.GetInt64(24))
	d.SetClusterSessionId(5).SetLeadershipTermId(6)
	assert.Equal(t, int64(6), h.GetInt64(8))
	assert.Equal(t, int64(5), h.GetInt64(16))
}

type recordingExtension struct {
	templateId, schemaId uint16
	length               int32
}

func (r *recordingExtension) OnExtensionMessage(_ uint16, templateId uint16, schemaId uint16, _ uint16,
	_ *atomic.Buffer, _ int32, length int32) term.ControlledPollAction {
	r.templateId, r.schemaId, r.length = templateId, schemaId, length
	return term.ControlledPollActionBreak
}

func sessionMessage(sessionId, leadershipTermId, timestamp int64, payload string) *atomic.Buffer {
	buf := atomic.MakeBuffer(make([]byte, int(SessionHeaderLength)+len(payload)))
	writeMessageHeader(buf, 0, sessionMessageHeaderBlockLength, sessionMessageHeaderTemplateId)
	buf.PutInt64(8, leadershipTermId)
	buf.PutInt64(16, sessionId)
	buf.PutInt64(24, timestamp)
	p := []byte(payload)
	buf.PutBytesSlice(SessionHeaderLength, p)
	return buf
}

type messageRecorder struct {
	recordingListener
	timestamps []int64
}

func (m *messageRecorder) OnMessage(s int64, ts int64, buffer *atomic.Buffer, offset int32, length int32,
	h *logbuffer.Header) {
	m.timestamps = append(m.timestamps, ts)
	m.recordingListener.OnMessage(s, ts, buffer, offset, length, h)
}

func TestDispatchEgress(t *testing.T) {
	l := &messageRecorder{}
	var errs []error
	errorHandler := func(err error) { errs = append(errs, err) }
	dispatch := func(buf *atomic.Buffer, ext ControlledEgressListenerExtension) term.ControlledPollAction {
		return dispatchEgress(9, buf, 0, buf.Capacity(), nil, uncontrolled{l}, ext, nil, nil, errorHandler)
	}

	dispatch(sessionMessage(9, 1, 1234, "hello"), nil)
	dispatch(sessionMessage(10, 1, 1, "other session"), nil)
	require.Len(t, l.messages, 1)
	assert.Equal(t, "hello", string(l.messages[0]))
	assert.Equal(t, int64(1234), l.timestamps[0])

	dispatch(encode(t, &codecs.SessionEvent{ClusterSessionId: 9, CorrelationId: 3, LeadershipTermId: 1,
		LeaderMemberId: 0, Code: codecs.EventCode.CLOSED, Detail: []byte("bye")}), nil)
	assert.Equal(t, []codecs.EventCodeEnum{codecs.EventCode.CLOSED}, l.sessionEvents)

	dispatch(encode(t, &codecs.NewLeaderEvent{ClusterSessionId: 9, LeadershipTermId: 2, LeaderMemberId: 1,
		IngressEndpoints: []byte("0=a:1,1=b:2")}), nil)
	assert.Equal(t, []int32{1}, l.newLeaders)

	adminBuf := encode(t, &codecs.AdminResponse{ClusterSessionId: 9, CorrelationId: 77,
		RequestType: codecs.AdminRequestType.SNAPSHOT, ResponseCode: codecs.AdminResponseCode.OK,
		Message: []byte("msg"), Payload: []byte("PAYLOAD")})
	var gotPayload string
	pl := &payloadListener{onPayload: func(b *atomic.Buffer, off, length int32) {
		gotPayload = string(b.GetBytesArray(off, length))
	}}
	dispatchEgress(9, adminBuf, 0, adminBuf.Capacity(), nil, uncontrolled{pl}, nil, nil, nil, errorHandler)
	assert.Equal(t, "PAYLOAD", gotPayload)
	assert.Equal(t, "msg", pl.message)
	assert.Equal(t, int64(77), pl.correlationId)

	// Foreign schema: extension or error.
	foreign := atomic.MakeBuffer(make([]byte, 16))
	foreign.PutUInt16(0, 8)
	foreign.PutUInt16(2, 3)
	foreign.PutUInt16(4, 500)
	assert.Equal(t, term.ControlledPollAction(term.ControlledPollActionContinue), dispatch(foreign, nil))
	require.Len(t, errs, 1)
	assert.True(t, errors.Is(errs[0], ErrCluster))
	ext := &recordingExtension{}
	assert.Equal(t, term.ControlledPollAction(term.ControlledPollActionBreak), dispatch(foreign, ext))
	assert.Equal(t, uint16(500), ext.schemaId)
	assert.Equal(t, uint16(3), ext.templateId)
	assert.Equal(t, int32(8), ext.length)
}

type payloadListener struct {
	NoOpEgressEvents
	onPayload     func(*atomic.Buffer, int32, int32)
	message       string
	correlationId int64
}

func (p *payloadListener) OnMessage(int64, int64, *atomic.Buffer, int32, int32, *logbuffer.Header) {}

func (p *payloadListener) OnAdminResponse(_ int64, correlationId int64, _ codecs.AdminRequestTypeEnum,
	_ codecs.AdminResponseCodeEnum, message string, payload *atomic.Buffer, off int32, length int32) {
	p.message = message
	p.correlationId = correlationId
	p.onPayload(payload, off, length)
}

func TestEgressPollerDecodesEvents(t *testing.T) {
	p := NewEgressPoller(nil, 10)
	buf := encode(t, &codecs.SessionEvent{ClusterSessionId: 5, CorrelationId: 6, LeadershipTermId: 7,
		LeaderMemberId: 2, Code: codecs.EventCode.OK, Version: ProtocolSemanticVersion,
		LeaderHeartbeatTimeoutNs: 2_000_000_000, Detail: []byte("")})
	assert.Equal(t, term.ControlledPollAction(term.ControlledPollActionBreak),
		p.onFragment(buf, 0, buf.Capacity(), nil))
	assert.True(t, p.IsPollComplete())
	assert.Equal(t, int64(5), p.ClusterSessionId())
	assert.Equal(t, int64(6), p.CorrelationId())
	assert.Equal(t, int64(7), p.LeadershipTermId())
	assert.Equal(t, int32(2), p.LeaderMemberId())
	assert.Equal(t, codecs.EventCode.OK, p.EventCode())
	assert.Equal(t, int64(2_000_000_000), p.LeaderHeartbeatTimeoutNs())
	assert.False(t, p.IsChallenged())
	// A complete poll aborts further fragments until reset.
	assert.Equal(t, term.ControlledPollAction(term.ControlledPollActionAbort),
		p.onFragment(buf, 0, buf.Capacity(), nil))

	p.reset()
	buf = encode(t, &codecs.SessionEvent{ClusterSessionId: 5, CorrelationId: 6, Code: codecs.EventCode.REDIRECT,
		LeaderHeartbeatTimeoutNs: (&codecs.SessionEvent{}).LeaderHeartbeatTimeoutNsNullValue(),
		Detail:                   []byte("0=a:1")})
	p.onFragment(buf, 0, buf.Capacity(), nil)
	assert.Equal(t, int64(nullValue), p.LeaderHeartbeatTimeoutNs())
	assert.Equal(t, "0=a:1", p.Detail())

	p.reset()
	buf = encode(t, &codecs.Challenge{CorrelationId: 8, ClusterSessionId: 9, EncodedChallenge: []byte("chal")})
	p.onFragment(buf, 0, buf.Capacity(), nil)
	assert.True(t, p.IsChallenged())
	assert.Equal(t, []byte("chal"), p.EncodedChallenge())
	assert.Equal(t, int64(8), p.CorrelationId())

	p.reset()
	buf = encode(t, &codecs.NewLeaderEvent{ClusterSessionId: 1, LeadershipTermId: 3, LeaderMemberId: 1,
		IngressEndpoints: []byte("1=b:2")})
	p.onFragment(buf, 0, buf.Capacity(), nil)
	assert.Equal(t, int32(1), p.LeaderMemberId())
	assert.Equal(t, "1=b:2", p.Detail())

	p.reset()
	assert.False(t, p.IsPollComplete())
	assert.Equal(t, codecs.EventCode.NullValue, p.EventCode())
	assert.Equal(t, int32(nullValue), p.TemplateId())
}

func TestEncodeConnectRequest(t *testing.T) {
	data, err := encodeMessage(&codecs.SessionConnectRequest{CorrelationId: 1, ResponseStreamId: 102,
		Version: ProtocolSemanticVersion, ResponseChannel: []byte("aeron:udp?endpoint=localhost:1234"),
		EncodedCredentials: []byte("cred"), ClientInfo: []byte("name=x")})
	require.NoError(t, err)
	buf := atomic.MakeBuffer(data)
	hdr := readMessageHeader(buf, 0)
	assert.Equal(t, uint16(3), hdr.templateId)
	assert.Equal(t, uint16(17), hdr.version)
	var decoded codecs.SessionConnectRequest
	require.NoError(t, decodeBody(buf, 0, buf.Capacity(), hdr, &decoded))
	assert.Equal(t, ProtocolSemanticVersion, decoded.Version)
	assert.Equal(t, "cred", string(decoded.EncodedCredentials))
	assert.Equal(t, "name=x", string(decoded.ClientInfo))
}
