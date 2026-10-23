package archive

import (
	"bytes"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/lirm/aeron-go/aeron"
	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/idlestrategy"
	"github.com/lirm/aeron-go/aeron/logbuffer/term"
	"github.com/lirm/aeron-go/aeron/util"
	"github.com/lirm/aeron-go/archive/codecs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOptionsConcludeAppliesControlChannelDefaults(t *testing.T) {
	o := DefaultOptions()
	o.ControlTermBufferLength = 128 * 1024
	o.ControlTermBufferSparse = false
	o.ControlMtuLength = 4096
	c := o.Clone()
	require.NoError(t, c.conclude())
	assert.True(t, c.IsConcluded())
	assert.False(t, o.IsConcluded())
	assert.Equal(t, defaultOptions.RequestChannel, o.RequestChannel, "caller options must not be mutated")

	for _, ch := range []string{c.RequestChannel, c.ResponseChannel} {
		uri, err := aeron.ParseChannelUri(ch)
		require.NoError(t, err)
		assert.Equal(t, "131072", uri.Get(aeron.TermLengthParamName))
		assert.Equal(t, "4096", uri.Get(aeron.MtuLengthParamName))
		assert.Equal(t, "false", uri.Get(aeron.SparseParamName))
	}
	assert.NotContains(t, c.RequestChannel, "session-id", "session-id comes from the driver in NewAsyncConnect")
	assert.NotNil(t, c.Lock)
	assert.NotNil(t, c.IdleStrategy)
	assert.Equal(t, MessageRetryAttemptsDefault, c.MessageRetryAttempts)
	assert.NotNil(t, c.CredentialsSupplier)
	assert.Error(t, c.conclude(), "double conclude")
}

func TestOptionsConcludeKeepsExplicitParams(t *testing.T) {
	o := DefaultOptions()
	o.RequestChannel = "aeron:udp?endpoint=localhost:8010|term-length=1m|mtu=8k|sparse=false|session-id=7"
	require.NoError(t, o.conclude())
	uri, _ := aeron.ParseChannelUri(o.RequestChannel)
	assert.Equal(t, "1m", uri.Get(aeron.TermLengthParamName))
	assert.Equal(t, "8k", uri.Get(aeron.MtuLengthParamName))
	assert.Equal(t, "false", uri.Get(aeron.SparseParamName))
}

func TestOptionsApplySessionIDOverwritesLikeJava(t *testing.T) {
	o := DefaultOptions()
	o.RequestChannel = "aeron:udp?endpoint=localhost:8010|session-id=7"
	require.NoError(t, o.conclude())
	var stream int32
	require.NoError(t, o.applySessionID(func(s int32) (int32, error) { stream = s; return -42, nil }, v153))
	assert.Equal(t, o.RequestStream, stream)
	req, _ := aeron.ParseChannelUri(o.RequestChannel)
	resp, _ := aeron.ParseChannelUri(o.ResponseChannel)
	assert.Equal(t, "-42", req.Get(aeron.SessionIdParamName))
	assert.Equal(t, "-42", resp.Get(aeron.SessionIdParamName))

	boom := errors.New("boom")
	o = DefaultOptions()
	require.NoError(t, o.conclude())
	assert.ErrorIs(t, o.applySessionID(func(int32) (int32, error) { return 0, boom }, v153), boom)
}

var v153 = util.SemanticVersionCompose(1, 53, 3)

func TestOptionsApplySessionIDSkipsOldOrUnknownDriver(t *testing.T) {
	for _, v := range []uint32{0, util.SemanticVersionCompose(1, 43, 0)} {
		o := DefaultOptions()
		require.NoError(t, o.conclude())
		req, resp := o.RequestChannel, o.ResponseChannel
		require.NoError(t, o.applySessionID(func(int32) (int32, error) { panic("must not allocate") }, v))
		assert.Equal(t, req, o.RequestChannel)
		assert.Equal(t, resp, o.ResponseChannel)
	}
}

func TestOptionsDefaultsMatchJava(t *testing.T) {
	o := DefaultOptions()
	assert.Equal(t, 10*time.Second, o.Timeout, "Java MESSAGE_TIMEOUT_DEFAULT_NS")
	assert.Equal(t, 3, o.MessageRetryAttempts, "Java MESSAGE_RETRY_ATTEMPTS_DEFAULT")
	require.NoError(t, o.conclude())
	_, isBackoff := o.IdleStrategy.(*idlestrategy.BackoffIdleStrategy)
	assert.True(t, isBackoff, "Java defaults to a BackoffIdleStrategy, got %T", o.IdleStrategy)
}

func TestOptionsConcludeResponseModeHasNoSessionID(t *testing.T) {
	o := DefaultOptions()
	o.ResponseChannel = "aeron:udp?control-mode=response|control=localhost:10002"
	require.NoError(t, o.conclude())
	require.NoError(t, o.applySessionID(func(int32) (int32, error) { panic("must not allocate") }, v153))
	assert.NotContains(t, o.RequestChannel, "session-id")
	assert.NotContains(t, o.ResponseChannel, "session-id")
}

func TestOptionsConcludeValidation(t *testing.T) {
	for name, mutate := range map[string]func(*Options){
		"request":  func(o *Options) { o.RequestChannel = "" },
		"response": func(o *Options) { o.ResponseChannel = "" },
		"timeout":  func(o *Options) { o.Timeout = 0 },
		"retries":  func(o *Options) { o.MessageRetryAttempts = -1 },
		"name":     func(o *Options) { o.ClientName = strings.Repeat("x", MaxClientNameLength+1) },
		"uri":      func(o *Options) { o.RequestChannel = "not-a-uri" },
	} {
		o := DefaultOptions()
		mutate(o)
		assert.Error(t, o.conclude(), name)
	}
}

func TestSemanticVersion(t *testing.T) {
	v := SemanticVersionCompose(1, 12, 3)
	assert.Equal(t, int32(1), SemanticVersionMajor(v))
	assert.Equal(t, int32(12), SemanticVersionMinor(v))
	assert.Equal(t, int32(3), SemanticVersionPatch(v))
	assert.Equal(t, "1.12.3", SemanticVersionString(v))
	assert.Equal(t, SemanticVersionCompose(1, 12, 0), ProtocolSemanticVersion)
	assert.Equal(t, int32(1), ProtocolMajorVersion)
}

func TestAsyncConnectStateString(t *testing.T) {
	assert.Equal(t, "AWAIT_SUBSCRIPTION", AsyncConnectAwaitSubscription.String())
	assert.Equal(t, "DONE", AsyncConnectDone.String())
	assert.Equal(t, 10, int(AsyncConnectDone))
	assert.Equal(t, "UNKNOWN(42)", AsyncConnectState(42).String())
}

func TestCredentialsSuppliers(t *testing.T) {
	assert.Nil(t, NullCredentialsSupplier{}.EncodedCredentials())
	s := StaticCredentialsSupplier{Credentials: []byte("a"), ChallengeResponse: []byte("b")}
	assert.Equal(t, []byte("a"), s.EncodedCredentials())
	assert.Equal(t, []byte("b"), s.OnChallenge([]byte("x")))
	f := CredentialsSupplierFuncs{Challenge: func(c []byte) []byte { return append([]byte("re:"), c...) }}
	assert.Nil(t, f.EncodedCredentials())
	assert.Equal(t, []byte("re:x"), f.OnChallenge([]byte("x")))
}

func encodeMessage(t *testing.T, msg interface {
	Encode(*codecs.SbeGoMarshaller, *bytes.Buffer, bool) error
}, blockLength, templateID uint16) *atomic.Buffer {
	m := codecs.NewSbeGoMarshaller()
	buf := new(bytes.Buffer)
	hdr := codecs.MessageHeader{BlockLength: blockLength, TemplateId: templateID, SchemaId: 101,
		Version: (*codecs.ControlResponse)(nil).SbeSchemaVersion()}
	require.NoError(t, hdr.Encode(m, buf))
	require.NoError(t, msg.Encode(m, buf, false))
	b := buf.Bytes()
	return atomic.MakeBuffer(b, len(b))
}

type controlResponseMsg struct{ codecs.ControlResponse }

func (c *controlResponseMsg) Encode(m *codecs.SbeGoMarshaller, w *bytes.Buffer, r bool) error {
	return c.ControlResponse.Encode(m, w, r)
}

type challengeMsg struct{ codecs.Challenge }

func (c *challengeMsg) Encode(m *codecs.SbeGoMarshaller, w *bytes.Buffer, r bool) error {
	return c.Challenge.Encode(m, w, r)
}

func TestConnectResponsePollerDecodes(t *testing.T) {
	p := newConnectResponsePoller(nil)
	p.reset()

	cr := &controlResponseMsg{codecs.ControlResponse{ControlSessionId: 5, CorrelationId: 9, RelevantId: 77,
		Code: codecs.ControlResponseCode.ERROR, Version: ProtocolSemanticVersion, ErrorMessage: []byte("nope")}}
	b := encodeMessage(t, cr, cr.SbeBlockLength(), cr.SbeTemplateId())
	assert.Equal(t, term.ControlledPollAction(term.ControlledPollActionBreak), p.onFragment(b, 0, b.Capacity(), nil))
	assert.True(t, p.isPollComplete)
	assert.False(t, p.wasChallenged())
	assert.Equal(t, int64(5), p.controlSessionID)
	assert.Equal(t, int64(9), p.correlationID)
	assert.Equal(t, int64(77), p.relevantID)
	assert.Equal(t, codecs.ControlResponseCode.ERROR, p.code)
	assert.Equal(t, "nope", p.errorMessage)
	assert.Equal(t, term.ControlledPollAction(term.ControlledPollActionAbort), p.onFragment(b, 0, b.Capacity(), nil), "already complete")

	p.reset()
	ch := &challengeMsg{codecs.Challenge{ControlSessionId: 6, CorrelationId: 10, Version: ProtocolSemanticVersion,
		EncodedChallenge: []byte("challenge!")}}
	b = encodeMessage(t, ch, ch.SbeBlockLength(), ch.SbeTemplateId())
	assert.Equal(t, term.ControlledPollAction(term.ControlledPollActionBreak), p.onFragment(b, 0, b.Capacity(), nil))
	assert.True(t, p.wasChallenged())
	assert.Equal(t, []byte("challenge!"), p.encodedChallenge)
	assert.Equal(t, int64(6), p.controlSessionID)

	p.reset()
	ping, err := codecs.KeepAliveRequestPacket(codecs.NewSbeGoMarshaller(), false, 1, 2)
	require.NoError(t, err)
	pb := atomic.MakeBuffer(ping, len(ping))
	assert.Equal(t, term.ControlledPollAction(term.ControlledPollActionContinue), p.onFragment(pb, 0, pb.Capacity(), nil))
	assert.False(t, p.isPollComplete)
}

func TestConnectErrorUnwrap(t *testing.T) {
	err := error(&ConnectError{Message: "x", cause: ErrIncompatibleArchiveVersion})
	assert.True(t, errors.Is(err, ErrIncompatibleArchiveVersion))
	var ce *ConnectError
	assert.True(t, errors.As(err, &ce))
}
