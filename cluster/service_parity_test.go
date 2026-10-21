package cluster

import (
	"bytes"
	"path/filepath"
	"testing"

	"github.com/lirm/aeron-go/aeron"
	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/logbuffer"
	"github.com/lirm/aeron-go/aeron/util"
	"github.com/lirm/aeron-go/cluster/codecs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type recordingHandler struct {
	joinLogs       []bool // isStandby
	joinRoles      []Role
	termPos        int64
	termAck        bool
	requestedAck   int64
	sessionMsgs    [][]byte
	sessionIds     []int64
	actions        []int32
	timers         []int64
	leadershipApps []int32
}

func (h *recordingHandler) onJoinLog(_, _ int64, _, _, _ int32, _ bool, role Role, _ string, isStandby bool) {
	h.joinLogs = append(h.joinLogs, isStandby)
	h.joinRoles = append(h.joinRoles, role)
}
func (h *recordingHandler) onServiceTerminationPosition(p int64, ack bool) {
	h.termPos, h.termAck = p, ack
}
func (h *recordingHandler) onRequestServiceAck(p int64) { h.requestedAck = p }
func (h *recordingHandler) onTimerEvent(_, correlationId, _ int64) {
	h.timers = append(h.timers, correlationId)
}
func (h *recordingHandler) onSessionOpen(int64, int64, int64, int64, int32, string, []byte)   {}
func (h *recordingHandler) onSessionClose(int64, int64, int64, int64, codecs.CloseReasonEnum) {}
func (h *recordingHandler) onServiceAction(_, _, _ int64, _ codecs.ClusterActionEnum, flags int32) {
	h.actions = append(h.actions, flags)
}
func (h *recordingHandler) onNewLeadershipTermEvent(_, _, _, _ int64, _, _ int32, _ codecs.ClusterTimeUnitEnum,
	appVersion int32) {
	h.leadershipApps = append(h.leadershipApps, appVersion)
}
func (h *recordingHandler) onSessionMessage(_, clusterSessionId, _ int64, buffer *atomic.Buffer, offset,
	length int32, _ *logbuffer.Header) {
	h.sessionIds = append(h.sessionIds, clusterSessionId)
	h.sessionMsgs = append(h.sessionMsgs, buffer.GetBytesArray(offset, length))
}

func encodeSbe(t *testing.T, templateId, blockLength uint16, encode func(*codecs.SbeGoMarshaller, *bytes.Buffer) error) []byte {
	t.Helper()
	body := &bytes.Buffer{}
	require.NoError(t, encode(codecs.NewSbeGoMarshaller(), body))
	b := atomic.MakeBuffer(make([]byte, SBEHeaderLength+body.Len()))
	b.PutUInt16(0, blockLength)
	b.PutUInt16(2, templateId)
	b.PutUInt16(4, ClusterSchemaId)
	b.PutUInt16(6, ClusterSchemaVersion)
	raw := body.Bytes()
	b.PutBytesSlice(SBEHeaderLength, raw)
	return b.GetBytesArray(0, b.Capacity())
}

// termWriter lays fragments out in a fake term buffer so a logbuffer.Header can be pointed at each frame.
type termWriter struct {
	term   *atomic.Buffer
	offset int32
}

func newTermWriter() *termWriter {
	return &termWriter{term: atomic.MakeBuffer(make([]byte, 64*1024))}
}

func (w *termWriter) frame(payload []byte, flags uint8) (*atomic.Buffer, int32, int32, *logbuffer.Header) {
	frameOffset := w.offset
	frameLength := logbuffer.DataFrameHeader.Length + int32(len(payload))
	w.term.PutInt32(frameOffset+logbuffer.DataFrameHeader.FrameLengthFieldOffset, frameLength)
	w.term.PutUInt8(frameOffset+logbuffer.DataFrameHeader.FlagsFieldOffset, flags)
	dataOffset := frameOffset + logbuffer.DataFrameHeader.Length
	if len(payload) > 0 {
		w.term.PutBytesSlice(dataOffset, payload)
	}
	w.offset += util.AlignInt32(frameLength, logbuffer.FrameAlignment)
	hdr := &logbuffer.Header{}
	hdr.Wrap(w.term.Ptr(), w.term.Capacity())
	hdr.SetOffset(frameOffset)
	return w.term, dataOffset, int32(len(payload)), hdr
}

func (w *termWriter) skip(n int32) { w.offset += n }

func TestServiceAdapterDecodesV17Messages(t *testing.T) {
	h := &recordingHandler{}
	adapter := &serviceAdapter{marshaller: codecs.NewSbeGoMarshaller(), handler: h}
	w := newTermWriter()

	jl := codecs.JoinLog{LogPosition: 64, MaxLogPosition: 128, MemberId: 0, LogSessionId: 7, LogStreamId: 100,
		IsStartup: codecs.BooleanType.TRUE, Role: int32(Leader), IsStandby: codecs.BooleanType.TRUE,
		LogChannel: []uint8("aeron:ipc")}
	msg := encodeSbe(t, uint16(jl.SbeTemplateId()), uint16(jl.SbeBlockLength()),
		func(m *codecs.SbeGoMarshaller, b *bytes.Buffer) error { return jl.Encode(m, b, true) })
	adapter.onFragment(w.frame(msg, 0xC0))
	require.Equal(t, []bool{true}, h.joinLogs)
	assert.Equal(t, Leader, h.joinRoles[0])

	tp := codecs.ServiceTerminationPosition{LogPosition: 1024, AckRequired: codecs.BooleanType.FALSE}
	msg = encodeSbe(t, uint16(tp.SbeTemplateId()), uint16(tp.SbeBlockLength()),
		func(m *codecs.SbeGoMarshaller, b *bytes.Buffer) error { return tp.Encode(m, b, true) })
	adapter.onFragment(w.frame(msg, 0xC0))
	assert.Equal(t, int64(1024), h.termPos)
	assert.False(t, h.termAck)

	ra := codecs.RequestServiceAck{LogPosition: 2048}
	msg = encodeSbe(t, uint16(ra.SbeTemplateId()), uint16(ra.SbeBlockLength()),
		func(m *codecs.SbeGoMarshaller, b *bytes.Buffer) error { return ra.Encode(m, b, true) })
	adapter.onFragment(w.frame(msg, 0xC0))
	assert.Equal(t, int64(2048), h.requestedAck)
}

func TestServiceAdapterRejectsWrongSchema(t *testing.T) {
	adapter := &serviceAdapter{marshaller: codecs.NewSbeGoMarshaller(), handler: &recordingHandler{}}
	msg := make([]byte, 16)
	msg[4] = 1 // schema id 1
	assert.Panics(t, func() { adapter.onFragment(newTermWriter().frame(msg, 0xC0)) })
}

func sessionMessage(t *testing.T, clusterSessionId int64, payload []byte) []byte {
	smh := codecs.SessionMessageHeader{LeadershipTermId: 1, ClusterSessionId: clusterSessionId, Timestamp: 5}
	hdr := encodeSbe(t, uint16(smh.SbeTemplateId()), uint16(smh.SbeBlockLength()),
		func(m *codecs.SbeGoMarshaller, b *bytes.Buffer) error { return smh.Encode(m, b, true) })
	return append(hdr, payload...)
}

func TestBoundedLogAdapterReassemblesFragments(t *testing.T) {
	h := &recordingHandler{}
	adapter := newBoundedLogAdapter(NewOptions(), h)
	w := newTermWriter()
	payload := bytes.Repeat([]byte("abcdefgh"), 40)
	msg := sessionMessage(t, 99, payload)

	adapter.onFragment(w.frame(msg[:100], 0x80))
	adapter.onFragment(w.frame(msg[100:200], 0x00))
	assert.Empty(t, h.sessionMsgs)
	adapter.onFragment(w.frame(msg[200:], 0x40))
	require.Len(t, h.sessionMsgs, 1)
	assert.Equal(t, int64(99), h.sessionIds[0])
	assert.Equal(t, payload, h.sessionMsgs[0])

	// unfragmented
	adapter.onFragment(w.frame(sessionMessage(t, 3, []byte{1, 2, 3}), 0xC0))
	require.Len(t, h.sessionMsgs, 2)
	assert.Equal(t, []byte{1, 2, 3}, h.sessionMsgs[1])
}

func TestBoundedLogAdapterDropsMessageWithFragmentGap(t *testing.T) {
	h := &recordingHandler{}
	adapter := newBoundedLogAdapter(NewOptions(), h)
	w := newTermWriter()
	msg := sessionMessage(t, 1, bytes.Repeat([]byte{7}, 200))

	adapter.onFragment(w.frame(msg[:100], 0x80))
	w.skip(logbuffer.FrameAlignment) // lost fragment
	adapter.onFragment(w.frame(msg[100:], 0x40))
	assert.Empty(t, h.sessionMsgs)

	// continuation without a begin is ignored as well
	adapter.onFragment(w.frame(msg[100:], 0x40))
	assert.Empty(t, h.sessionMsgs)
}

func TestBoundedLogAdapterActionFlagsAndLeadershipTerm(t *testing.T) {
	h := &recordingHandler{}
	adapter := newBoundedLogAdapter(NewOptions(), h)
	w := newTermWriter()

	car := codecs.ClusterActionRequest{LeadershipTermId: 1, LogPosition: 10, Timestamp: 2,
		Action: codecs.ClusterAction.SNAPSHOT, Flags: ClusterActionFlagsStandbySnapshot}
	adapter.onFragment(w.frame(encodeSbe(t, uint16(car.SbeTemplateId()), uint16(car.SbeBlockLength()),
		func(m *codecs.SbeGoMarshaller, b *bytes.Buffer) error { return car.Encode(m, b, true) }), 0xC0))
	assert.Equal(t, []int32{ClusterActionFlagsStandbySnapshot}, h.actions)

	nlt := codecs.NewLeadershipTermEvent{LeadershipTermId: 2, TimeUnit: codecs.ClusterTimeUnit.NANOS,
		AppVersion: int32(util.SemanticVersionCompose(1, 2, 3))}
	adapter.onFragment(w.frame(encodeSbe(t, uint16(nlt.SbeTemplateId()), uint16(nlt.SbeBlockLength()),
		func(m *codecs.SbeGoMarshaller, b *bytes.Buffer) error { return nlt.Encode(m, b, true) }), 0xC0))
	assert.Equal(t, []int32{nlt.AppVersion}, h.leadershipApps)
}

func TestMarkFileLayoutMatchesJava(t *testing.T) {
	path := filepath.Join(t.TempDir(), ServiceMarkFileName(0))
	cmf, err := NewClusterMarkFile(path, ClusterComponentContainer, ErrorBufferLength)
	require.NoError(t, err)
	defer cmf.Close()

	b := cmf.Buffer()
	assert.Equal(t, int32(HeaderLength+ErrorBufferLength), b.Capacity())
	assert.Equal(t, uint16(MarkFileHeaderBlockLength), b.GetUInt16(0))
	assert.Equal(t, uint16(MarkFileHeaderTemplateId), b.GetUInt16(2))
	assert.Equal(t, uint16(MarkFileSchemaId), b.GetUInt16(4))
	assert.Equal(t, uint16(MarkFileSchemaVersion), b.GetUInt16(6))
	assert.Equal(t, int32(0), cmf.VersionVolatile())

	cmf.SetMemberId(3)
	cmf.SetServiceId(1)
	cmf.SetClusterId(9)
	cmf.SetServiceStreamId(104)
	assert.Equal(t, int32(ClusterComponentContainer), b.GetInt32(8+4))
	assert.Equal(t, int32(104), b.GetInt32(8+44))
	assert.Equal(t, int32(3), b.GetInt32(8+56))
	assert.Equal(t, int32(1), b.GetInt32(8+60))
	assert.Equal(t, int32(HeaderLength), b.GetInt32(8+64))
	assert.Equal(t, int32(ErrorBufferLength), b.GetInt32(8+68))
	assert.Equal(t, int32(9), b.GetInt32(8+72))

	cmf.EncodeStrings("/dev/shm/aeron", "aeron:ipc", "aeron:udp", "svc", "", "/tmp/c")
	assert.Equal(t, []string{"/dev/shm/aeron", "aeron:ipc", "aeron:udp", "svc", "", "/tmp/c"}, cmf.DecodeStrings())
	assert.Equal(t, int32(len("/dev/shm/aeron")), b.GetInt32(8+128))

	cmf.SignalReady(12345)
	assert.Equal(t, MarkFileSemanticVersion, cmf.VersionVolatile())
	assert.Equal(t, int64(12345), cmf.ActivityTimestampVolatile())
	cmf.SignalTerminated()
	assert.Equal(t, int64(NullValue), cmf.ActivityTimestampVolatile())

	_, err = NewClusterMarkFile(path, ClusterComponentContainer, 1024)
	assert.Error(t, err)
}

func newTestAgent() *ClusteredServiceAgent {
	opts := NewOptions()
	return &ClusteredServiceAgent{
		opts:                 opts,
		sessionByIdMap:       map[int64]*containerClientSession{},
		sessionMsgHdrBuffer:  newSessionMessageHeaderBuffer(),
		role:                 Follower,
		terminationPosition:  NullPosition,
		requestedAckPosition: NullPosition,
	}
}

func TestAgentSessionsSortedLookupAndRemove(t *testing.T) {
	agent := newTestAgent()
	for _, id := range []int64{5, 1, 3} {
		agent.addSession(newContainerClientSession(id, 1, "aeron:ipc", nil, agent))
	}
	var ids []int64
	agent.ForEachClientSession(func(s ClientSession) { ids = append(ids, s.Id()) })
	assert.Equal(t, []int64{1, 3, 5}, ids)
	assert.Len(t, agent.ClientSessions(), 3)
	assert.Equal(t, int64(3), agent.GetClientSession(3).Id())
	assert.Nil(t, agent.GetClientSession(4))

	require.NotNil(t, agent.removeSession(3))
	assert.Nil(t, agent.removeSession(3))
	assert.Nil(t, agent.GetClientSession(3))
	assert.Len(t, agent.ClientSessions(), 2)
	assert.False(t, agent.CloseClientSession(42))
}

func TestAgentCloseClientSessionAlreadyClosing(t *testing.T) {
	agent := newTestAgent()
	s := newContainerClientSession(7, 1, "aeron:ipc", nil, agent)
	agent.addSession(s)
	s.markClosing()
	// no proxy: a second close must not try to send
	assert.True(t, agent.CloseClientSession(7))
	assert.True(t, s.IsClosing())
}

func TestAgentEgressMockedWhenNotLeader(t *testing.T) {
	agent := newTestAgent()
	s := newContainerClientSession(7, 1, "aeron:ipc", nil, agent)
	agent.addSession(s)
	buf := atomic.MakeBuffer(make([]byte, 8))
	assert.Equal(t, int64(ClientSessionMockedOffer), s.Offer(buf, 0, 8, nil))

	var claim logbuffer.Claim
	assert.Equal(t, int64(ClientSessionMockedOffer), s.TryClaim(16, &claim))
	assert.GreaterOrEqual(t, claim.Buffer().Capacity(), int32(16+SBEHeaderLength+SessionMessageHeaderLength))

	agent.role = Leader
	assert.Equal(t, int64(aeron.NotConnected), s.Offer(buf, 0, 8, nil))
	assert.Equal(t, int64(aeron.NotConnected), s.TryClaim(16, &claim))
}

func TestAgentLifecycleCallbackRestrictions(t *testing.T) {
	agent := newTestAgent()
	agent.activeLifecycleCallback = lifecycleOnTerminate
	assert.Panics(t, func() { agent.ScheduleTimer(1, 2) })
	agent.activeLifecycleCallback = lifecycleNone
	agent.isTerminated = true
	assert.Panics(t, func() { agent.CancelTimer(1) })
}

func TestAgentServiceProtocolState(t *testing.T) {
	agent := newTestAgent()
	agent.onServiceTerminationPosition(100, false)
	assert.Equal(t, int64(100), agent.terminationPosition)
	assert.False(t, agent.terminationAckRequired)
	agent.onRequestServiceAck(200)
	assert.Equal(t, int64(200), agent.requestedAckPosition)
	assert.Equal(t, int64(0), agent.nextAckId())
	assert.Equal(t, int64(1), agent.nextAckId())
}

func TestShouldSnapshotStandbyFlags(t *testing.T) {
	agent := newTestAgent()
	assert.True(t, agent.shouldSnapshot(ClusterActionFlagsDefault))
	assert.False(t, agent.shouldSnapshot(ClusterActionFlagsStandbySnapshot))
	agent.standbySnapshotFlags = ClusterActionFlagsStandbySnapshot
	assert.True(t, agent.shouldSnapshot(ClusterActionFlagsStandbySnapshot))
}

func TestSemanticVersionMajorValidatorAndRole(t *testing.T) {
	v := func(a, b, c uint8) int32 { return int32(util.SemanticVersionCompose(a, b, c)) }
	assert.True(t, SemanticVersionMajorValidator(v(1, 0, 0), v(1, 9, 9)))
	assert.False(t, SemanticVersionMajorValidator(v(1, 0, 0), v(2, 0, 0)))
	assert.Equal(t, "LEADER", Leader.String())
	assert.Equal(t, "FOLLOWER", Follower.String())
	assert.Equal(t, "Role(9)", Role(9).String())
}
