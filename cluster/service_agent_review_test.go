package cluster

import (
	"testing"
	"time"

	"github.com/lirm/aeron-go/aeron"
	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/counters"
	"github.com/lirm/aeron-go/aeron/logbuffer"
	"github.com/lirm/aeron-go/aeron/logbuffer/term"
	"github.com/lirm/aeron-go/cluster/codecs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakePub scripts publication results; unscripted calls succeed with position 64.
type fakePub struct {
	aeron.Publisher
	results []int64
	calls   int
	offers  [][]byte
	closed  bool
}

func (p *fakePub) next() int64 {
	p.calls++
	if len(p.results) == 0 {
		return 64
	}
	r := p.results[0]
	p.results = p.results[1:]
	return r
}

func (p *fakePub) Offer(buffer *atomic.Buffer, offset, length int32, _ term.ReservedValueSupplier) int64 {
	p.offers = append(p.offers, buffer.GetBytesArray(offset, length))
	return p.next()
}

func (p *fakePub) Offer2(_ *atomic.Buffer, _, _ int32, _ *atomic.Buffer, _, _ int32, _ term.ReservedValueSupplier) int64 {
	return p.next()
}

func (p *fakePub) TryClaim(_ int32, _ *logbuffer.Claim) int64 { return p.next() }
func (p *fakePub) TermBufferLength() int32                    { return 64 * 1024 }
func (p *fakePub) Close() error                               { p.closed = true; return nil }

type fakeService struct {
	onStart        func(Cluster, aeron.Image)
	onSessionClose func(ClientSession)
	terminates     int
}

func (s *fakeService) OnStart(c Cluster, img aeron.Image) {
	if s.onStart != nil {
		s.onStart(c, img)
	}
}
func (s *fakeService) OnSessionOpen(ClientSession, int64) {}
func (s *fakeService) OnSessionClose(session ClientSession, _ int64, _ codecs.CloseReasonEnum) {
	if s.onSessionClose != nil {
		s.onSessionClose(session)
	}
}
func (s *fakeService) OnSessionMessage(ClientSession, int64, *atomic.Buffer, int32, int32, *logbuffer.Header) {
}
func (s *fakeService) OnTimerEvent(int64, int64)                  {}
func (s *fakeService) OnTakeSnapshot(*aeron.ExclusivePublication) {}
func (s *fakeService) OnRoleChange(Role)                          {}
func (s *fakeService) OnTerminate(Cluster)                        { s.terminates++ }
func (s *fakeService) OnNewLeadershipTermEvent(int64, int64, int64, int64, int32, int32, codecs.ClusterTimeUnitEnum, int32) {
}

type countingWorker struct {
	calls     int
	lifecycle lifecycleCallback
	agent     *ClusteredServiceAgent
}

func (w *countingWorker) DoBackgroundWork(int64) int {
	w.calls++
	w.lifecycle = w.agent.activeLifecycleCallback
	return 0
}

func newReviewAgent(pub *fakePub, svc *fakeService) *ClusteredServiceAgent {
	agent := newTestAgent()
	agent.aeronClient = &aeron.Aeron{}
	agent.proxy = newConsensusModuleProxy(pub)
	agent.service = svc
	agent.opts.IdleStrategy = idleNoOp{}
	// keep idle() away from the slow-tick path, which needs a mark file
	agent.lastSlowTickNs = time.Now().Add(time.Hour).UnixNano()
	return agent
}

type idleNoOp struct{}

func (idleNoOp) Idle(int) {}

type testCounter struct {
	typeId int32
	regId  int64
	key    []byte
}

func newTestCountersReader(cs ...testCounter) *counters.Reader {
	values := atomic.MakeBuffer(make([]byte, 8*counters.CounterLength))
	meta := atomic.MakeBuffer(make([]byte, 8*counters.MetadataLength))
	for i, c := range cs {
		id := int32(i)
		m := counters.MetaDataOffset(id)
		meta.PutInt32(m+counters.TypeIdOffset, c.typeId)
		meta.PutInt64(m+counters.FreeForReuseDeadlineOffset, counters.NotFreeToReuse)
		if len(c.key) > 0 {
			meta.PutBytesArray(m+counters.KeyOffset, &c.key, 0, int32(len(c.key)))
		}
		values.PutInt64(counters.CounterOffset(id)+counters.RegistrationIdOffset, c.regId)
		meta.PutInt32Ordered(m, counters.RecordAllocated)
	}
	return counters.NewReader(values, meta)
}

// Java: terminate(logPosition == terminationPosition); overshooting the termination position is unexpected.
func TestAgentTerminationExpectedOnlyAtExactPosition(t *testing.T) {
	for _, tc := range []struct {
		logPos   int64
		expected bool
	}{{100, true}, {164, false}} {
		svc := &fakeService{}
		agent := newReviewAgent(&fakePub{}, svc)
		agent.isServiceActive = true
		agent.logPosition = tc.logPos
		agent.terminationPosition = 100
		agent.checkServiceProtocolPositions()
		require.True(t, agent.isTerminated)
		termErr, ok := agent.terminationErr.(*ClusterTerminationError)
		require.True(t, ok)
		assert.Equal(t, tc.expected, termErr.Expected, "logPosition=%d", tc.logPos)
		assert.Equal(t, 1, svc.terminates)
	}
}

// Java closeClientSession retries consensusModuleProxy.closeSession up to 3 times.
func TestAgentCloseClientSessionRetries(t *testing.T) {
	// proxy.send already retries 3 times per closeSession call
	pub := &fakePub{results: []int64{aeron.BackPressured, aeron.BackPressured, aeron.BackPressured, 64}}
	agent := newReviewAgent(pub, &fakeService{})
	s := newContainerClientSession(7, 1, "aeron:ipc", nil, agent)
	agent.addSession(s)
	assert.True(t, agent.CloseClientSession(7))
	assert.True(t, s.IsClosing())

	pub.results = make([]int64, 20)
	for i := range pub.results {
		pub.results[i] = aeron.AdminAction
	}
	pub.calls = 0
	s2 := newContainerClientSession(8, 1, "aeron:ipc", nil, agent)
	agent.addSession(s2)
	assert.False(t, agent.CloseClientSession(8))
	assert.Equal(t, closeSessionAttempts*sendAttempts, pub.calls)
	assert.False(t, s2.IsClosing())
}

// Java onSessionClose removes and disconnects the session before calling service.onSessionClose.
func TestAgentOnSessionCloseDisconnectsBeforeCallback(t *testing.T) {
	resp := &aeron.Publication{}
	svc := &fakeService{}
	agent := newReviewAgent(&fakePub{}, svc)
	s := newContainerClientSession(7, 1, "aeron:ipc", nil, agent)
	s.response = resp
	agent.addSession(s)
	called := false
	svc.onSessionClose = func(session ClientSession) {
		called = true
		assert.True(t, resp.IsClosed(), "response publication closed before callback")
		assert.Nil(t, agent.GetClientSession(7))
	}
	agent.onSessionClose(1, 10, 7, 99, codecs.CloseReason.CLIENT_ACTION)
	assert.True(t, called)
}

// Java ServiceSnapshotLoader ignores SECTION and NULL_VAL marks.
func TestSnapshotLoaderIgnoresSectionMark(t *testing.T) {
	m := codecs.NewSbeGoMarshaller()
	w := newTermWriter()
	loader := newSnapshotLoader(newTestAgent(), nil)
	for _, mark := range []codecs.SnapshotMarkEnum{
		codecs.SnapshotMark.BEGIN, codecs.SnapshotMark.SECTION, codecs.SnapshotMark.NullValue, codecs.SnapshotMark.END,
	} {
		pkt, err := codecs.SnapshotMarkerPacket(m, true, snapshotTypeId, 10, 1, 0, mark, codecs.ClusterTimeUnit.NANOS, 3)
		if mark == codecs.SnapshotMark.NullValue {
			pkt, err = codecs.SnapshotMarkerPacket(m, false, snapshotTypeId, 10, 1, 0, mark, codecs.ClusterTimeUnit.NANOS, 3)
		}
		require.NoError(t, err)
		loader.onFragment(w.frame(pkt, 0xC0))
		require.NoError(t, loader.err, "mark=%v", mark)
	}
	assert.True(t, loader.isDone)
	assert.Equal(t, int32(3), loader.appVersion)
}

// Java ConsensusModuleProxy.offer/tryClaim throw ClusterException on NOT_CONNECTED, CLOSED, MAX_POSITION_EXCEEDED.
func TestAgentOfferToConsensusModuleChecksResult(t *testing.T) {
	pub := &fakePub{}
	agent := newReviewAgent(pub, &fakeService{})
	buf := atomic.MakeBuffer(make([]byte, 8))
	var claim logbuffer.Claim
	pub.results = []int64{aeron.BackPressured, aeron.BackPressured}
	assert.Equal(t, aeron.BackPressured, agent.Offer(buf, 0, 8))
	assert.Equal(t, aeron.BackPressured, agent.TryClaim(8, &claim))
	for _, r := range []int64{aeron.NotConnected, aeron.PublicationClosed, aeron.MaxPositionExceeded} {
		pub.results = []int64{r, r}
		assert.Panics(t, func() { agent.Offer(buf, 0, 8) })
		assert.Panics(t, func() { agent.TryClaim(8, &claim) })
	}
}

// Java ClientSession.tryClaim on a follower rejects lengths above the max UDP payload.
func TestAgentTryClaimToSessionRejectsOversizeWhenNotLeader(t *testing.T) {
	agent := newReviewAgent(&fakePub{}, &fakeService{})
	s := newContainerClientSession(7, 1, "aeron:ipc", nil, agent)
	agent.addSession(s)
	var claim logbuffer.Claim
	maxPayload := int32(maxUdpPayloadLength) - logbuffer.DataFrameHeader.Length - SBEHeaderLength - SessionMessageHeaderLength
	assert.Equal(t, int64(ClientSessionMockedOffer), s.TryClaim(maxPayload, &claim))
	assert.Panics(t, func() { s.TryClaim(maxPayload+1, &claim) })
}

// Java registers an UnavailableCounterHandler that closes commit-pos; the agent then terminates as expected.
func TestAgentCommitPositionUnavailableTerminatesExpected(t *testing.T) {
	reader := newTestCountersReader(testCounter{typeId: commitPosCounterTypeId, regId: 99})
	agent := newReviewAgent(&fakePub{}, &fakeService{})
	agent.counters = reader
	commitPos, err := counters.NewReadableRegisteredCounter(reader, reader.GetCounterRegistrationId(0), 0)
	require.NoError(t, err)
	agent.storeCommitPosition(commitPos)

	agent.counterUnavailable(reader, 98, 0)
	assert.False(t, commitPos.IsClosed())
	agent.counterUnavailable(reader, 99, 0)
	assert.True(t, commitPos.IsClosed())

	agent.lastSlowTickNs = 0
	defer func() {
		r := recover()
		termErr, ok := r.(*ClusterTerminationError)
		require.True(t, ok, "recovered %v", r)
		assert.True(t, termErr.Expected)
	}()
	agent.checkForClockTick(time.Now().UnixNano())
}

// Java idle(workCount) only runs the clock tick and background work when no work was done.
func TestAgentIdleRunsBackgroundWorkWhenIdle(t *testing.T) {
	agent := newReviewAgent(&fakePub{}, &fakeService{})
	w := &countingWorker{agent: agent}
	agent.backgroundWorker = w
	agent.idle(0)
	assert.Equal(t, 0, w.calls, "service not active")
	agent.isServiceActive = true
	agent.idle(1)
	assert.Equal(t, 0, w.calls)
	agent.activeLifecycleCallback = lifecycleOnTerminate
	agent.idle(0)
	assert.Equal(t, 1, w.calls)
	assert.Equal(t, lifecycleDoBackgroundWork, w.lifecycle)
	assert.Equal(t, lifecycleOnTerminate, agent.activeLifecycleCallback)
}

// Java acks recovery with relevantId=NULL_VALUE when onStart fails, then rethrows.
func TestAgentRecoverStateFailureAcksNullRelevantId(t *testing.T) {
	key := make([]byte, 32)
	kb := atomic.MakeBuffer(key)
	kb.PutInt64(0, NullValue)
	kb.PutInt64(8, 0)
	kb.PutInt64(16, 0)
	kb.PutInt32(24, 0)
	reader := newTestCountersReader(testCounter{typeId: recoveryStateCounterTypeId, regId: 1, key: key})
	pub := &fakePub{}
	svc := &fakeService{onStart: func(Cluster, aeron.Image) { panic("boom") }}
	agent := newReviewAgent(pub, svc)
	agent.counters = reader

	err := agent.recoverState()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "boom")
	assert.False(t, agent.isServiceActive)
	require.Len(t, pub.offers, 1)
	ack := atomic.MakeBuffer(pub.offers[0])
	assert.Equal(t, int64(NullValue), ack.GetInt64(SBEHeaderLength+24))
	assert.Equal(t, lifecycleNone, agent.activeLifecycleCallback)

	pub.offers = nil
	svc.onStart = nil
	require.NoError(t, agent.recoverState())
	require.Len(t, pub.offers, 1)
	assert.Equal(t, int64(0), atomic.MakeBuffer(pub.offers[0]).GetInt64(SBEHeaderLength+24))
}
