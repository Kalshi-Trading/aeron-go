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

package replaymerge

import (
	"errors"
	"math"
	"testing"

	"github.com/lirm/aeron-go/aeron"
	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/logbuffer"
	"github.com/lirm/aeron-go/aeron/logbuffer/term"
	"github.com/lirm/aeron-go/archive"
	"github.com/lirm/aeron-go/archive/codecs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	testSessionID            = int64(77)
	testRecordingID          = int64(5)
	testReplaySession        = int64(0x7_0000_0123)
	testReplayImageSessionID = int32(0x123)
	testStreamID             = int32(1001)
	testSubChannel           = "aeron:udp?control-mode=manual|session-id=123"
	testReplayChannel        = "aeron:udp?session-id=123"
	testReplayDest           = "aeron:udp?endpoint=localhost:0"
	testReplayDestPort       = "aeron:udp?endpoint=localhost:24325"
	testLiveDest             = "aeron:udp?endpoint=localhost:24326|control=localhost:24327"
	testTimeoutMs            = int64(1000)
)

type fakeImage struct {
	aeron.Image     // methods not used by the test panic
	position        int64
	closed          bool
	transportCount  int32
	termLength      int32
	polls           int
	controlledPolls int
}

func (i *fakeImage) IsClosed() bool { return i.closed }
func (i *fakeImage) Poll(term.FragmentHandler, int) int {
	i.polls++
	return 1
}
func (i *fakeImage) BoundedPoll(term.FragmentHandler, int64, int) int { return 0 }
func (i *fakeImage) ControlledPoll(term.ControlledFragmentHandler, int) int {
	i.controlledPolls++
	return 2
}
func (i *fakeImage) Position() int64                   { return i.position }
func (i *fakeImage) IsEndOfStream() bool               { return false }
func (i *fakeImage) SessionID() int32                  { return testReplayImageSessionID }
func (i *fakeImage) CorrelationID() int64              { return 0 }
func (i *fakeImage) SubscriptionRegistrationID() int64 { return 0 }
func (i *fakeImage) TermBufferLength() int32           { return i.termLength }
func (i *fakeImage) ActiveTransportCount() int32       { return i.transportCount }
func (i *fakeImage) Close() error                      { return nil }

type fakeSubscription struct {
	channel          string
	resolvedEndpoint string
	image            *fakeImage
	added            []string
	removed          []string
	addErr           error
	lookupSessionID  int32
}

func (s *fakeSubscription) Channel() string   { return s.channel }
func (s *fakeSubscription) StreamID() int32   { return testStreamID }
func (s *fakeSubscription) IsConnected() bool { return s.image != nil }
func (s *fakeSubscription) ImageBySessionID(sessionID int32) aeron.Image {
	s.lookupSessionID = sessionID
	if s.image == nil {
		return nil
	}
	return s.image
}
func (s *fakeSubscription) ResolvedEndpoint() string { return s.resolvedEndpoint }
func (s *fakeSubscription) AddDestination(c string) error {
	if s.addErr != nil {
		return s.addErr
	}
	s.added = append(s.added, c)
	return nil
}
func (s *fakeSubscription) RemoveDestination(c string) error {
	s.removed = append(s.removed, c)
	return nil
}

type replayCall struct {
	correlationID, recordingID, position, length int64
	channel                                      string
	streamID                                     int32
}

type fakeArchive struct {
	correlationID    int64
	responses        []*codecs.ControlResponse
	maxPosCalls      []int64 // correlation ids
	replays          []replayCall
	stopReplays      []int64 // replay session ids
	responseDown     bool
	aeronClosed      bool
	requestConnected bool
	// onMaxPos returns the position to respond with for a max recorded position request.
	onMaxPos   func() int64
	replayErr  *codecs.ControlResponse
	sendFailed bool
}

func ok(correlationID, relevantID int64) *codecs.ControlResponse {
	return &codecs.ControlResponse{ControlSessionId: testSessionID, CorrelationId: correlationID,
		RelevantId: relevantID, Code: codecs.ControlResponseCode.OK}
}

func (a *fakeArchive) isAeronClosed() bool       { return a.aeronClosed }
func (a *fakeArchive) isRequestConnected() bool  { return a.requestConnected }
func (a *fakeArchive) isResponseConnected() bool { return !a.responseDown }
func (a *fakeArchive) nextCorrelationID() int64 {
	a.correlationID++
	return a.correlationID
}
func (a *fakeArchive) controlSessionID() int64 { return testSessionID }
func (a *fakeArchive) maxRecordedPosition(correlationID, recordingID int64) (bool, error) {
	if a.sendFailed {
		return false, nil
	}
	a.maxPosCalls = append(a.maxPosCalls, correlationID)
	if a.onMaxPos != nil {
		a.responses = append(a.responses, ok(correlationID, a.onMaxPos()))
	}
	return true, nil
}
func (a *fakeArchive) replay(correlationID, recordingID, position, length int64, ch string, stream int32) (bool, error) {
	a.replays = append(a.replays, replayCall{correlationID, recordingID, position, length, ch, stream})
	if a.replayErr != nil {
		r := *a.replayErr
		r.CorrelationId = correlationID
		a.responses = append(a.responses, &r)
	} else {
		a.responses = append(a.responses, ok(correlationID, testReplaySession))
	}
	return true, nil
}
func (a *fakeArchive) stopReplay(correlationID, replaySessionID int64) (bool, error) {
	a.stopReplays = append(a.stopReplays, replaySessionID)
	return true, nil
}
func (a *fakeArchive) pollResponse() (int, *codecs.ControlResponse) {
	if len(a.responses) == 0 {
		return 0, nil
	}
	r := a.responses[0]
	a.responses = a.responses[1:]
	return 1, r
}

type fakeClock struct{ now int64 }

func (c *fakeClock) time() int64 { return c.now }

type harness struct {
	t     *testing.T
	sub   *fakeSubscription
	arch  *fakeArchive
	clock *fakeClock
	rm    *ReplayMerge
}

func newHarness(t *testing.T, replayDestination string) *harness {
	h := &harness{
		t:     t,
		sub:   &fakeSubscription{channel: testSubChannel, resolvedEndpoint: "127.0.0.1:41234"},
		arch:  &fakeArchive{requestConnected: true},
		clock: &fakeClock{now: 1_000_000},
	}
	rm, err := newReplayMerge(h.sub, h.arch, testReplayChannel, replayDestination, testLiveDest, testRecordingID,
		0, h.clock.time, testTimeoutMs)
	require.NoError(t, err)
	h.rm = rm
	return h
}

// step advances the clock and does one unit of work, failing the test on error.
func (h *harness) step(advanceMs int64) {
	h.t.Helper()
	h.clock.now += advanceMs
	_, err := h.rm.DoWork()
	require.NoError(h.t, err, h.rm.String())
}

func (h *harness) runUntil(state State, maxSteps int) {
	h.t.Helper()
	for i := 0; i < maxSteps && h.rm.State() != state; i++ {
		h.step(10)
	}
	require.Equal(h.t, state, h.rm.State(), h.rm.String())
}

func TestDefaults(t *testing.T) {
	assert.Equal(t, int64(5000), MergeProgressTimeoutDefaultMs)
	assert.Equal(t, int32(32*1024*1024), LiveAddMaxWindow)
	assert.Equal(t, int64(0), ReplayRemoveThreshold)
	assert.Equal(t, "ATTEMPT_LIVE_JOIN", StateAttemptLiveJoin.String())
	assert.Equal(t, "State(42)", State(42).String())
}

func TestConstructorValidation(t *testing.T) {
	arch := &fakeArchive{}
	clock := (&fakeClock{}).time
	cases := []struct {
		name, sub, replayChannel, replayDest, liveDest string
	}{
		{"ipc subscription", "aeron:ipc?control-mode=manual", testReplayChannel, testReplayDest, testLiveDest},
		{"ipc replay channel", testSubChannel, "aeron:ipc", testReplayDest, testLiveDest},
		{"ipc replay destination", testSubChannel, testReplayChannel, "aeron:ipc", testLiveDest},
		{"ipc live destination", testSubChannel, testReplayChannel, testReplayDest, "aeron:ipc"},
		{"not manual", "aeron:udp?endpoint=localhost:1", testReplayChannel, testReplayDest, testLiveDest},
		{"no replay endpoint", testSubChannel, testReplayChannel, "aeron:udp?interface=localhost", testLiveDest},
		{"bad replay channel", testSubChannel, "garbage", testReplayDest, testLiveDest},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			sub := &fakeSubscription{channel: c.sub}
			rm, err := newReplayMerge(sub, arch, c.replayChannel, c.replayDest, c.liveDest, 1, 0, clock, 1000)
			assert.Error(t, err)
			assert.Nil(t, rm)
			assert.Empty(t, sub.added)
		})
	}

	sub := &fakeSubscription{channel: testSubChannel, addErr: errors.New("boom")}
	_, err := newReplayMerge(sub, arch, testReplayChannel, testReplayDest, testLiveDest, 1, 0, clock, 1000)
	assert.ErrorContains(t, err, "boom")

	_, err = NewReplayMerge(nil, nil, testReplayChannel, testReplayDest, testLiveDest, 1, 0, 1000)
	assert.Error(t, err)
}

func TestConstructorFixedPort(t *testing.T) {
	h := newHarness(t, testReplayDestPort)
	assert.Equal(t, StateGetRecordingPosition, h.rm.State())
	assert.Equal(t, []string{testReplayDestPort}, h.sub.added)
	uri, err := aeron.ParseChannelUri(h.rm.ReplayChannel())
	require.NoError(t, err)
	assert.Equal(t, "localhost:24325", uri.Get(aeron.EndpointParamName))
	assert.Equal(t, "0", uri.Get(aeron.LingerParamName))
	assert.Equal(t, "false", uri.Get(aeron.EosParamName))
	assert.Equal(t, "123", uri.Get(aeron.SessionIdParamName))
}

func TestResolveReplayPort(t *testing.T) {
	h := newHarness(t, testReplayDest)
	assert.Equal(t, StateResolveReplayPort, h.rm.State())

	h.sub.resolvedEndpoint = ""
	h.step(1)
	assert.Equal(t, StateResolveReplayPort, h.rm.State())

	h.sub.resolvedEndpoint = "127.0.0.1:41234"
	h.step(1)
	assert.Equal(t, StateGetRecordingPosition, h.rm.State())
	uri, err := aeron.ParseChannelUri(h.rm.ReplayChannel())
	require.NoError(t, err)
	// Java semantics: the replay channel had no endpoint, so the resolved endpoint is used as is.
	assert.Equal(t, "127.0.0.1:41234", uri.Get(aeron.EndpointParamName))
}

func TestResolveReplayPortTimeout(t *testing.T) {
	h := newHarness(t, testReplayDest)
	h.sub.resolvedEndpoint = ""
	h.step(testTimeoutMs)
	h.clock.now++
	_, err := h.rm.DoWork()
	assert.ErrorIs(t, err, ErrNoProgress)
	assert.ErrorContains(t, err, "state=RESOLVE_REPLAY_PORT")
	assert.True(t, h.rm.HasFailed())

	// Failed is terminal for DoWork.
	n, err := h.rm.DoWork()
	assert.NoError(t, err)
	assert.Zero(t, n)
}

func TestReplaceEndpointWildcardPort(t *testing.T) {
	cases := []struct{ channel, resolved, want string }{
		{"aeron:udp?session-id=1", "127.0.0.1:5000", "127.0.0.1:5000"},
		{"aeron:udp?endpoint=localhost:0", "127.0.0.1:5000", "localhost:5000"},
		{"aeron:udp?endpoint=localhost:4000", "127.0.0.1:5000", "localhost:4000"},
		{"aeron:udp?endpoint=[::1]:0", "[::1]:5000", "[::1]:5000"},
	}
	for _, c := range cases {
		uri, err := aeron.ParseChannelUri(c.channel)
		require.NoError(t, err)
		require.NoError(t, replaceEndpointWildcardPort(uri, c.resolved))
		assert.Equal(t, c.want, uri.Get(aeron.EndpointParamName), c.channel)
	}
	uri, _ := aeron.ParseChannelUri("aeron:udp")
	assert.Error(t, replaceEndpointWildcardPort(uri, "localhost"))
	assert.Error(t, replaceEndpointWildcardPort(uri, "localhost:0"))
}

func TestHappyPathMerge(t *testing.T) {
	h := newHarness(t, testReplayDest)
	recorded := int64(1 << 20)
	h.arch.onMaxPos = func() int64 { return recorded }

	h.runUntil(StateReplay, 20)
	require.Len(t, h.arch.replays, 0)
	h.step(1)
	require.Len(t, h.arch.replays, 1)
	r := h.arch.replays[0]
	assert.Equal(t, testRecordingID, r.recordingID)
	assert.Equal(t, int64(0), r.position)
	assert.Equal(t, int64(math.MaxInt64), r.length)
	assert.Equal(t, testStreamID, r.streamID)
	assert.Contains(t, r.channel, "endpoint=127.0.0.1:41234")

	h.step(1)
	assert.Equal(t, StateCatchup, h.rm.State())
	assert.Nil(t, h.rm.Image())

	// Image appears, still behind the recorded position.
	img := &fakeImage{position: 0, transportCount: 1, termLength: 64 * 1024}
	h.sub.image = img
	h.step(1)
	assert.Equal(t, testReplayImageSessionID, h.sub.lookupSessionID)
	assert.Same(t, img, h.rm.Image())
	assert.Equal(t, StateCatchup, h.rm.State())

	// Catch up to the recorded position.
	img.position = recorded
	h.step(1)
	assert.Equal(t, StateAttemptLiveJoin, h.rm.State())

	// Recording moved on by less than termLength/4: add live destination and go back to catchup.
	recorded += 8 * 1024
	h.runUntil(StateCatchup, 50)
	assert.True(t, h.rm.IsLiveAdded())
	assert.Equal(t, []string{testReplayDest, testLiveDest}, h.sub.added)

	// Catch up fully with both transports active: merge.
	img.position = recorded
	img.transportCount = 2
	h.runUntil(StateMerged, 100)
	assert.True(t, h.rm.IsMerged())
	assert.Equal(t, []string{testReplayDest}, h.sub.removed)
	assert.Equal(t, []int64{testReplaySession}, h.arch.stopReplays)

	// Close after merge leaves destinations alone and does not stop the replay again.
	require.NoError(t, h.rm.Close())
	assert.True(t, h.rm.IsClosed())
	assert.Equal(t, []string{testReplayDest}, h.sub.removed)
	assert.Len(t, h.arch.stopReplays, 1)
	require.NoError(t, h.rm.Close())
}

func TestLiveNotAddedWhenTooFarBehind(t *testing.T) {
	h := newHarness(t, testReplayDestPort)
	recorded := int64(1 << 20)
	h.arch.onMaxPos = func() int64 { return recorded }
	h.runUntil(StateCatchup, 50)
	img := &fakeImage{position: recorded, transportCount: 1, termLength: 64 * 1024}
	h.sub.image = img
	h.step(1)
	assert.Equal(t, StateAttemptLiveJoin, h.rm.State())

	recorded += 16*1024 + 32 // just over termLength/4
	h.runUntil(StateCatchup, 50)
	assert.False(t, h.rm.IsLiveAdded())

	// Merge requires two active transports even when caught up.
	img.position = recorded
	recorded += 64
	h.runUntil(StateAttemptLiveJoin, 5)
	h.runUntil(StateCatchup, 50)
	assert.True(t, h.rm.IsLiveAdded())
	img.position = recorded
	h.runUntil(StateAttemptLiveJoin, 5)
	h.runUntil(StateCatchup, 50)
	assert.False(t, h.rm.IsMerged())
}

func TestGetRecordingPositionBackoffAndNullPosition(t *testing.T) {
	h := newHarness(t, testReplayDestPort)
	h.arch.onMaxPos = func() int64 { return archive.RecordingPositionNull }

	h.step(0) // sends request at t0
	require.Len(t, h.arch.maxPosCalls, 1)
	h.step(0) // receives NULL position, stays in state
	assert.Equal(t, StateGetRecordingPosition, h.rm.State())
	h.step(0) // backoff (16ms) not elapsed
	assert.Len(t, h.arch.maxPosCalls, 1)
	h.step(16)
	assert.Len(t, h.arch.maxPosCalls, 2)

	// backoff doubles up to 500ms
	expected := []int64{32, 64, 128, 256, 500, 500}
	for _, backoff := range expected {
		h.step(0) // consume response
		h.step(backoff - 1)
		n := len(h.arch.maxPosCalls)
		h.step(1)
		require.Equal(t, n+1, len(h.arch.maxPosCalls), "backoff %d", backoff)
		h.clock.now -= 0
		h.rm.timeOfLastProgressMs = h.clock.now // keep the progress check happy
	}
}

func TestArchiveErrorFailsMerge(t *testing.T) {
	h := newHarness(t, testReplayDestPort)
	h.arch.onMaxPos = func() int64 { return 1024 }
	h.arch.replayErr = &codecs.ControlResponse{ControlSessionId: testSessionID, RelevantId: 3,
		Code: codecs.ControlResponseCode.ERROR, ErrorMessage: []uint8("unknown recording")}
	h.runUntil(StateReplay, 20)
	h.step(1)
	_, err := h.rm.DoWork()
	var archErr *ArchiveError
	require.ErrorAs(t, err, &archErr)
	assert.Equal(t, int32(3), archErr.ErrorCode)
	assert.Equal(t, "unknown recording", archErr.Message)
	assert.True(t, h.rm.HasFailed())

	// Close after failure removes the replay destination, replay was never active.
	require.NoError(t, h.rm.Close())
	assert.Equal(t, []string{testReplayDestPort}, h.sub.removed)
	assert.Empty(t, h.arch.stopReplays)
}

func TestResponsesForOtherSessionsIgnored(t *testing.T) {
	h := newHarness(t, testReplayDestPort)
	h.step(0)
	other := ok(h.arch.maxPosCalls[0], 1234)
	other.ControlSessionId = testSessionID + 1
	otherErr := &codecs.ControlResponse{ControlSessionId: testSessionID + 1, Code: codecs.ControlResponseCode.ERROR}
	h.arch.responses = append(h.arch.responses, otherErr, other)
	h.step(0)
	h.step(0)
	assert.Equal(t, StateGetRecordingPosition, h.rm.State())
	h.arch.responses = append(h.arch.responses, ok(h.arch.maxPosCalls[0], 4096))
	h.step(0)
	assert.Equal(t, StateReplay, h.rm.State())
}

func TestScheduledArchivePollDetectsErrors(t *testing.T) {
	h := newHarness(t, testReplayDestPort)
	h.arch.onMaxPos = func() int64 { return 1 << 20 }
	h.runUntil(StateCatchup, 20)
	h.arch.responses = append(h.arch.responses, &codecs.ControlResponse{ControlSessionId: testSessionID,
		CorrelationId: 999, Code: codecs.ControlResponseCode.ERROR, ErrorMessage: []uint8("replay aborted")})
	h.clock.now += archivePollIntervalMs + 1
	_, err := h.rm.DoWork()
	var archErr *ArchiveError
	require.ErrorAs(t, err, &archErr)
	assert.Equal(t, int64(999), archErr.CorrelationID)
}

func TestArchiveNotConnected(t *testing.T) {
	h := newHarness(t, testReplayDestPort)
	h.step(0)
	h.arch.responseDown = true
	_, err := h.rm.DoWork()
	assert.ErrorIs(t, err, ErrArchiveNotConnected)
	assert.True(t, h.rm.HasFailed())
}

func TestImageClosedDuringCatchup(t *testing.T) {
	h := newHarness(t, testReplayDestPort)
	h.arch.onMaxPos = func() int64 { return 1 << 20 }
	h.runUntil(StateCatchup, 20)
	h.sub.image = &fakeImage{position: 100, termLength: 64 * 1024}
	h.step(1)
	h.sub.image.closed = true
	_, err := h.rm.DoWork()
	assert.ErrorIs(t, err, ErrImageClosed)
	assert.True(t, h.rm.HasFailed())
}

func TestCatchupNoProgressTimesOut(t *testing.T) {
	h := newHarness(t, testReplayDestPort)
	h.arch.onMaxPos = func() int64 { return 1 << 20 }
	h.runUntil(StateCatchup, 20)
	h.sub.image = &fakeImage{position: 100, transportCount: 1, termLength: 64 * 1024}
	h.step(1)
	h.step(testTimeoutMs)
	h.clock.now++
	_, err := h.rm.DoWork()
	assert.ErrorIs(t, err, ErrNoProgress)
	assert.ErrorContains(t, err, "state=CATCHUP, activeTransportCount=1")
}

func TestCloseStopsActiveReplay(t *testing.T) {
	h := newHarness(t, testReplayDest)
	h.arch.onMaxPos = func() int64 { return 1 << 20 }
	h.runUntil(StateCatchup, 20)
	require.NoError(t, h.rm.Close())
	assert.Equal(t, StateClosed, h.rm.State())
	assert.Equal(t, []string{testReplayDest}, h.sub.removed)
	assert.Equal(t, []int64{testReplaySession}, h.arch.stopReplays)

	// No work after close.
	n, err := h.rm.DoWork()
	assert.NoError(t, err)
	assert.Zero(t, n)
}

func TestCloseWithClosedAeron(t *testing.T) {
	h := newHarness(t, testReplayDest)
	h.arch.aeronClosed = true
	require.NoError(t, h.rm.Close())
	assert.Empty(t, h.sub.removed)
}

func TestPollAndControlledPoll(t *testing.T) {
	h := newHarness(t, testReplayDestPort)
	h.arch.onMaxPos = func() int64 { return 1 << 20 }
	handler := func(*atomic.Buffer, int32, int32, *logbuffer.Header) {}
	controlled := func(*atomic.Buffer, int32, int32, *logbuffer.Header) term.ControlledPollAction {
		return term.ControlledPollActionContinue
	}
	n, err := h.rm.Poll(handler, 10)
	require.NoError(t, err)
	assert.Zero(t, n)

	h.runUntil(StateCatchup, 20)
	img := &fakeImage{position: 0, termLength: 64 * 1024}
	h.sub.image = img
	n, err = h.rm.Poll(handler, 10)
	require.NoError(t, err)
	assert.Equal(t, 1, n)
	n, err = h.rm.ControlledPoll(controlled, 10)
	require.NoError(t, err)
	assert.Equal(t, 2, n)
	assert.Equal(t, 1, img.polls)
	assert.Equal(t, 1, img.controlledPolls)

	h.arch.onMaxPos = nil
	img.position = 1 << 20
	h.step(1)
	require.Equal(t, StateAttemptLiveJoin, h.rm.State())
	h.step(600) // sends max recorded position request
	h.arch.responseDown = true
	_, err = h.rm.Poll(handler, 10)
	assert.ErrorIs(t, err, ErrArchiveNotConnected)
}
