// Copyright 2026 Kalshi Trading.
// Copyright 2026 Adaptive Financial Consulting Limited (Java reference implementation).
//
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

package persistentsubscription

import (
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/lirm/aeron-go/aeron"
	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/logbuffer"
	"github.com/lirm/aeron-go/aeron/logbuffer/term"
	"github.com/lirm/aeron-go/archive"
	"github.com/lirm/aeron-go/archive/codecs"
)

type fakeImage struct {
	sessionID int32
	position  int64
	closed    bool
}

func (i *fakeImage) IsClosed() bool                                         { return i.closed }
func (i *fakeImage) Poll(term.FragmentHandler, int) int                     { return 0 }
func (i *fakeImage) BoundedPoll(term.FragmentHandler, int64, int) int       { return 0 }
func (i *fakeImage) ControlledPoll(term.ControlledFragmentHandler, int) int { return 0 }
func (i *fakeImage) Position() int64                                        { return i.position }
func (i *fakeImage) IsEndOfStream() bool                                    { return false }
func (i *fakeImage) SessionID() int32                                       { return i.sessionID }
func (i *fakeImage) CorrelationID() int64                                   { return 0 }
func (i *fakeImage) SubscriptionRegistrationID() int64                      { return 0 }
func (i *fakeImage) TermBufferLength() int32                                { return 64 * 1024 }
func (i *fakeImage) ActiveTransportCount() int32                            { return 1 }
func (i *fakeImage) Close() error                                           { i.closed = true; return nil }

type fakeSubscription struct {
	id       int64
	channel  string
	images   []*fakeImage
	endpoint string
}

func (s *fakeSubscription) registrationID() int64 { return s.id }
func (s *fakeSubscription) imageCount() int       { return len(s.images) }
func (s *fakeSubscription) imageAtIndex(i int) aeron.Image {
	return s.images[i]
}
func (s *fakeSubscription) imageBySessionID(id int32) aeron.Image {
	for _, i := range s.images {
		if i.sessionID == id {
			return i
		}
	}
	return nil
}
func (s *fakeSubscription) resolvedEndpoint() string { return s.endpoint }

type fakePublication struct{}

func (fakePublication) isConnected() bool { return true }

type pendingSub struct {
	sub      *fakeSubscription
	resolved bool
	err      error
}

type fakeAeron struct {
	nextID  int64
	subs    map[int64]*pendingSub
	removed []int64
	pubs    map[int64]bool
}

func newFakeAeron() *fakeAeron {
	return &fakeAeron{nextID: 100, subs: map[int64]*pendingSub{}, pubs: map[int64]bool{}}
}

func (f *fakeAeron) nextCorrelationID() int64 { f.nextID++; return f.nextID }
func (f *fakeAeron) asyncAddSubscription(channel string, _ int32) (int64, error) {
	id := f.nextCorrelationID()
	f.subs[id] = &pendingSub{sub: &fakeSubscription{id: id, channel: channel}}
	return id, nil
}
func (f *fakeAeron) getSubscription(id int64) (subscriptionHandle, error) {
	p := f.subs[id]
	if p.err != nil {
		return nil, p.err
	}
	if !p.resolved {
		return nil, nil
	}
	return p.sub, nil
}
func (f *fakeAeron) asyncAddExclusivePublication(string, int32) (int64, error) {
	id := f.nextCorrelationID()
	f.pubs[id] = true
	return id, nil
}
func (f *fakeAeron) getExclusivePublication(int64) (requestPublication, error) {
	return fakePublication{}, nil
}
func (f *fakeAeron) asyncRemoveSubscription(id int64) { f.removed = append(f.removed, id) }
func (f *fakeAeron) asyncRemovePublication(int64)     {}
func (f *fakeAeron) doWork() int                      { return 0 }
func (f *fakeAeron) isClosed() bool                   { return false }

// lastSub returns the most recently added subscription for channel.
func (f *fakeAeron) lastSub(t *testing.T, channelPrefix string) *pendingSub {
	var found *pendingSub
	var foundID int64
	for id, p := range f.subs {
		if strings.HasPrefix(p.sub.channel, channelPrefix) && id > foundID {
			found, foundID = p, id
		}
	}
	if found == nil {
		t.Fatalf("no subscription for %s", channelPrefix)
	}
	return found
}

type sentRequest struct {
	kind          string
	correlationID int64
	position      int64
	channel       string
	replaySession int64
}

type fakeArchive struct {
	connected bool
	closed    bool
	sendOK    bool
	sent      []sentRequest
}

func (f *fakeArchive) poll() int         { return 0 }
func (f *fakeArchive) isConnected() bool { return f.connected && !f.closed }
func (f *fakeArchive) isClosed() bool    { return f.closed }
func (f *fakeArchive) record(r sentRequest) bool {
	if !f.isConnected() || !f.sendOK {
		return false
	}
	f.sent = append(f.sent, r)
	return true
}
func (f *fakeArchive) trySendListRecordingRequest(c int64, _ int64) bool {
	return f.record(sentRequest{kind: "list", correlationID: c})
}
func (f *fakeArchive) trySendMaxRecordedPositionRequest(c int64, _ int64) bool {
	return f.record(sentRequest{kind: "max", correlationID: c})
}
func (f *fakeArchive) trySendReplayTokenRequest(c int64, _ int64) bool {
	return f.record(sentRequest{kind: "token", correlationID: c})
}
func (f *fakeArchive) trySendReplayRequest(c int64, _ int64, position int64, _ int64, channel string, _ int32) bool {
	return f.record(sentRequest{kind: "replay", correlationID: c, position: position, channel: channel})
}
func (f *fakeArchive) trySendReplayRequestOn(_ requestPublication, c int64, _ int64, position int64, _ int64,
	channel string, _ int32, _ int64) bool {
	return f.record(sentRequest{kind: "replay-response", correlationID: c, position: position, channel: channel})
}
func (f *fakeArchive) trySendStopReplayRequest(c int64, session int64) bool {
	return f.record(sentRequest{kind: "stop", correlationID: c, replaySession: session})
}
func (f *fakeArchive) close() { f.closed = true }

func (f *fakeArchive) last(t *testing.T, kind string) sentRequest {
	for i := len(f.sent) - 1; i >= 0; i-- {
		if f.sent[i].kind == kind {
			return f.sent[i]
		}
	}
	t.Fatalf("no %s request sent: %+v", kind, f.sent)
	return sentRequest{}
}

type testListener struct {
	joined, left int
	errs         []error
}

func (l *testListener) OnLiveJoined()     { l.joined++ }
func (l *testListener) OnLiveLeft()       { l.left++ }
func (l *testListener) OnError(err error) { l.errs = append(l.errs, err) }

type harness struct {
	t        *testing.T
	ps       *PersistentSubscription
	aeron    *fakeAeron
	archive  *fakeArchive
	listener *testListener
	now      int64
}

const (
	testRecordingID = 7
	testStreamID    = 1001
	testTimeout     = time.Second
)

func newHarness(t *testing.T, startPosition int64, replayChannel string) *harness {
	h := &harness{t: t, aeron: newFakeAeron(), archive: &fakeArchive{connected: true, sendOK: true},
		listener: &testListener{}}
	ctx := NewContext()
	ctx.RecordingID = testRecordingID
	ctx.StartPosition = startPosition
	ctx.LiveChannel = "aeron:udp?endpoint=localhost:20121"
	ctx.LiveStreamID = testStreamID
	ctx.ReplayChannel = replayChannel
	ctx.ReplayStreamID = 2002
	ctx.Listener = h.listener
	ctx.ArchiveOptions = archive.DefaultOptions()
	ctx.ArchiveOptions.Timeout = testTimeout
	ctx.NanoClock = func() int64 { return h.now }
	if err := ctx.validate(); err != nil {
		t.Fatal(err)
	}
	h.ps = newPersistentSubscription(ctx, h.aeron)
	h.ps.archive = h.archive
	return h
}

func (h *harness) poll(n int) {
	for i := 0; i < n; i++ {
		h.ps.Poll(func(*aeronBuffer, int32, int32, *aeronHeader) {}, 10)
	}
}

func (h *harness) expectState(s State) {
	h.t.Helper()
	if h.ps.State() != s {
		h.t.Fatalf("expected state %s, got %s", s, h.ps.State())
	}
}

func (h *harness) respondDescriptor(start, stop int64, streamID int32) {
	list := h.archive.last(h.t, "list")
	h.ps.onRecordingDescriptor(&codecs.RecordingDescriptor{CorrelationId: list.correlationID,
		RecordingId: testRecordingID, StartPosition: start, StopPosition: stop, TermBufferLength: 64 * 1024,
		StreamId: streamID})
}

func (h *harness) respond(kind string, code codecs.ControlResponseCodeEnum, relevantID int64, msg string) {
	h.ps.onControlResponse(h.archive.last(h.t, kind).correlationID, relevantID, code, msg)
}

func (h *harness) toListResponse() {
	h.poll(2)
	h.expectState(StateAwaitListRecordingResponse)
}

func expectReason(t *testing.T, err error, reason Reason) {
	t.Helper()
	got, ok := ReasonOf(err)
	if !ok || got != reason {
		t.Fatalf("expected reason %s, got %v (%v)", reason, got, err)
	}
}

func TestStateNames(t *testing.T) {
	if StateAwaitArchiveConnection.String() != "AWAIT_ARCHIVE_CONNECTION" || StateFailed.String() != "FAILED" ||
		int(StateFailed) != 17 || StateLive.String() != "LIVE" {
		t.Fatal("state names/codes do not match Java")
	}
}

func TestContextValidation(t *testing.T) {
	valid := func() *Context {
		c := NewContext()
		c.RecordingID, c.LiveChannel, c.LiveStreamID = 1, "aeron:ipc", 1
		c.ReplayChannel, c.ReplayStreamID = "aeron:ipc", 2
		return c
	}
	if err := valid().validate(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	cases := map[string]func(c *Context){
		"recordingId must be set":   func(c *Context) { c.RecordingID = aeron.NullValue },
		"liveStreamId must be set":  func(c *Context) { c.LiveStreamID = aeron.NullValue },
		"liveChannel must be set":   func(c *Context) { c.LiveChannel = "" },
		"replayChannel must be set": func(c *Context) { c.ReplayChannel = "" },
		"replayStreamId must be set": func(c *Context) {
			c.ReplayStreamID = aeron.NullValue
		},
		"invalid recordingId":   func(c *Context) { c.RecordingID = -5 },
		"invalid startPosition": func(c *Context) { c.StartPosition = -3 },
		"not frame aligned":     func(c *Context) { c.StartPosition = 33 },
		"media type mismatch": func(c *Context) {
			c.ReplayChannel = "aeron:ipc?control-mode=response"
		},
	}
	for want, mutate := range cases {
		c := valid()
		mutate(c)
		err := c.validate()
		if err == nil || !errors.Is(err, ErrConfiguration) || !strings.Contains(err.Error(), want) {
			t.Errorf("%s: got %v", want, err)
		}
	}
}

func TestReplayChannelType(t *testing.T) {
	for channel, want := range map[string]replayChannelType{
		"aeron:ipc":                                   replayChannelSessionSpecific,
		"aeron:udp?endpoint=localhost:5000":           replayChannelSessionSpecific,
		"aeron:udp?endpoint=localhost:0":              replayChannelDynamicPort,
		"aeron:udp?control-mode=response|control=x:1": replayChannelResponse,
	} {
		uri, _ := aeron.ParseChannelUri(channel)
		if got := replayChannelTypeOf(uri); got != want {
			t.Errorf("%s: got %d want %d", channel, got, want)
		}
	}
}

func TestRegistrationErrorParsing(t *testing.T) {
	err := toRegistrationError(5, fmt.Errorf("error on 5: 10: resource temporarily unavailable"))
	if !err.IsTransient() || !isTransient(err) {
		t.Fatalf("expected transient: %v", err)
	}
	if isTransient(toRegistrationError(5, fmt.Errorf("error on 5: 1: bad channel"))) {
		t.Fatal("expected non transient")
	}
}

func TestRecordingNotFoundFails(t *testing.T) {
	h := newHarness(t, FromStart, "aeron:ipc")
	h.expectState(StateAwaitArchiveConnection)
	h.toListResponse()
	h.respond("list", codecs.ControlResponseCode.RECORDING_UNKNOWN, testRecordingID, "")
	h.poll(1)
	h.expectState(StateFailed)
	expectReason(t, h.ps.FailureReason(), ReasonRecordingNotFound)
	if !h.ps.HasFailed() || !h.archive.closed || len(h.listener.errs) != 1 {
		t.Fatalf("failed=%v archiveClosed=%v errs=%v", h.ps.HasFailed(), h.archive.closed, h.listener.errs)
	}
}

func TestStreamIDMismatchFails(t *testing.T) {
	h := newHarness(t, FromStart, "aeron:ipc")
	h.toListResponse()
	h.respondDescriptor(0, aeron.NullValue, testStreamID+1)
	h.poll(1)
	expectReason(t, h.ps.FailureReason(), ReasonStreamIDMismatch)
}

func TestInvalidStartPositions(t *testing.T) {
	for _, tc := range []struct{ start, recStart, recStop int64 }{
		{0, 1024, aeron.NullValue},
		{4096, 0, 2048},
	} {
		h := newHarness(t, tc.start, "aeron:ipc")
		h.toListResponse()
		h.respondDescriptor(tc.recStart, tc.recStop, testStreamID)
		h.poll(1)
		expectReason(t, h.ps.FailureReason(), ReasonInvalidStartPosition)
	}
}

func TestListRecordingTimeoutResends(t *testing.T) {
	h := newHarness(t, FromStart, "aeron:ipc")
	h.toListResponse()
	first := h.archive.last(t, "list").correlationID
	h.now += int64(testTimeout)
	h.poll(2)
	h.expectState(StateAwaitListRecordingResponse)
	if h.archive.last(t, "list").correlationID == first {
		t.Fatal("expected list recording request to be resent")
	}
}

func TestFromLiveJoinsLive(t *testing.T) {
	h := newHarness(t, FromLive, "aeron:ipc")
	h.toListResponse()
	h.respondDescriptor(0, aeron.NullValue, testStreamID)
	h.poll(2)
	h.expectState(StateAwaitLive)
	live := h.aeron.lastSub(t, "aeron:udp?endpoint=localhost:20121")
	live.resolved = true
	h.poll(1)
	h.expectState(StateAwaitLive)
	h.now += int64(testTimeout)
	h.poll(1)
	var event *Event
	if len(h.listener.errs) != 1 || !errors.As(h.listener.errs[0], &event) {
		t.Fatalf("expected no-image event, got %v", h.listener.errs)
	}
	live.sub.images = append(live.sub.images, &fakeImage{sessionID: 9, position: 4096})
	h.poll(1)
	h.expectState(StateLive)
	if h.listener.joined != 1 || h.ps.JoinDifference() != 0 || !h.ps.IsLive() {
		t.Fatalf("joined=%d joinDifference=%d", h.listener.joined, h.ps.JoinDifference())
	}

	// Live loss: refresh the descriptor and replay from the last live position.
	live.sub.images[0].closed = true
	h.poll(1)
	h.expectState(StateSendListRecordingRequest)
	if h.listener.left != 1 || h.ps.Position() != 4096 {
		t.Fatalf("left=%d position=%d", h.listener.left, h.ps.Position())
	}
	h.poll(1)
	h.respondDescriptor(0, aeron.NullValue, testStreamID)
	h.poll(1)
	h.expectState(StateSendReplayRequest)
	h.poll(1)
	if r := h.archive.last(t, "replay"); r.position != 4096 {
		t.Fatalf("expected replay from 4096, got %d", r.position)
	}
}

func TestLiveBehindLastPositionFails(t *testing.T) {
	h := newHarness(t, 8192, "aeron:ipc")
	h.toListResponse()
	h.respondDescriptor(0, 8192, testStreamID) // start == stop position: go straight to live
	h.poll(2)
	h.expectState(StateAwaitLive)
	live := h.aeron.lastSub(t, "aeron:udp?endpoint=localhost:20121")
	live.resolved = true
	live.sub.images = append(live.sub.images, &fakeImage{sessionID: 9, position: 4096})
	h.poll(1)
	h.expectState(StateFailed)
	expectReason(t, h.ps.FailureReason(), ReasonGeneric)
}

func TestReplayThenSwitchToLive(t *testing.T) {
	h := newHarness(t, FromStart, "aeron:ipc")
	h.toListResponse()
	h.respondDescriptor(0, aeron.NullValue, testStreamID)
	h.poll(1)
	h.expectState(StateSendReplayRequest)
	h.poll(1)
	h.expectState(StateAwaitReplayResponse)
	if r := h.archive.last(t, "replay"); r.position != 0 || r.channel != "aeron:ipc" {
		t.Fatalf("unexpected replay request %+v", r)
	}
	h.respond("replay", codecs.ControlResponseCode.OK, 55, "")
	h.poll(1)
	h.expectState(StateAddReplaySubscription)
	h.poll(1)
	replay := h.aeron.lastSub(t, "aeron:ipc")
	if !strings.Contains(replay.sub.channel, "session-id=55") {
		t.Fatalf("replay subscription should be session specific: %s", replay.sub.channel)
	}
	replay.resolved = true
	h.poll(1)
	h.expectState(StateReplay)
	replayImage := &fakeImage{sessionID: 55, position: 1000}
	replay.sub.images = append(replay.sub.images, replayImage)
	h.poll(1) // requests max recorded position
	h.respond("max", codecs.ControlResponseCode.OK, 1000, "")
	h.poll(1) // caught up: adds live subscription
	live := h.aeron.lastSub(t, "aeron:udp?endpoint=localhost:20121")
	live.resolved = true
	live.sub.images = append(live.sub.images, &fakeImage{sessionID: 9, position: 1000})
	h.poll(1)
	h.expectState(StateAttemptSwitch)
	if h.ps.JoinDifference() != 0 {
		t.Fatalf("join difference %d", h.ps.JoinDifference())
	}
	h.poll(1)
	h.expectState(StateLive)
	if h.listener.joined != 1 {
		t.Fatal("expected live joined")
	}
	if stop := h.archive.last(t, "stop"); stop.replaySession != 55 {
		t.Fatalf("expected replay session 55 stopped, got %+v", stop)
	}
}

func TestReplayRequestErrorReasons(t *testing.T) {
	for code, reason := range map[int64]Reason{int64(ArchiveErrorInvalidPosition): ReasonInvalidStartPosition,
		int64(ArchiveErrorUnknownRecording): ReasonRecordingNotFound, 99: ReasonGeneric} {
		h := newHarness(t, FromStart, "aeron:ipc")
		h.toListResponse()
		h.respondDescriptor(0, aeron.NullValue, testStreamID)
		h.poll(2)
		h.respond("replay", codecs.ControlResponseCode.ERROR, code, "boom")
		h.poll(1)
		expectReason(t, h.ps.FailureReason(), reason)
	}
}

func TestDynamicPortReplay(t *testing.T) {
	h := newHarness(t, FromStart, "aeron:udp?endpoint=localhost:0")
	h.toListResponse()
	h.respondDescriptor(0, aeron.NullValue, testStreamID)
	h.poll(1)
	h.expectState(StateAddReplaySubscription)
	h.poll(1)
	replay := h.aeron.lastSub(t, "aeron:udp?endpoint=localhost:0")
	replay.resolved = true
	h.poll(1)
	h.expectState(StateAwaitReplayChannelEndpoint)
	h.poll(1)
	h.expectState(StateAwaitReplayChannelEndpoint)
	replay.sub.endpoint = "aeron:udp?endpoint=127.0.0.1:40123"
	h.poll(2)
	if r := h.archive.last(t, "replay"); !strings.Contains(r.channel, "endpoint=127.0.0.1:40123") {
		t.Fatalf("replay request should use resolved endpoint: %s", r.channel)
	}
}

func TestTransientReplaySubscriptionErrorRetries(t *testing.T) {
	h := newHarness(t, FromStart, "aeron:udp?endpoint=localhost:0")
	h.toListResponse()
	h.respondDescriptor(0, aeron.NullValue, testStreamID)
	h.poll(2)
	replay := h.aeron.lastSub(t, "aeron:udp?endpoint=localhost:0")
	replay.err = &RegistrationError{ErrorCode: 10, Cause: errors.New("busy")}
	h.poll(1)
	h.expectState(StateAddReplaySubscription)
	if len(h.listener.errs) != 1 || h.ps.HasFailed() {
		t.Fatalf("expected non-terminal error, got %v", h.listener.errs)
	}
	h.poll(1)
	replay = h.aeron.lastSub(t, "aeron:udp?endpoint=localhost:0")
	replay.err = &RegistrationError{ErrorCode: 1, Cause: errors.New("bad")}
	h.poll(1)
	h.expectState(StateFailed)
}

func TestArchiveDisconnectDuringReplay(t *testing.T) {
	h := newHarness(t, FromStart, "aeron:ipc")
	h.toListResponse()
	h.respondDescriptor(0, aeron.NullValue, testStreamID)
	h.poll(2)
	h.respond("replay", codecs.ControlResponseCode.OK, 55, "")
	h.poll(2)
	replay := h.aeron.lastSub(t, "aeron:ipc")
	replay.resolved = true
	h.poll(1)
	replay.sub.images = append(replay.sub.images, &fakeImage{sessionID: 55, position: 2048})
	h.poll(1)
	h.archive.connected = false
	h.ps.onDisconnected()
	h.expectState(StateAwaitArchiveConnection)
	if h.ps.Position() != 2048 {
		t.Fatalf("expected position 2048, got %d", h.ps.Position())
	}
	h.archive.connected = true
	h.toListResponse()
	h.respondDescriptor(0, aeron.NullValue, testStreamID)
	h.poll(2)
	if r := h.archive.last(t, "replay"); r.position != 2048 {
		t.Fatalf("expected replay to resume from 2048, got %d", r.position)
	}
}

func TestArchiveErrorAfterCloseIsTerminal(t *testing.T) {
	h := newHarness(t, FromStart, "aeron:ipc")
	h.ps.onArchiveError(errors.New("transient"))
	if h.ps.HasFailed() {
		t.Fatal("error while archive open must be non-terminal")
	}
	h.archive.closed = true
	h.ps.onArchiveError(errors.New("fatal"))
	if !h.ps.HasFailed() || h.ps.FailureReason() == nil {
		t.Fatal("error after archive closed must be terminal")
	}
}

type (
	aeronBuffer = atomic.Buffer
	aeronHeader = logbuffer.Header
)
