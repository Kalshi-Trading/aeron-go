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

// Package persistentsubscription is a port of the Java client's io.aeron.archive.client.PersistentSubscription:
// a subscription to a recorded live stream that transparently starts from a position in the recording, replays
// from the archive, switches to the live stream once caught up (without gaps or duplicates) and falls back to
// replay if the live stream is lost.
package persistentsubscription

import (
	"time"

	"github.com/lirm/aeron-go/aeron"
	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/logbuffer"
	"github.com/lirm/aeron-go/aeron/logbuffer/term"
	"github.com/lirm/aeron-go/aeron/logging"
	"github.com/lirm/aeron-go/archive/codecs"
)

var logger = logging.MustGetLogger("persistentsubscription")

// replayAllAndFollow is the replay length that follows a live recording (Java's AeronArchive.REPLAY_ALL_AND_FOLLOW).
const replayAllAndFollow int64 = -1

// minInt64 is Java's Long.MIN_VALUE, the join difference before any join has been attempted.
const minInt64 int64 = -1 << 63

// pendingCloseTimeout bounds how long Close keeps releasing registrations that were still pending.
const pendingCloseTimeout = 5 * time.Second

type asyncArchiveOp struct {
	correlationID    int64
	deadlineNs       int64
	relevantID       int64
	code             codecs.ControlResponseCodeEnum
	errorMessage     string
	responseReceived bool
}

func (op *asyncArchiveOp) init(correlationID int64, deadlineNs int64) {
	op.correlationID = correlationID
	op.deadlineNs = deadlineNs
	op.responseReceived = false
}

func (op *asyncArchiveOp) onControlResponse(relevantID int64, code codecs.ControlResponseCodeEnum,
	errorMessage string) {
	op.relevantID = relevantID
	op.code = code
	op.errorMessage = errorMessage
	op.responseReceived = true
}

type listRecordingRequest struct {
	asyncArchiveOp
	remaining        int
	recordingID      int64
	startPosition    int64
	stopPosition     int64
	termBufferLength int32
	streamID         int32
}

func (r *listRecordingRequest) onRecordingDescriptor(d *codecs.RecordingDescriptor) {
	r.recordingID = d.RecordingId
	r.startPosition = d.StartPosition
	r.stopPosition = d.StopPosition
	r.termBufferLength = d.TermBufferLength
	r.streamID = d.StreamId
	r.remaining--
	if r.remaining == 0 {
		r.responseReceived = true
	}
}

type maxRecordedPositionState int

const (
	requestMaxPosition maxRecordedPositionState = iota
	awaitMaxPosition
	recheckRequired
)

// maxRecordedPosition decides when the replay is close enough to the recording's end to add the live subscription.
type maxRecordedPosition struct {
	asyncArchiveOp
	ps                   *PersistentSubscription
	state                maxRecordedPositionState
	maxRecordedPosition  int64
	closeEnoughThreshold int64
}

func (m *maxRecordedPosition) reset(closeEnoughThreshold int64) {
	m.closeEnoughThreshold = closeEnoughThreshold
	m.state = requestMaxPosition
}

func (m *maxRecordedPosition) isCaughtUp(replayedPosition int64) bool {
	ps := m.ps
	switch m.state {
	case requestMaxPosition:
		correlationID := ps.aeron.nextCorrelationID()
		if ps.archive.trySendMaxRecordedPositionRequest(correlationID, ps.recordingID) {
			m.init(correlationID, ps.nanoClock()+ps.messageTimeoutNs)
			m.state = awaitMaxPosition
		}
	case awaitMaxPosition:
		if m.responseReceived {
			if m.code == codecs.ControlResponseCode.OK {
				m.maxRecordedPosition = m.relevantID
				if replayedPosition >= m.maxRecordedPosition-m.closeEnoughThreshold {
					return true
				}
				m.state = recheckRequired
			} else {
				ps.setState(StateFailed)
				ps.onTerminalError(&ArchiveError{
					ErrorCode:     int32(m.relevantID),
					CorrelationID: m.correlationID,
					Message: "get max position request failed code=" + codeString(m.code) +
						" errorMessage='" + m.errorMessage + "'",
				})
			}
		} else if m.deadlineNs-ps.nanoClock() < 0 {
			m.state = requestMaxPosition
		}
	case recheckRequired:
		if replayedPosition >= m.maxRecordedPosition {
			m.state = requestMaxPosition
		}
	}
	return false
}

// PersistentSubscription consumes a recorded stream from a given position, replaying from the archive and
// switching seamlessly to the live stream. It is not safe for concurrent use: call Poll or ControlledPoll from a
// single goroutine (the duty cycle), which also drives the archive client and invokes the Listener.
type PersistentSubscription struct {
	ctx               *Context
	recordingID       int64
	listener          Listener
	liveChannel       string
	liveStreamID      int32
	replayChannel     string
	replayChannelURI  aeron.ChannelUri
	replayChannelType replayChannelType
	replayStreamID    int32
	aeron             aeronClient
	archive           archiveClient
	nanoClock         func() int64
	messageTimeoutNs  int64

	stateCounter          *aeron.Counter
	joinDifferenceCounter *aeron.Counter
	liveLeftCounter       *aeron.Counter
	liveJoinedCounter     *aeron.Counter

	state State

	listRecordingRequest listRecordingRequest
	maxRecordedPosition  maxRecordedPosition
	replayRequest        asyncArchiveOp
	replayTokenRequest   asyncArchiveOp

	replaySessionID      int64
	replaySubscriptionID int64
	replaySubscription   subscriptionHandle
	replayImageDeadline  int64
	replayImage          aeron.Image
	requestPublicationID int64
	requestPublication   requestPublication
	replayToken          int64

	liveSubscriptionID        int64
	liveSubscription          subscriptionHandle
	liveImageDeadline         int64
	liveImageDeadlineBreached bool
	liveImage                 aeron.Image

	controlledFragmentHandler   term.ControlledFragmentHandler
	uncontrolledFragmentHandler term.FragmentHandler
	controlledAssembler         imageControlledFragmentAssembler
	uncontrolledAssembler       imageFragmentAssembler

	joinDifference           int64
	nextLivePosition         int64
	position                 int64
	lastObservedLivePosition int64
	failureReason            error
	closed                   bool
}

// New creates a PersistentSubscription. The Context is validated and the Aeron client and counters allocated if
// not supplied. Nothing else happens until Poll or ControlledPoll is called.
func New(ctx *Context) (*PersistentSubscription, error) {
	if err := ctx.conclude(); err != nil {
		return nil, err
	}
	ps := newPersistentSubscription(ctx, newRealAeronClient(ctx.Aeron))
	ps.archive = newAsyncArchive(ctx.ArchiveOptions, ctx.newArchiveAeronContext, ps, ctx.NanoClock)
	return ps, nil
}

// newPersistentSubscription wires a concluded (or test) context; the caller must set ps.archive.
func newPersistentSubscription(ctx *Context, aeronClient aeronClient) *PersistentSubscription {
	replayChannelURI, _ := aeron.ParseChannelUri(ctx.ReplayChannel)
	ps := &PersistentSubscription{
		ctx:                      ctx,
		recordingID:              ctx.RecordingID,
		listener:                 ctx.Listener,
		liveChannel:              ctx.LiveChannel,
		liveStreamID:             ctx.LiveStreamID,
		replayChannel:            ctx.ReplayChannel,
		replayChannelURI:         replayChannelURI,
		replayChannelType:        replayChannelTypeOf(replayChannelURI),
		replayStreamID:           ctx.ReplayStreamID,
		aeron:                    aeronClient,
		nanoClock:                ctx.NanoClock,
		messageTimeoutNs:         int64(ctx.archiveOptions().Timeout),
		stateCounter:             ctx.StateCounter,
		joinDifferenceCounter:    ctx.JoinDifferenceCounter,
		liveLeftCounter:          ctx.LiveLeftCounter,
		liveJoinedCounter:        ctx.LiveJoinedCounter,
		state:                    StateAwaitArchiveConnection,
		replaySessionID:          aeron.NullValue,
		replaySubscriptionID:     aeron.NullValue,
		requestPublicationID:     aeron.NullValue,
		replayToken:              aeron.NullValue,
		liveSubscriptionID:       aeron.NullValue,
		nextLivePosition:         aeron.NullValue,
		position:                 ctx.StartPosition,
		lastObservedLivePosition: aeron.NullValue,
	}
	if ps.listener == nil {
		ps.listener = ListenerFuncs{}
	}
	for _, op := range []*asyncArchiveOp{&ps.listRecordingRequest.asyncArchiveOp,
		&ps.maxRecordedPosition.asyncArchiveOp, &ps.replayRequest, &ps.replayTokenRequest} {
		op.correlationID = aeron.NullValue
	}
	ps.maxRecordedPosition.ps = ps
	ps.controlledAssembler.delegate = ps.onFragmentControlled
	ps.uncontrolledAssembler.delegate = ps.onFragmentUncontrolled
	ps.setJoinDifference(minInt64)
	setCounter(ps.stateCounter, int64(ps.state))
	return ps
}

func replayChannelTypeOf(uri aeron.ChannelUri) replayChannelType {
	if uri.Get("control-mode") == "response" {
		return replayChannelResponse
	}
	if uri.IsUdp() {
		endpoint := uri.Get("endpoint")
		if len(endpoint) >= 2 && endpoint[len(endpoint)-2:] == ":0" {
			return replayChannelDynamicPort
		}
	}
	return replayChannelSessionSpecific
}

func setCounter(c *aeron.Counter, value int64) {
	if c != nil && !c.IsClosed() {
		c.Counter().Set(value)
	}
}

func incrementCounter(c *aeron.Counter) {
	if c != nil && !c.IsClosed() {
		c.Counter().Set(c.Counter().Get() + 1)
	}
}

// Poll polls for messages, delivering complete (reassembled) messages to handler, and performs the state machine
// work. Returns the amount of work done (fragments read plus state machine work).
func (ps *PersistentSubscription) Poll(handler term.FragmentHandler, fragmentLimit int) int {
	ps.uncontrolledFragmentHandler = handler
	defer func() { ps.uncontrolledFragmentHandler = nil }()
	return ps.doWork(fragmentLimit, false)
}

// ControlledPoll is like Poll but the handler controls consumption via its returned action.
func (ps *PersistentSubscription) ControlledPoll(handler term.ControlledFragmentHandler, fragmentLimit int) int {
	ps.controlledFragmentHandler = handler
	defer func() { ps.controlledFragmentHandler = nil }()
	return ps.doWork(fragmentLimit, true)
}

func (ps *PersistentSubscription) doWork(fragmentLimit int, isControlled bool) int {
	if ps.closed {
		return 0
	}
	workCount := ps.aeron.doWork()
	workCount += ps.archive.poll()
	switch ps.state {
	case StateAwaitArchiveConnection:
		workCount += ps.awaitArchiveConnection()
	case StateSendListRecordingRequest:
		workCount += ps.sendListRecordingRequest()
	case StateAwaitListRecordingResponse:
		workCount += ps.awaitListRecordingResponse()
	case StateSendReplayRequest:
		workCount += ps.sendReplayRequest()
	case StateAwaitReplayResponse:
		workCount += ps.awaitReplayResponse()
	case StateAddReplaySubscription:
		workCount += ps.addReplaySubscription()
	case StateAwaitReplaySubscription:
		workCount += ps.awaitReplaySubscription()
	case StateAwaitReplayChannelEndpoint:
		workCount += ps.awaitReplayChannelEndpoint()
	case StateAddRequestPublication:
		workCount += ps.addRequestPublication()
	case StateAwaitRequestPublication:
		workCount += ps.awaitRequestPublication()
	case StateSendReplayTokenRequest:
		workCount += ps.sendReplayTokenRequest()
	case StateAwaitReplayToken:
		workCount += ps.awaitReplayToken()
	case StateReplay:
		workCount += ps.replay(fragmentLimit, isControlled)
	case StateAttemptSwitch:
		workCount += ps.attemptSwitch(fragmentLimit, isControlled)
	case StateAddLiveSubscription:
		workCount += ps.addLiveSubscription()
	case StateAwaitLive:
		workCount += ps.awaitLive()
	case StateLive:
		workCount += ps.live(fragmentLimit, isControlled)
	}
	return workCount
}

// State returns the current state.
func (ps *PersistentSubscription) State() State {
	return ps.state
}

// IsLive reports whether messages are being consumed from the live stream.
func (ps *PersistentSubscription) IsLive() bool {
	return ps.state == StateLive
}

// IsReplaying reports whether messages are being consumed from a replay (including while switching to live).
func (ps *PersistentSubscription) IsReplaying() bool {
	return ps.state == StateReplay || ps.state == StateAttemptSwitch
}

// HasFailed reports whether the PersistentSubscription has terminally failed; see FailureReason.
func (ps *PersistentSubscription) HasFailed() bool {
	return ps.state == StateFailed
}

// FailureReason returns the terminal error once HasFailed is true, otherwise nil.
func (ps *PersistentSubscription) FailureReason() error {
	return ps.failureReason
}

// JoinDifference returns live position minus replay position when the last switch to live was attempted, 0 when
// joined live directly, or math.MinInt64 when no join has been attempted since the last replay set up.
func (ps *PersistentSubscription) JoinDifference() int64 {
	return ps.joinDifference
}

// Position returns the position up to which the stream has been consumed (or the start position before any
// consumption).
func (ps *PersistentSubscription) Position() int64 {
	return ps.position
}

// Close releases all resources: live/replay subscriptions, the replay session, the archive session, the counters
// and the Aeron client if owned.
func (ps *PersistentSubscription) Close() error {
	if ps.closed {
		return nil
	}
	ps.closed = true
	ps.closeLive()
	ps.closeReplay()
	ps.archive.close()
	if real, ok := ps.aeron.(*realAeronClient); ok && !ps.ctx.OwnsAeronClient && real.hasPending() {
		go func() {
			deadline := time.Now().Add(pendingCloseTimeout)
			for real.hasPending() && time.Now().Before(deadline) && !real.isClosed() {
				real.doWork()
				time.Sleep(time.Millisecond)
			}
		}()
	}
	ps.ctx.close()
	return nil
}

func (ps *PersistentSubscription) closeLive() {
	if !ps.ctx.OwnsAeronClient {
		ps.cleanUpLiveSubscription()
	}
}

func (ps *PersistentSubscription) closeReplay() {
	ps.cleanUpReplay()
	if !ps.ctx.OwnsAeronClient {
		ps.cleanUpRequestPublication()
		ps.cleanUpReplaySubscription()
	}
}

func (ps *PersistentSubscription) setJoinDifference(joinDifference int64) {
	ps.joinDifference = joinDifference
	setCounter(ps.joinDifferenceCounter, joinDifference)
}

func (ps *PersistentSubscription) awaitArchiveConnection() int {
	if !ps.archive.isConnected() {
		return 0
	}
	ps.setState(StateSendListRecordingRequest)
	return 1
}

func (ps *PersistentSubscription) sendListRecordingRequest() int {
	correlationID := ps.aeron.nextCorrelationID()
	if !ps.archive.trySendListRecordingRequest(correlationID, ps.recordingID) {
		if ps.archive.isConnected() {
			return 0
		}
		ps.setState(StateAwaitArchiveConnection)
		return 1
	}
	ps.listRecordingRequest.init(correlationID, ps.nanoClock()+ps.messageTimeoutNs)
	ps.listRecordingRequest.remaining = 1
	ps.setState(StateAwaitListRecordingResponse)
	return 1
}

func (ps *PersistentSubscription) awaitListRecordingResponse() int {
	if !ps.listRecordingRequest.responseReceived {
		if ps.nanoClock()-ps.listRecordingRequest.deadlineNs >= 0 {
			if ps.archive.isConnected() {
				ps.setState(StateSendListRecordingRequest)
			} else {
				ps.setState(StateAwaitArchiveConnection)
			}
			return 1
		}
		return 0
	}

	if err := ps.validateDescriptor(); err != nil {
		ps.setState(StateFailed)
		ps.onTerminalError(err)
	} else if ps.position == FromLive ||
		(ps.listRecordingRequest.stopPosition != aeron.NullValue &&
			ps.position == ps.listRecordingRequest.stopPosition) {
		ps.setState(StateAddLiveSubscription)
	} else {
		ps.setUpReplay()
	}
	return 1
}

func (ps *PersistentSubscription) validateDescriptor() error {
	r := &ps.listRecordingRequest
	if r.remaining != 0 {
		if r.code == codecs.ControlResponseCode.RECORDING_UNKNOWN {
			return newError(ReasonRecordingNotFound, "unknown recording id: %d", ps.recordingID)
		}
		return newError(ReasonGeneric, "list recording request for recording id %d failed code=%s: %s",
			ps.recordingID, codeString(r.code), r.errorMessage)
	}
	if ps.liveStreamID != r.streamID {
		return newError(ReasonStreamIDMismatch,
			"Requested live stream with ID: %d does not match stream ID: %d for recording: %d",
			ps.liveStreamID, r.streamID, ps.recordingID)
	}
	if r.stopPosition != aeron.NullValue && ps.lastObservedLivePosition > r.stopPosition {
		return newError(ReasonInvalidStartPosition,
			"recording %d stopped at position=%d which is earlier than last observed live position=%d",
			ps.recordingID, r.stopPosition, ps.lastObservedLivePosition)
	}
	if ps.position >= 0 {
		if ps.position < r.startPosition {
			return newError(ReasonInvalidStartPosition,
				"requested replay start position=%d is less than recording start position=%d for recording %d",
				ps.position, r.startPosition, ps.recordingID)
		}
		if r.stopPosition != aeron.NullValue && ps.position > r.stopPosition {
			return newError(ReasonInvalidStartPosition,
				"requested replay start position=%d must be less than the limit position=%d for recording %d",
				ps.position, r.stopPosition, ps.recordingID)
		}
	} else if ps.position == FromStart {
		ps.position = r.startPosition
	}
	return nil
}

func (ps *PersistentSubscription) setUpReplay() {
	ps.resetReplayCatchupState()
	if ps.replayChannelType == replayChannelSessionSpecific {
		ps.setState(StateSendReplayRequest)
	} else {
		ps.setState(StateAddReplaySubscription)
	}
}

func (ps *PersistentSubscription) refreshRecordingDescriptor() {
	ps.resetReplayCatchupState()
	ps.setState(StateSendListRecordingRequest)
}

func (ps *PersistentSubscription) resetReplayCatchupState() {
	ps.setJoinDifference(minInt64)
	ps.maxRecordedPosition.reset(int64(ps.listRecordingRequest.termBufferLength >> 2))
	ps.nextLivePosition = aeron.NullValue
}

func (ps *PersistentSubscription) cleanUpReplay() {
	if ps.replaySessionID != aeron.NullValue {
		ps.archive.trySendStopReplayRequest(ps.aeron.nextCorrelationID(), ps.replaySessionID)
		ps.replaySessionID = aeron.NullValue
	}
}

func (ps *PersistentSubscription) cleanUpReplaySubscription() {
	if ps.replaySubscriptionID != aeron.NullValue {
		ps.aeron.asyncRemoveSubscription(ps.replaySubscriptionID)
	}
	if ps.replaySubscription != nil {
		ps.aeron.asyncRemoveSubscription(ps.replaySubscription.registrationID())
	}
	ps.replaySubscriptionID = aeron.NullValue
	ps.replaySubscription = nil
	ps.replayImage = nil
}

func (ps *PersistentSubscription) cleanUpRequestPublication() {
	if ps.requestPublicationID != aeron.NullValue {
		ps.aeron.asyncRemovePublication(ps.requestPublicationID)
	}
	ps.requestPublicationID = aeron.NullValue
	ps.requestPublication = nil
}

func (ps *PersistentSubscription) cleanUpLiveSubscription() {
	if ps.liveSubscriptionID != aeron.NullValue {
		ps.aeron.asyncRemoveSubscription(ps.liveSubscriptionID)
	}
	if ps.liveSubscription != nil {
		ps.aeron.asyncRemoveSubscription(ps.liveSubscription.registrationID())
	}
	ps.liveSubscriptionID = aeron.NullValue
	ps.liveSubscription = nil
	ps.liveImage = nil
}

func (ps *PersistentSubscription) sendReplayRequest() int {
	correlationID := ps.aeron.nextCorrelationID()
	channel := ps.replayChannel
	if ps.replayChannelType != replayChannelSessionSpecific {
		channel = ps.replayChannelURI.String()
	}

	var sent bool
	if ps.replayChannelType == replayChannelResponse {
		sent = ps.archive.trySendReplayRequestOn(ps.requestPublication, correlationID, ps.recordingID, ps.position,
			replayAllAndFollow, channel, ps.replayStreamID, ps.replayToken)
	} else {
		sent = ps.archive.trySendReplayRequest(correlationID, ps.recordingID, ps.position, replayAllAndFollow,
			channel, ps.replayStreamID)
	}
	if !sent {
		if ps.archive.isConnected() {
			return 0
		}
		ps.cleanUpRequestPublication()
		ps.cleanUpReplaySubscription()
		ps.setState(StateAwaitArchiveConnection)
		return 1
	}
	ps.replayRequest.init(correlationID, ps.nanoClock()+ps.messageTimeoutNs)
	ps.setState(StateAwaitReplayResponse)
	return 1
}

func (ps *PersistentSubscription) awaitReplayResponse() int {
	if !ps.replayRequest.responseReceived {
		if ps.nanoClock()-ps.replayRequest.deadlineNs >= 0 {
			ps.cleanUpRequestPublication()
			ps.cleanUpReplaySubscription()
			if ps.archive.isConnected() {
				ps.setUpReplay()
			} else {
				ps.setState(StateAwaitArchiveConnection)
			}
			return 1
		}
		return 0
	}

	if ps.replayRequest.code != codecs.ControlResponseCode.OK {
		ps.setState(StateFailed)
		ps.cleanUpRequestPublication()
		ps.cleanUpReplaySubscription()
		reason := ReasonGeneric
		switch int32(ps.replayRequest.relevantID) {
		case ArchiveErrorInvalidPosition:
			reason = ReasonInvalidStartPosition
		case ArchiveErrorUnknownRecording:
			reason = ReasonRecordingNotFound
		}
		ps.onTerminalError(newError(reason, "replay request failed: %s", ps.replayRequest.errorMessage))
		return 1
	}

	ps.replaySessionID = ps.replayRequest.relevantID
	switch ps.replayChannelType {
	case replayChannelSessionSpecific:
		ps.replayChannelURI.Set("session-id", itoa(int64(int32(ps.replaySessionID))))
		ps.setState(StateAddReplaySubscription)
	case replayChannelDynamicPort:
		ps.replayImageDeadline = ps.nanoClock() + ps.messageTimeoutNs
		ps.setState(StateReplay)
	case replayChannelResponse:
		ps.cleanUpRequestPublication()
		ps.replayImageDeadline = ps.nanoClock() + ps.messageTimeoutNs
		ps.setState(StateReplay)
	}
	return 1
}

func (ps *PersistentSubscription) addReplaySubscription() int {
	channel := ps.replayChannel
	if ps.replayChannelType == replayChannelSessionSpecific {
		channel = ps.replayChannelURI.String()
	}
	id, err := ps.aeron.asyncAddSubscription(channel, ps.replayStreamID)
	if err != nil {
		ps.cleanUpReplay()
		ps.setState(StateFailed)
		ps.onTerminalError(err)
		return 1
	}
	ps.replaySubscriptionID = id
	ps.setState(StateAwaitReplaySubscription)
	return 1
}

func (ps *PersistentSubscription) awaitReplaySubscription() int {
	subscription, err := ps.aeron.getSubscription(ps.replaySubscriptionID)
	if err != nil {
		ps.replaySubscriptionID = aeron.NullValue
		ps.cleanUpReplay()
		if isTransient(err) {
			ps.setUpReplay()
			ps.listener.OnError(err)
		} else {
			ps.setState(StateFailed)
			ps.onTerminalError(err)
		}
		return 1
	}
	if subscription == nil {
		return 0
	}
	ps.replaySubscriptionID = aeron.NullValue
	ps.replaySubscription = subscription
	switch ps.replayChannelType {
	case replayChannelSessionSpecific:
		ps.replayImageDeadline = ps.nanoClock() + ps.messageTimeoutNs
		ps.setState(StateReplay)
	case replayChannelDynamicPort:
		ps.setState(StateAwaitReplayChannelEndpoint)
	case replayChannelResponse:
		ps.setState(StateAddRequestPublication)
	}
	return 1
}

func (ps *PersistentSubscription) awaitReplayChannelEndpoint() int {
	resolvedChannel := ps.replaySubscription.resolvedEndpoint()
	if resolvedChannel == "" {
		return 0
	}
	if uri, err := aeron.ParseChannelUri(resolvedChannel); err == nil {
		if endpoint := uri.Get("endpoint"); endpoint != "" {
			ps.replayChannelURI.Set("endpoint", endpoint)
		}
	}
	ps.setState(StateSendReplayRequest)
	return 1
}

func (ps *PersistentSubscription) addRequestPublication() int {
	options := ps.ctx.archiveOptions()
	uri, err := aeron.ParseChannelUri(options.RequestChannel)
	if err != nil {
		ps.setState(StateFailed)
		ps.onTerminalError(err)
		return 1
	}
	for _, key := range []string{"session-id", "term-id", "init-term-id", "term-offset"} {
		uri.Remove(key)
	}
	uri.Set("response-correlation-id", itoa(ps.replaySubscription.registrationID()))
	uri.Set("ssc", "false")
	id, err := ps.aeron.asyncAddExclusivePublication(uri.String(), options.RequestStream)
	if err != nil {
		ps.cleanUpReplaySubscription()
		ps.setState(StateFailed)
		ps.onTerminalError(err)
		return 1
	}
	ps.requestPublicationID = id
	ps.setState(StateAwaitRequestPublication)
	return 1
}

func (ps *PersistentSubscription) awaitRequestPublication() int {
	publication, err := ps.aeron.getExclusivePublication(ps.requestPublicationID)
	if err != nil {
		ps.cleanUpRequestPublication()
		ps.cleanUpReplaySubscription()
		if isTransient(err) {
			ps.setUpReplay()
			ps.listener.OnError(err)
		} else {
			ps.setState(StateFailed)
			ps.onTerminalError(err)
		}
		return 1
	}
	if publication == nil {
		return 0
	}
	ps.requestPublication = publication
	ps.setState(StateSendReplayTokenRequest)
	return 1
}

func (ps *PersistentSubscription) sendReplayTokenRequest() int {
	correlationID := ps.aeron.nextCorrelationID()
	if !ps.archive.trySendReplayTokenRequest(correlationID, ps.recordingID) {
		if ps.archive.isConnected() {
			return 0
		}
		ps.cleanUpRequestPublication()
		ps.cleanUpReplaySubscription()
		ps.setState(StateAwaitArchiveConnection)
		return 1
	}
	ps.replayTokenRequest.init(correlationID, ps.nanoClock()+ps.messageTimeoutNs)
	ps.setState(StateAwaitReplayToken)
	return 1
}

func (ps *PersistentSubscription) awaitReplayToken() int {
	if !ps.replayTokenRequest.responseReceived {
		if ps.nanoClock()-ps.replayTokenRequest.deadlineNs >= 0 {
			ps.cleanUpRequestPublication()
			ps.cleanUpReplaySubscription()
			if ps.archive.isConnected() {
				ps.setUpReplay()
			} else {
				ps.setState(StateAwaitArchiveConnection)
			}
			return 1
		}
		return 0
	}
	if ps.replayTokenRequest.code != codecs.ControlResponseCode.OK {
		ps.setState(StateFailed)
		ps.cleanUpRequestPublication()
		ps.cleanUpReplaySubscription()
		ps.onTerminalError(&ArchiveError{
			ErrorCode:     int32(ps.replayTokenRequest.relevantID),
			CorrelationID: ps.replayTokenRequest.correlationID,
			Message:       "replay token request failed: " + ps.replayTokenRequest.errorMessage,
		})
		return 1
	}
	ps.replayToken = ps.replayTokenRequest.relevantID
	if ps.replayChannelURI.IsIpc() {
		ps.setState(StateSendReplayRequest)
	} else {
		ps.setState(StateAwaitReplayChannelEndpoint)
	}
	return 1
}

func (ps *PersistentSubscription) replay(fragmentLimit int, isControlled bool) int {
	replayImage := ps.replayImage
	if replayImage == nil {
		replayImage = ps.replaySubscription.imageBySessionID(int32(ps.replaySessionID))
		if replayImage == nil {
			if ps.nanoClock()-ps.replayImageDeadline >= 0 {
				ps.cleanUpReplay()
				ps.cleanUpReplaySubscription()
				ps.setUpReplay()
				return 1
			}
			return 0
		}
		ps.replayImage = replayImage
	}

	if replayImage.IsClosed() {
		ps.position = replayImage.Position()
		ps.cleanUpLiveSubscription()
		ps.cleanUpReplay()
		ps.cleanUpReplaySubscription()
		ps.refreshRecordingDescriptor()
		return 1
	}

	if ps.liveSubscription == nil && ps.liveSubscriptionID != aeron.NullValue {
		subscription, err := ps.aeron.getSubscription(ps.liveSubscriptionID)
		if err != nil {
			ps.liveSubscriptionID = aeron.NullValue
			if !isTransient(err) {
				ps.cleanUpReplay()
				ps.cleanUpReplaySubscription()
				ps.setState(StateFailed)
				ps.onTerminalError(err)
			} else {
				ps.listener.OnError(err)
			}
			return 1
		}
		if subscription != nil {
			ps.liveSubscription = subscription
			ps.liveSubscriptionID = aeron.NullValue
			ps.setLiveImageDeadline()
		}
	}

	if ps.liveSubscription != nil {
		if ps.liveSubscription.imageCount() > 0 {
			if liveImage := ps.liveSubscription.imageAtIndex(0); liveImage != nil {
				ps.liveImage = liveImage
				ps.setJoinDifference(liveImage.Position() - replayImage.Position())
				ps.setState(StateAttemptSwitch)
				return 1
			}
		} else if !ps.liveImageDeadlineBreached && ps.nanoClock()-ps.liveImageDeadline >= 0 {
			ps.onLiveImageDeadlineBreached()
		}
	}

	fragments := ps.doPoll(replayImage, fragmentLimit, isControlled)
	ps.position = replayImage.Position()

	if ps.liveSubscriptionID == aeron.NullValue && ps.liveSubscription == nil &&
		ps.state == StateReplay && ps.maxRecordedPosition.isCaughtUp(ps.position) {
		ps.doAddLiveSubscription()
	}
	return fragments
}

// doAddLiveSubscription adds the live subscription; returns false (after failing) if the driver rejected it.
func (ps *PersistentSubscription) doAddLiveSubscription() bool {
	ps.liveImage = nil
	ps.liveSubscription = nil
	id, err := ps.aeron.asyncAddSubscription(ps.liveChannel, ps.liveStreamID)
	if err != nil {
		ps.cleanUpReplay()
		ps.cleanUpReplaySubscription()
		ps.setState(StateFailed)
		ps.onTerminalError(err)
		return false
	}
	ps.liveSubscriptionID = id
	return true
}

func (ps *PersistentSubscription) setLiveImageDeadline() {
	ps.liveImageDeadline = ps.nanoClock() + ps.messageTimeoutNs
	ps.liveImageDeadlineBreached = false
}

func (ps *PersistentSubscription) onLiveImageDeadlineBreached() {
	ps.liveImageDeadlineBreached = true
	ps.listener.OnError(&Event{Message: "No image became available on the live subscription within " +
		time.Duration(ps.messageTimeoutNs).String() + ". This could be caused by the publisher being down, or by " +
		"a misconfiguration of the subscriber or a firewall between them."})
}

func (ps *PersistentSubscription) attemptSwitch(fragmentLimit int, isControlled bool) int {
	fragments := 0
	livePosition := ps.liveImage.Position()
	replayPosition := ps.replayImage.Position()

	if replayPosition == livePosition {
		ps.setState(StateLive)
	} else {
		if ps.replayImage.IsClosed() {
			ps.position = replayPosition
			ps.advanceLastObservedLivePosition(livePosition)
			ps.cleanUpLiveSubscription()
			ps.cleanUpReplay()
			ps.cleanUpReplaySubscription()
			ps.refreshRecordingDescriptor()
			return 1
		}
		if ps.liveImage.IsClosed() {
			ps.cleanUpLiveSubscription()
			ps.resetReplayCatchupState()
			ps.setState(StateReplay)
			return 1
		}
		fragments += ps.liveImage.ControlledPoll(ps.onLiveCatchupFragment, fragmentLimit)
		if isControlled {
			fragments += ps.replayImage.ControlledPoll(ps.onReplayCatchupFragmentControlled, fragmentLimit)
		} else {
			fragments += ps.replayImage.ControlledPoll(ps.onReplayCatchupFragmentUncontrolled, fragmentLimit)
		}
		ps.position = ps.replayImage.Position()
	}

	if ps.IsLive() {
		ps.position = ps.liveImage.Position()
		ps.cleanUpReplay()
		ps.cleanUpReplaySubscription()
		ps.onLiveJoined()
	}
	return fragments
}

func (ps *PersistentSubscription) onTerminalError(err error) {
	ps.failureReason = err
	ps.listener.OnError(err)
}

func (ps *PersistentSubscription) onLiveCatchupFragment(_ *atomic.Buffer, _ int32, _ int32,
	header *logbuffer.Header) term.ControlledPollAction {
	currentLivePosition := header.Position()
	if currentLivePosition <= ps.replayImage.Position() {
		return term.ControlledPollActionContinue
	}
	ps.nextLivePosition = currentLivePosition
	return term.ControlledPollActionAbort
}

func (ps *PersistentSubscription) onReplayCatchupFragmentControlled(buffer *atomic.Buffer, offset int32,
	length int32, header *logbuffer.Header) term.ControlledPollAction {
	if header.Position() == ps.nextLivePosition {
		ps.setState(StateLive)
		return term.ControlledPollActionAbort
	}
	return ps.controlledAssembler.onFragment(buffer, offset, length, header)
}

func (ps *PersistentSubscription) onReplayCatchupFragmentUncontrolled(buffer *atomic.Buffer, offset int32,
	length int32, header *logbuffer.Header) term.ControlledPollAction {
	if header.Position() == ps.nextLivePosition {
		ps.setState(StateLive)
		return term.ControlledPollActionAbort
	}
	ps.uncontrolledAssembler.onFragment(buffer, offset, length, header)
	return term.ControlledPollActionContinue
}

func (ps *PersistentSubscription) addLiveSubscription() int {
	if ps.doAddLiveSubscription() {
		ps.setState(StateAwaitLive)
	}
	return 1
}

func (ps *PersistentSubscription) awaitLive() int {
	if ps.liveSubscription == nil {
		subscription, err := ps.aeron.getSubscription(ps.liveSubscriptionID)
		if err != nil {
			ps.liveSubscriptionID = aeron.NullValue
			if isTransient(err) {
				ps.setState(StateAddLiveSubscription)
				ps.listener.OnError(err)
			} else {
				ps.setState(StateFailed)
				ps.onTerminalError(err)
			}
			return 1
		}
		if subscription != nil {
			ps.liveSubscription = subscription
			ps.liveSubscriptionID = aeron.NullValue
			ps.setLiveImageDeadline()
		}
	}

	if ps.liveSubscription != nil {
		if ps.liveSubscription.imageCount() > 0 {
			image := ps.liveSubscription.imageAtIndex(0)
			if image == nil {
				return 0
			}
			livePosition := image.Position()
			ps.advanceLastObservedLivePosition(livePosition)
			if ps.position >= 0 {
				if livePosition < ps.position {
					ps.cleanUpLiveSubscription()
					ps.setState(StateFailed)
					ps.onTerminalError(newError(ReasonGeneric,
						"live stream joined at position %d which is earlier than last seen position %d",
						livePosition, ps.position))
					return 1
				} else if livePosition > ps.position {
					ps.cleanUpLiveSubscription()
					ps.refreshRecordingDescriptor()
					return 1
				}
			}
			ps.liveImage = image
			ps.position = livePosition
			ps.setJoinDifference(0)
			ps.setState(StateLive)
			ps.onLiveJoined()
			return 1
		} else if !ps.liveImageDeadlineBreached && ps.nanoClock()-ps.liveImageDeadline >= 0 {
			ps.onLiveImageDeadlineBreached()
		}
	}
	return 0
}

func (ps *PersistentSubscription) live(fragmentLimit int, isControlled bool) int {
	image := ps.liveImage
	fragments := ps.doPoll(image, fragmentLimit, isControlled)
	ps.position = image.Position()
	if fragments == 0 && image.IsClosed() {
		finalPosition := image.Position()
		ps.advanceLastObservedLivePosition(finalPosition)
		ps.position = finalPosition
		ps.cleanUpLiveSubscription()
		ps.refreshRecordingDescriptor()
		ps.onLiveLeft()
		return 1
	}
	return fragments
}

func (ps *PersistentSubscription) advanceLastObservedLivePosition(livePosition int64) {
	if livePosition > ps.lastObservedLivePosition {
		ps.lastObservedLivePosition = livePosition
	}
}

func (ps *PersistentSubscription) setState(newState State) {
	if newState == ps.state {
		return
	}
	logger.Debugf("PersistentSubscription state change: %s -> %s recordingId=%d", ps.state, newState,
		ps.recordingID)
	ps.state = newState
	setCounter(ps.stateCounter, int64(newState))
	if newState == StateFailed {
		ps.archive.close()
	}
}

func (ps *PersistentSubscription) onLiveJoined() {
	logger.Debugf("PersistentSubscription joined live recordingId=%d liveSessionId=%d position=%d",
		ps.recordingID, ps.liveImage.SessionID(), ps.liveImage.Position())
	incrementCounter(ps.liveJoinedCounter)
	ps.listener.OnLiveJoined()
}

func (ps *PersistentSubscription) onLiveLeft() {
	logger.Debugf("PersistentSubscription left live recordingId=%d position=%d", ps.recordingID, ps.position)
	incrementCounter(ps.liveLeftCounter)
	ps.listener.OnLiveLeft()
}

func (ps *PersistentSubscription) doPoll(image aeron.Image, fragmentLimit int, isControlled bool) int {
	if isControlled {
		return image.ControlledPoll(ps.controlledAssembler.onFragment, fragmentLimit)
	}
	return image.Poll(ps.uncontrolledAssembler.onFragment, fragmentLimit)
}

func (ps *PersistentSubscription) onFragmentControlled(buffer *atomic.Buffer, offset int32, length int32,
	header *logbuffer.Header) term.ControlledPollAction {
	return ps.controlledFragmentHandler(buffer, offset, length, header)
}

func (ps *PersistentSubscription) onFragmentUncontrolled(buffer *atomic.Buffer, offset int32, length int32,
	header *logbuffer.Header) {
	ps.uncontrolledFragmentHandler(buffer, offset, length, header)
}

// archiveListener implementation (Java's PersistentSubscription.ArchiveListener).

func (ps *PersistentSubscription) onConnected() {}

func (ps *PersistentSubscription) onDisconnected() {
	switch ps.state {
	case StateAwaitArchiveConnection, StateAttemptSwitch, StateLive, StateFailed:
		return
	}
	if ps.replayImage != nil {
		ps.position = ps.replayImage.Position()
	}
	ps.cleanUpRequestPublication()
	ps.cleanUpLiveSubscription()
	ps.cleanUpReplay()
	ps.cleanUpReplaySubscription()
	ps.setState(StateAwaitArchiveConnection)
}

func (ps *PersistentSubscription) onControlResponse(correlationID int64, relevantID int64,
	code codecs.ControlResponseCodeEnum, errorMessage string) {
	switch correlationID {
	case ps.maxRecordedPosition.correlationID:
		ps.maxRecordedPosition.onControlResponse(relevantID, code, errorMessage)
	case ps.listRecordingRequest.correlationID:
		ps.listRecordingRequest.onControlResponse(relevantID, code, errorMessage)
	case ps.replayRequest.correlationID:
		ps.replayRequest.onControlResponse(relevantID, code, errorMessage)
	case ps.replayTokenRequest.correlationID:
		ps.replayTokenRequest.onControlResponse(relevantID, code, errorMessage)
	}
}

func (ps *PersistentSubscription) onRecordingDescriptor(descriptor *codecs.RecordingDescriptor) {
	if descriptor.CorrelationId == ps.listRecordingRequest.correlationID {
		ps.listRecordingRequest.onRecordingDescriptor(descriptor)
	}
}

func (ps *PersistentSubscription) onArchiveError(err error) {
	if ps.archive.isClosed() {
		ps.setState(StateFailed)
		ps.onTerminalError(err)
	} else {
		ps.listener.OnError(err)
	}
}
