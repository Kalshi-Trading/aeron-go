// Copyright (C) 2021-2022 Talos, Inc.
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

package archive

import (
	"fmt"
	"time"

	"github.com/lirm/aeron-go/aeron"
	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/logbuffer"
	"github.com/lirm/aeron-go/aeron/logbuffer/term"
	"github.com/lirm/aeron-go/archive/codecs"
)

const controlFragmentLimit = 10

// Control contains everything required for the archive subscription/response side.
//
// All decoding state is owned by the Control, so separate Archive instances can be polled from separate
// goroutines. A single Control must not be polled concurrently.
type Control struct {
	Subscription *aeron.Subscription

	// Polling results
	Results ControlResults

	archive *Archive // link to parent

	// A single assembler is kept for the lifetime of the Control so that a message whose fragments span
	// two polls (or two different synchronous operations) is still reassembled correctly.
	assembler     *aeron.ControlledFragmentAssembler
	activeHandler term.ControlledFragmentHandler
	decoder       responseDecoder

	// Context of the synchronous operation in progress
	correlationID     int64
	sessionID         int64
	descriptorsWanted int

	// Scratch decoders for messages that are not retained
	controlResponse codecs.ControlResponse
	challenge       codecs.Challenge
	signalEvent     codecs.RecordingSignalEvent
}

// ControlResults for holding state over a Control request/response
// The polling mechanism is not parameterized so we need to set state for the results as we go
// These pieces are filled out by various ResponsePollers which will set IsPollComplete to true
type ControlResults struct {
	CorrelationId                    int64
	ControlResponse                  *codecs.ControlResponse
	RecordingDescriptors             []*codecs.RecordingDescriptor
	RecordingSubscriptionDescriptors []*codecs.RecordingSubscriptionDescriptor
	IsPollComplete                   bool
	FragmentsReceived                int
	ErrorResponse                    error // An *ArchiveException when the archive returned an ERROR response
}

// PollContext contains the information we'll need in the image Poll()
// callback to match against our request or for async events to invoke
// the appropriate listener
type PollContext struct {
	control       *Control
	correlationID int64
}

var ErrNotConnected = fmt.Errorf("not connected")

// CodecIds stops us allocating every object when we need only one
// Arguably SBE should give us a static value
type CodecIds struct {
	controlResponse                 uint16
	challenge                       uint16
	recordingDescriptor             uint16
	recordingSubscriptionDescriptor uint16
	recordingSignalEvent            uint16
	recordingStarted                uint16
	recordingProgress               uint16
	recordingStopped                uint16
	ping                            uint16
}

// codecIds is written once by init() and only read afterwards.
var codecIds CodecIds

func init() {
	codecIds.controlResponse = new(codecs.ControlResponse).SbeTemplateId()
	codecIds.challenge = new(codecs.Challenge).SbeTemplateId()
	codecIds.recordingDescriptor = new(codecs.RecordingDescriptor).SbeTemplateId()
	codecIds.recordingSubscriptionDescriptor = new(codecs.RecordingSubscriptionDescriptor).SbeTemplateId()
	codecIds.recordingSignalEvent = new(codecs.RecordingSignalEvent).SbeTemplateId()
	codecIds.recordingStarted = new(codecs.RecordingStarted).SbeTemplateId()
	codecIds.recordingProgress = new(codecs.RecordingProgress).SbeTemplateId()
	codecIds.recordingStopped = new(codecs.RecordingStopped).SbeTemplateId()
	codecIds.ping = new(codecs.Ping).SbeTemplateId()
}

func (control *Control) rangeChecking() bool {
	return control.archive != nil && control.archive.Options != nil && control.archive.Options.RangeChecking
}

func (control *Control) onError(err error) {
	if control.archive != nil && control.archive.Listeners != nil && control.archive.Listeners.ErrorListener != nil {
		control.archive.Listeners.ErrorListener(err)
	}
}

// onSignal hands a copy of the (reused) scratch event to the shared dispatchRecordingSignal.
func (control *Control) onSignal(s *codecs.RecordingSignalEvent) {
	if control.archive == nil || control.archive.Listeners == nil {
		return
	}
	event := *s
	dispatchRecordingSignal(control.archive, &event)
}

func (control *Control) archiveSessionID() int64 {
	if control.archive == nil {
		return aeron.NullValue
	}
	return control.archive.SessionID
}

// wrap decodes the message header, reporting failures to the ErrorListener.
func (control *Control) wrap(buffer *atomic.Buffer, offset, length int32) bool {
	if err := control.decoder.wrap(buffer, offset, length); err != nil {
		control.onError(err)
		return false
	}
	return true
}

// decode decodes the message body, reporting failures to the ErrorListener.
func (control *Control) decode(msg sbeDecoder) bool {
	if err := control.decoder.decode(msg); err != nil {
		control.onError(err)
		return false
	}
	return true
}

func copyControlResponse(r *codecs.ControlResponse) *codecs.ControlResponse {
	c := *r
	c.ErrorMessage = append([]uint8(nil), r.ErrorMessage...)
	return &c
}

func (control *Control) completeWith(r *codecs.ControlResponse, err error) term.ControlledPollAction {
	control.Results.ControlResponse = copyControlResponse(r)
	control.Results.CorrelationId = r.CorrelationId
	control.Results.ErrorResponse = err
	control.Results.IsPollComplete = true
	return term.ControlledPollActionBreak
}

// poll provides the control response poller using local state to pass
// back data from the underlying subscription
func (control *Control) poll(handler term.ControlledFragmentHandler, fragmentLimit int) int {
	if control.assembler == nil {
		control.assembler = aeron.NewControlledFragmentAssembler(control.dispatch, aeron.DefaultFragmentAssemblyBufferLength)
	}
	if control.decoder.marshaller == nil {
		control.decoder = newResponseDecoder(false)
	}
	control.decoder.rangeChecking = control.rangeChecking()

	control.Results.ControlResponse = nil  // Clear old results
	control.Results.IsPollComplete = false // Clear completion flag
	control.activeHandler = handler

	return control.Subscription.ControlledPoll(control.assembler.OnFragment, fragmentLimit)
}

func (control *Control) dispatch(buffer *atomic.Buffer, offset int32, length int32, header *logbuffer.Header) term.ControlledPollAction {
	return control.activeHandler(buffer, offset, length, header)
}

// PollForErrorResponse polls the response stream for errors or async events.
//
// It will continue until it either receives an error or the queue is empty.
//
// If any control messages are present then they will be discarded so this
// call should not be used unless there are no outstanding operations.
//
// This may be used to check for errors, to dispatch async events, and
// to catch up on messages not for this session if for example the
// same channel and stream are in use by other sessions.
//
// Returns an error if we detect an archive operation in progress
// and a count of how many messages were consumed
func (control *Control) PollForErrorResponse() (int, error) {
	received := 0

	control.Results.ErrorResponse = nil
	if !control.Subscription.IsConnected() {
		if control.archive != nil {
			control.archive.setState(ArchiveStateDisconnected)
		}
		return received, ErrNotConnected
	}

	// Poll for async events, errors etc until the queue is drained
	for {
		ret := control.poll(control.errorResponseFragmentHandler, controlFragmentLimit)
		received += ret

		if control.Results.ErrorResponse != nil {
			return received, control.Results.ErrorResponse
		}
		if ret == 0 {
			return received, nil
		}
	}
}

// errorResponseFragmentHandler is used to check for errors and async events on an idle control
// session. Messages for other sessions are ignored, recording signals are dispatched and an ERROR
// response (or a message from an in-flight synchronous operation) on our session stops the poll.
func (control *Control) errorResponseFragmentHandler(buffer *atomic.Buffer, offset int32, length int32, header *logbuffer.Header) term.ControlledPollAction {
	if control.Results.ErrorResponse != nil {
		return term.ControlledPollActionAbort
	}
	if !control.wrap(buffer, offset, length) {
		return term.ControlledPollActionContinue
	}
	sessionID := control.archiveSessionID()

	switch control.decoder.templateId() {
	case codecIds.controlResponse:
		r := &control.controlResponse
		if !control.decode(r) || r.ControlSessionId != sessionID {
			return term.ControlledPollActionContinue
		}
		if r.Code == codecs.ControlResponseCode.ERROR {
			control.Results.ErrorResponse = newArchiveResponseException(string(r.ErrorMessage), r.RelevantId, r.CorrelationId)
			return term.ControlledPollActionBreak
		}

	case codecIds.challenge:
		c := &control.challenge
		if control.decode(c) && c.ControlSessionId == sessionID {
			control.Results.ErrorResponse = fmt.Errorf("received and ignoring challenge (correlationID:%d), PollForErrorResponse should not be called in parallel with sync operations", c.CorrelationId)
			return term.ControlledPollActionBreak
		}

	case codecIds.recordingDescriptor:
		var rd codecs.RecordingDescriptor
		if control.decode(&rd) && rd.ControlSessionId == sessionID {
			control.Results.ErrorResponse = fmt.Errorf("received and ignoring recordingDescriptor (correlationID:%d), PollForErrorResponse should not be called in parallel with sync operations", rd.CorrelationId)
			return term.ControlledPollActionBreak
		}

	case codecIds.recordingSubscriptionDescriptor:
		var rsd codecs.RecordingSubscriptionDescriptor
		if control.decode(&rsd) && rsd.ControlSessionId == sessionID {
			control.Results.ErrorResponse = fmt.Errorf("received and ignoring recordingSubscriptionDescriptor (correlationID:%d), PollForErrorResponse should not be called in parallel with sync operations", rsd.CorrelationId)
			return term.ControlledPollActionBreak
		}

	case codecIds.recordingSignalEvent:
		s := &control.signalEvent
		if control.decode(s) && s.ControlSessionId == sessionID {
			control.onSignal(s)
		}

	case codecIds.ping:
		// Liveness message from the archive's control session, nothing to do.
	}
	return term.ControlledPollActionContinue
}

// Poll for control response events. Returns number of fragments read during the operation.
// Zero if no events are available.
//
// A poll completes on the first ControlResponse (for any session); its result is in Results and
// Results.ErrorResponse is set if the response code was ERROR. Recording signals for this session are
// dispatched to the RecordingSignalListener.
func (control *Control) Poll() (workCount int) {
	if control.Results.IsPollComplete {
		control.Results = ControlResults{}
	}
	return control.poll(control.onFragment, controlFragmentLimit)
}

func (control *Control) onFragment(buffer *atomic.Buffer, offset int32, length int32, header *logbuffer.Header) term.ControlledPollAction {
	if control.Results.IsPollComplete {
		return term.ControlledPollActionAbort
	}
	if !control.wrap(buffer, offset, length) {
		return term.ControlledPollActionContinue
	}

	switch control.decoder.templateId() {
	case codecIds.controlResponse:
		r := &control.controlResponse
		if !control.decode(r) {
			return term.ControlledPollActionContinue
		}
		var err error
		if r.Code == codecs.ControlResponseCode.ERROR {
			err = newArchiveResponseException(string(r.ErrorMessage), r.RelevantId, r.CorrelationId)
		}
		return control.completeWith(r, err)

	case codecIds.recordingSignalEvent:
		s := &control.signalEvent
		if control.decode(s) && s.ControlSessionId == control.archiveSessionID() {
			control.onSignal(s)
		}

	case codecIds.ping:
		// Liveness message from the archive's control session, nothing to do.
	}
	return term.ControlledPollActionContinue
}

// PollForResponse polls for the response to correlationID on control session sessionID.
//
// Messages for other sessions are skipped, recording signals for the session are dispatched, and ERROR
// responses for other requests on the session are passed to the ErrorListener. Returns (relevantId, nil)
// on OK and (relevantId, *ArchiveException) on any other response code. The response itself is left in
// Control.Results.ControlResponse.
func (control *Control) PollForResponse(correlationID int64, sessionID int64) (int64, error) {
	logger.Debugf("PollForResponse(%d:%d)", correlationID, sessionID)

	control.correlationID = correlationID
	control.sessionID = sessionID
	control.Results.ErrorResponse = nil

	start := time.Now()
	for {
		ret := control.poll(control.responseFragmentHandler, controlFragmentLimit)

		if control.Results.IsPollComplete {
			r := control.Results.ControlResponse
			switch r.Code {
			case codecs.ControlResponseCode.OK:
				return r.RelevantId, nil
			case codecs.ControlResponseCode.ERROR:
				logger.Debug(control.Results.ErrorResponse)
				return r.RelevantId, control.Results.ErrorResponse
			default:
				err := NewArchiveException(
					fmt.Sprintf("unexpected response code %d for correlationId=%d: %s", r.Code, correlationID, r.ErrorMessage),
					ErrorCodeGeneric, correlationID)
				control.Results.ErrorResponse = err
				return r.RelevantId, err
			}
		}

		if ret > 0 {
			continue
		}
		if err := control.checkForDisconnect(); err != nil {
			return 0, err
		}
		if time.Since(start) > control.archive.Options.Timeout {
			return 0, control.timeoutError("awaiting response", correlationID)
		}
		control.archive.Options.IdleStrategy.Idle(0)
	}
}

// checkForDisconnect mirrors Java AeronArchive.checkForDisconnect: once the control response subscription has
// no connected image the archive is marked DISCONNECTED and an *ArchiveException is returned.
func (control *Control) checkForDisconnect() error {
	sub := control.Subscription
	if sub.IsConnected() {
		return nil
	}
	if control.archive != nil {
		control.archive.setState(ArchiveStateDisconnected)
	}
	return NewArchiveException(
		fmt.Sprintf("response channel from archive is not connected, channel=%s, streamId=%d, imageCount=%d",
			sub.Channel(), sub.StreamID(), sub.ImageCount()),
		ErrorCodeGeneric, aeron.NullValue)
}

// timeoutError mirrors the Java TimeoutException raised by AeronArchive.checkDeadline.
func (control *Control) timeoutError(what string, correlationID int64) error {
	return aeron.NewTimeoutError(aeron.CategoryError, fmt.Sprintf("%s - correlationId=%d messageTimeout=%s",
		what, correlationID, control.archive.Options.Timeout))
}

func (control *Control) responseFragmentHandler(buffer *atomic.Buffer, offset int32, length int32, header *logbuffer.Header) term.ControlledPollAction {
	if control.Results.IsPollComplete {
		return term.ControlledPollActionAbort
	}
	if !control.wrap(buffer, offset, length) {
		return term.ControlledPollActionContinue
	}

	switch control.decoder.templateId() {
	case codecIds.controlResponse:
		r := &control.controlResponse
		if !control.decode(r) || r.ControlSessionId != control.sessionID {
			return term.ControlledPollActionContinue
		}
		if r.Code == codecs.ControlResponseCode.ERROR {
			ex := newArchiveResponseException(string(r.ErrorMessage), r.RelevantId, r.CorrelationId)
			if r.CorrelationId == control.correlationID {
				return control.completeWith(r, ex)
			}
			control.onError(ex)
			return term.ControlledPollActionContinue
		}
		if r.CorrelationId == control.correlationID {
			return control.completeWith(r, nil)
		}
		logger.Debugf("PollForResponse: ignoring response for correlationID=%d", r.CorrelationId)

	case codecIds.challenge:
		// Java's ControlResponsePoller completes on a Challenge with code NULL_VAL, so pollForResponse fails
		// with "unexpected response code" instead of waiting for a response that will never arrive.
		c := &control.challenge
		if control.decode(c) && c.ControlSessionId == control.sessionID && c.CorrelationId == control.correlationID {
			return control.completeWith(&codecs.ControlResponse{
				ControlSessionId: c.ControlSessionId,
				CorrelationId:    c.CorrelationId,
				RelevantId:       aeron.NullValue,
				Code:             codecs.ControlResponseCode.NullValue,
			}, nil)
		}

	case codecIds.recordingSignalEvent:
		s := &control.signalEvent
		if control.decode(s) && s.ControlSessionId == control.sessionID {
			control.onSignal(s)
		}

	case codecIds.ping:
		// Liveness message from the archive's control session, nothing to do.

	default:
		// Descriptors from abandoned list requests, other challenges and unknown templates are skipped.
		logger.Debugf("PollForResponse: ignoring templateId %d", control.decoder.templateId())
	}
	return term.ControlledPollActionContinue
}

func (control *Control) descriptorFragmentHandler(buffer *atomic.Buffer, offset int32, length int32, header *logbuffer.Header) term.ControlledPollAction {
	if control.Results.IsPollComplete || control.Results.FragmentsReceived >= control.descriptorsWanted {
		return term.ControlledPollActionAbort
	}
	if !control.wrap(buffer, offset, length) {
		return term.ControlledPollActionContinue
	}

	switch control.decoder.templateId() {
	case codecIds.recordingDescriptor:
		rd := new(codecs.RecordingDescriptor)
		if !control.decode(rd) || rd.ControlSessionId != control.sessionID || rd.CorrelationId != control.correlationID {
			return term.ControlledPollActionContinue
		}
		control.Results.RecordingDescriptors = append(control.Results.RecordingDescriptors, rd)
		control.Results.FragmentsReceived++
		if control.Results.FragmentsReceived >= control.descriptorsWanted {
			return term.ControlledPollActionBreak
		}

	case codecIds.recordingSubscriptionDescriptor:
		rsd := new(codecs.RecordingSubscriptionDescriptor)
		if !control.decode(rsd) || rsd.ControlSessionId != control.sessionID || rsd.CorrelationId != control.correlationID {
			return term.ControlledPollActionContinue
		}
		control.Results.RecordingSubscriptionDescriptors = append(control.Results.RecordingSubscriptionDescriptors, rsd)
		control.Results.FragmentsReceived++
		if control.Results.FragmentsReceived >= control.descriptorsWanted {
			return term.ControlledPollActionBreak
		}

	case codecIds.controlResponse:
		r := &control.controlResponse
		if !control.decode(r) || r.ControlSessionId != control.sessionID {
			return term.ControlledPollActionContinue
		}
		switch {
		case r.CorrelationId == control.correlationID &&
			(r.Code == codecs.ControlResponseCode.RECORDING_UNKNOWN || r.Code == codecs.ControlResponseCode.SUBSCRIPTION_UNKNOWN):
			return control.completeWith(r, nil)
		case r.Code == codecs.ControlResponseCode.ERROR:
			ex := newArchiveResponseException(string(r.ErrorMessage), r.RelevantId, r.CorrelationId)
			if r.CorrelationId == control.correlationID {
				return control.completeWith(r, ex)
			}
			control.onError(ex)
		}

	case codecIds.recordingSignalEvent:
		s := &control.signalEvent
		if control.decode(s) && s.ControlSessionId == control.sessionID {
			control.onSignal(s)
		}

	case codecIds.ping:
		// Liveness message from the archive's control session, nothing to do.
	}
	return term.ControlledPollActionContinue
}

// PollForDescriptors polls for up to fragmentsWanted recording (or subscription) descriptors for
// correlationID on control session sessionID, collecting them in Control.Results.
//
// It completes when fragmentsWanted descriptors have been received or the archive terminates the list with
// RECORDING_UNKNOWN/SUBSCRIPTION_UNKNOWN (left in Results.ControlResponse). An ERROR response for the
// request is returned as an *ArchiveException. The timeout restarts whenever a descriptor arrives.
func (control *Control) PollForDescriptors(correlationID int64, sessionID int64, fragmentsWanted int32) error {
	control.Results.ControlResponse = nil
	control.Results.IsPollComplete = false
	control.Results.RecordingDescriptors = nil
	control.Results.RecordingSubscriptionDescriptors = nil
	control.Results.FragmentsReceived = 0
	control.Results.ErrorResponse = nil

	control.correlationID = correlationID
	control.sessionID = sessionID
	control.descriptorsWanted = int(fragmentsWanted)

	start := time.Now()
	for control.Results.FragmentsReceived < control.descriptorsWanted {
		received := control.Results.FragmentsReceived
		fragments := control.poll(control.descriptorFragmentHandler, controlFragmentLimit)

		if control.Results.ErrorResponse != nil {
			return control.Results.ErrorResponse
		}
		if control.Results.IsPollComplete {
			return nil
		}
		if control.Results.FragmentsReceived != received {
			start = time.Now()
		}

		if fragments > 0 {
			continue
		}
		if err := control.checkForDisconnect(); err != nil {
			return err
		}
		if time.Since(start) > control.archive.Options.Timeout {
			return control.timeoutError("awaiting recording descriptors", correlationID)
		}
		control.archive.Options.IdleStrategy.Idle(0)
	}
	return nil
}
