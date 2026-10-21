// Copyright (C) 2026 Kalshi Trading
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
	"github.com/lirm/aeron-go/aeron"
	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/logbuffer"
	"github.com/lirm/aeron-go/aeron/logbuffer/term"
	"github.com/lirm/aeron-go/archive/codecs"
)

// RecordingDescriptorPoller is the Go port of io.aeron.archive.client.RecordingDescriptorPoller. It
// dispatches the recording descriptors of one list request (controlSessionId + correlationId) to a
// RecordingDescriptorConsumer. Other sessions' messages and unrelated templates are skipped. It is not safe
// for concurrent use.
type RecordingDescriptorPoller struct {
	subscription            *aeron.Subscription
	fragmentLimit           int
	controlSessionId        int64
	errorHandler            func(error)
	recordingSignalConsumer RecordingSignalConsumer
	assembler               *aeron.ControlledFragmentAssembler
	decoder                 responseDecoder
	err                     error

	correlationId        int64
	remainingRecordCount int32
	isDispatchComplete   bool
	consumer             RecordingDescriptorConsumer

	controlResponse codecs.ControlResponse
	descriptor      codecs.RecordingDescriptor
	signalEvent     codecs.RecordingSignalEvent
}

// NewRecordingDescriptorPoller creates a poller. errorHandler receives errors for other requests on the
// same control session and recordingSignalConsumer receives the session's signals; both may be nil.
// A fragmentLimit <= 0 uses the default of 10.
func NewRecordingDescriptorPoller(
	subscription *aeron.Subscription,
	errorHandler func(error),
	recordingSignalConsumer RecordingSignalConsumer,
	controlSessionId int64,
	fragmentLimit int,
) *RecordingDescriptorPoller {
	if fragmentLimit <= 0 {
		fragmentLimit = defaultPollerFragmentLimit
	}
	p := &RecordingDescriptorPoller{
		subscription:            subscription,
		fragmentLimit:           fragmentLimit,
		controlSessionId:        controlSessionId,
		errorHandler:            errorHandler,
		recordingSignalConsumer: recordingSignalConsumer,
		decoder:                 newResponseDecoder(false),
		correlationId:           aeron.NullValue,
	}
	p.assembler = aeron.NewControlledFragmentAssembler(p.onFragment, aeron.DefaultFragmentAssemblyBufferLength)
	return p
}

// Reset prepares the poller for a new list request expecting up to recordCount descriptors.
func (p *RecordingDescriptorPoller) Reset(correlationId int64, recordCount int32, consumer RecordingDescriptorConsumer) {
	p.correlationId = correlationId
	p.remainingRecordCount = recordCount
	p.consumer = consumer
	p.isDispatchComplete = false
}

// Poll the subscription. A matching ERROR response is returned as an *ArchiveException.
func (p *RecordingDescriptorPoller) Poll() (int, error) {
	p.isDispatchComplete = false
	p.err = nil
	n := p.subscription.ControlledPoll(p.assembler.OnFragment, p.fragmentLimit)
	return n, p.err
}

// Subscription returns the subscription being polled.
func (p *RecordingDescriptorPoller) Subscription() *aeron.Subscription { return p.subscription }

// ControlSessionId this poller filters on.
func (p *RecordingDescriptorPoller) ControlSessionId() int64 { return p.controlSessionId }

// CorrelationId of the current request.
func (p *RecordingDescriptorPoller) CorrelationId() int64 { return p.correlationId }

// IsDispatchComplete reports whether the requested count was reached or the archive reported no more recordings.
func (p *RecordingDescriptorPoller) IsDispatchComplete() bool { return p.isDispatchComplete }

// RemainingRecordCount is the number of descriptors still expected.
func (p *RecordingDescriptorPoller) RemainingRecordCount() int32 { return p.remainingRecordCount }

func (p *RecordingDescriptorPoller) onFragment(buffer *atomic.Buffer, offset, length int32, _ *logbuffer.Header) term.ControlledPollAction {
	if p.isDispatchComplete {
		return term.ControlledPollActionAbort
	}
	if err := p.decoder.wrap(buffer, offset, length); err != nil {
		p.err = err
		return term.ControlledPollActionBreak
	}
	switch p.decoder.templateId() {
	case codecIds.controlResponse:
		if err := p.decoder.decode(&p.controlResponse); err != nil {
			p.err = err
			return term.ControlledPollActionBreak
		}
		if action, done := handleDescriptorListResponse(&p.controlResponse, p.controlSessionId, p.correlationId,
			codecs.ControlResponseCode.RECORDING_UNKNOWN, p.errorHandler, &p.err); done {
			p.isDispatchComplete = p.err == nil
			return action
		}
	case codecIds.recordingDescriptor:
		if err := p.decoder.decode(&p.descriptor); err != nil {
			p.err = err
			return term.ControlledPollActionBreak
		}
		if p.descriptor.ControlSessionId == p.controlSessionId && p.descriptor.CorrelationId == p.correlationId {
			if p.consumer != nil {
				DispatchDescriptor(&p.descriptor, p.consumer)
			}
			p.remainingRecordCount--
			if p.remainingRecordCount <= 0 {
				p.isDispatchComplete = true
				return term.ControlledPollActionBreak
			}
		}
	case codecIds.recordingSignalEvent:
		if err := p.decoder.decode(&p.signalEvent); err != nil {
			p.err = err
			return term.ControlledPollActionBreak
		}
		if p.signalEvent.ControlSessionId == p.controlSessionId && p.recordingSignalConsumer != nil {
			dispatchSignal(&p.signalEvent, p.recordingSignalConsumer)
		}
	}
	return term.ControlledPollActionContinue
}

// handleDescriptorListResponse applies the Java descriptor poller rules to a control response:
// the terminating unknown code for our request completes the dispatch, an ERROR for our request is
// returned via errOut, and an ERROR for another request on the session goes to errorHandler.
func handleDescriptorListResponse(
	r *codecs.ControlResponse,
	controlSessionId, correlationId int64,
	terminatingCode codecs.ControlResponseCodeEnum,
	errorHandler func(error),
	errOut *error,
) (term.ControlledPollAction, bool) {
	if r.ControlSessionId != controlSessionId {
		return term.ControlledPollActionContinue, false
	}
	if r.Code == terminatingCode && r.CorrelationId == correlationId {
		return term.ControlledPollActionBreak, true
	}
	if r.Code == codecs.ControlResponseCode.ERROR {
		ex := newArchiveResponseException(string(r.ErrorMessage), r.RelevantId, r.CorrelationId)
		if r.CorrelationId == correlationId {
			*errOut = ex
			return term.ControlledPollActionBreak, true
		}
		if errorHandler != nil {
			errorHandler(ex)
		}
	}
	return term.ControlledPollActionContinue, false
}
