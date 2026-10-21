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
	"fmt"

	"github.com/lirm/aeron-go/aeron"
	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/logbuffer"
	"github.com/lirm/aeron-go/aeron/logbuffer/term"
	"github.com/lirm/aeron-go/archive/codecs"
)

// RecordingSignalPoller is the Go port of io.aeron.archive.client.RecordingSignalPoller. It completes a poll
// on each ControlResponse or RecordingSignalEvent for its control session and exposes the decoded fields.
// It is not safe for concurrent use.
type RecordingSignalPoller struct {
	subscription     *aeron.Subscription
	fragmentLimit    int
	controlSessionId int64
	assembler        *aeron.ControlledFragmentAssembler
	decoder          responseDecoder
	err              error

	controlResponse codecs.ControlResponse
	signalEvent     codecs.RecordingSignalEvent

	templateId              int32
	correlationId           int64
	relevantId              int64
	version                 int32
	code                    codecs.ControlResponseCodeEnum
	errorMessage            string
	recordingId             int64
	recordingSubscriptionId int64
	recordingPosition       int64
	recordingSignal         codecs.RecordingSignalEnum
	isPollComplete          bool
}

// NewRecordingSignalPoller creates a poller. A fragmentLimit <= 0 uses the default of 10.
func NewRecordingSignalPoller(controlSessionId int64, subscription *aeron.Subscription, fragmentLimit int) *RecordingSignalPoller {
	if fragmentLimit <= 0 {
		fragmentLimit = defaultPollerFragmentLimit
	}
	p := &RecordingSignalPoller{
		subscription:     subscription,
		fragmentLimit:    fragmentLimit,
		controlSessionId: controlSessionId,
		decoder:          newResponseDecoder(false),
	}
	p.assembler = aeron.NewControlledFragmentAssembler(p.onFragment, aeron.DefaultFragmentAssemblyBufferLength)
	p.reset()
	return p
}

func (p *RecordingSignalPoller) reset() {
	p.isPollComplete = false
	p.templateId = aeron.NullValue
	p.correlationId = aeron.NullValue
	p.relevantId = aeron.NullValue
	p.version = 0
	p.code = codecs.ControlResponseCode.NullValue
	p.errorMessage = ""
	p.recordingId = aeron.NullValue
	p.recordingSubscriptionId = aeron.NullValue
	p.recordingPosition = aeron.NullValue
	p.recordingSignal = codecs.RecordingSignal.NullValue
}

// Poll the subscription, resetting state first if the previous poll completed.
func (p *RecordingSignalPoller) Poll() (int, error) {
	if p.isPollComplete {
		p.reset()
	}
	p.err = nil
	n := p.subscription.ControlledPoll(p.assembler.OnFragment, p.fragmentLimit)
	return n, p.err
}

// Subscription returns the subscription being polled.
func (p *RecordingSignalPoller) Subscription() *aeron.Subscription { return p.subscription }

// ControlSessionId this poller filters on.
func (p *RecordingSignalPoller) ControlSessionId() int64 { return p.controlSessionId }

// TemplateId of the last decoded message, or aeron.NullValue.
func (p *RecordingSignalPoller) TemplateId() int32 { return p.templateId }

// CorrelationId of the last decoded message, or aeron.NullValue.
func (p *RecordingSignalPoller) CorrelationId() int64 { return p.correlationId }

// RelevantId of the last control response, or aeron.NullValue.
func (p *RecordingSignalPoller) RelevantId() int64 { return p.relevantId }

// Version of the last control response.
func (p *RecordingSignalPoller) Version() int32 { return p.version }

// Code of the last control response.
func (p *RecordingSignalPoller) Code() codecs.ControlResponseCodeEnum { return p.code }

// ErrorMessage of the last control response.
func (p *RecordingSignalPoller) ErrorMessage() string { return p.errorMessage }

// RecordingId of the last signal, or aeron.NullValue.
func (p *RecordingSignalPoller) RecordingId() int64 { return p.recordingId }

// RecordingSubscriptionId of the last signal, or aeron.NullValue.
func (p *RecordingSignalPoller) RecordingSubscriptionId() int64 { return p.recordingSubscriptionId }

// RecordingPosition of the last signal, or aeron.NullValue.
func (p *RecordingSignalPoller) RecordingPosition() int64 { return p.recordingPosition }

// RecordingSignal of the last signal, or codecs.RecordingSignal.NullValue.
func (p *RecordingSignalPoller) RecordingSignal() codecs.RecordingSignalEnum {
	return p.recordingSignal
}

// IsPollComplete reports whether a message for the control session was decoded.
func (p *RecordingSignalPoller) IsPollComplete() bool { return p.isPollComplete }

func (p *RecordingSignalPoller) String() string {
	return fmt.Sprintf("RecordingSignalPoller{controlSessionId=%d, correlationId=%d, relevantId=%d, code=%d, "+
		"templateId=%d, version=%d, errorMessage=%q, recordingId=%d, recordingSubscriptionId=%d, "+
		"recordingPosition=%d, recordingSignal=%d, isPollComplete=%t}",
		p.controlSessionId, p.correlationId, p.relevantId, p.code, p.templateId, p.version, p.errorMessage,
		p.recordingId, p.recordingSubscriptionId, p.recordingPosition, p.recordingSignal, p.isPollComplete)
}

func (p *RecordingSignalPoller) onFragment(buffer *atomic.Buffer, offset, length int32, _ *logbuffer.Header) term.ControlledPollAction {
	if p.isPollComplete {
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
		r := &p.controlResponse
		if r.ControlSessionId == p.controlSessionId {
			p.templateId = int32(codecIds.controlResponse)
			p.correlationId = r.CorrelationId
			p.relevantId = r.RelevantId
			p.code = r.Code
			p.version = r.Version
			p.errorMessage = string(r.ErrorMessage)
			p.isPollComplete = true
			return term.ControlledPollActionBreak
		}
	case codecIds.recordingSignalEvent:
		if err := p.decoder.decode(&p.signalEvent); err != nil {
			p.err = err
			return term.ControlledPollActionBreak
		}
		s := &p.signalEvent
		if s.ControlSessionId == p.controlSessionId {
			p.templateId = int32(codecIds.recordingSignalEvent)
			p.correlationId = s.CorrelationId
			p.recordingId = s.RecordingId
			p.recordingSubscriptionId = s.SubscriptionId
			p.recordingPosition = s.Position
			p.recordingSignal = s.Signal
			p.isPollComplete = true
			return term.ControlledPollActionBreak
		}
	}
	return term.ControlledPollActionContinue
}
