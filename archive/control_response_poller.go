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

// ControlResponsePoller is the Go port of io.aeron.archive.client.ControlResponsePoller. It polls a control
// response subscription for one ControlResponse, Challenge or RecordingSignalEvent per completed poll and
// exposes the decoded fields. Messages of other templates are skipped. It is not safe for concurrent use.
type ControlResponsePoller struct {
	subscription  *aeron.Subscription
	fragmentLimit int
	assembler     *aeron.ControlledFragmentAssembler
	decoder       responseDecoder
	err           error

	controlResponse codecs.ControlResponse
	challenge       codecs.Challenge
	signalEvent     codecs.RecordingSignalEvent

	controlSessionId int64
	correlationId    int64
	relevantId       int64
	templateId       int32
	version          int32
	code             codecs.ControlResponseCodeEnum
	errorMessage     string
	recordingId      int64
	subscriptionId   int64
	position         int64
	recordingSignal  codecs.RecordingSignalEnum
	encodedChallenge []byte
	isPollComplete   bool
}

// NewControlResponsePoller creates a poller for the subscription. A fragmentLimit <= 0 uses the Java default of 10.
func NewControlResponsePoller(subscription *aeron.Subscription, fragmentLimit int) *ControlResponsePoller {
	if fragmentLimit <= 0 {
		fragmentLimit = defaultPollerFragmentLimit
	}
	p := &ControlResponsePoller{
		subscription:  subscription,
		fragmentLimit: fragmentLimit,
		decoder:       newResponseDecoder(false),
	}
	p.assembler = aeron.NewControlledFragmentAssembler(p.onFragment, aeron.DefaultFragmentAssemblyBufferLength)
	p.reset()
	return p
}

func (p *ControlResponsePoller) reset() {
	p.controlSessionId = aeron.NullValue
	p.correlationId = aeron.NullValue
	p.relevantId = aeron.NullValue
	p.templateId = aeron.NullValue
	p.version = 0
	p.code = codecs.ControlResponseCode.NullValue
	p.errorMessage = ""
	p.recordingId = aeron.NullValue
	p.subscriptionId = aeron.NullValue
	p.position = aeron.NullValue
	p.recordingSignal = codecs.RecordingSignal.NullValue
	p.encodedChallenge = nil
	p.isPollComplete = false
}

// Subscription returns the subscription being polled.
func (p *ControlResponsePoller) Subscription() *aeron.Subscription { return p.subscription }

// Poll the subscription. If the previous poll completed, the decoded state is reset first. The error is
// non-nil (an *ArchiveException) only if a message could not be decoded, e.g. a schema id mismatch.
func (p *ControlResponsePoller) Poll() (int, error) {
	if p.isPollComplete {
		p.reset()
	}
	p.err = nil
	n := p.subscription.ControlledPoll(p.assembler.OnFragment, p.fragmentLimit)
	return n, p.err
}

// ControlSessionId of the last polled message, or aeron.NullValue.
func (p *ControlResponsePoller) ControlSessionId() int64 { return p.controlSessionId }

// CorrelationId of the last polled message, or aeron.NullValue.
func (p *ControlResponsePoller) CorrelationId() int64 { return p.correlationId }

// RelevantId of the last control response, or aeron.NullValue.
func (p *ControlResponsePoller) RelevantId() int64 { return p.relevantId }

// TemplateId of the last decoded message, or aeron.NullValue.
func (p *ControlResponsePoller) TemplateId() int32 { return p.templateId }

// Version of the last control response or challenge.
func (p *ControlResponsePoller) Version() int32 { return p.version }

// Code of the last control response, or codecs.ControlResponseCode.NullValue.
func (p *ControlResponsePoller) Code() codecs.ControlResponseCodeEnum { return p.code }

// ErrorMessage of the last control response.
func (p *ControlResponsePoller) ErrorMessage() string { return p.errorMessage }

// RecordingId of the last recording signal, or aeron.NullValue.
func (p *ControlResponsePoller) RecordingId() int64 { return p.recordingId }

// SubscriptionId of the last recording signal, or aeron.NullValue.
func (p *ControlResponsePoller) SubscriptionId() int64 { return p.subscriptionId }

// Position of the last recording signal, or aeron.NullValue.
func (p *ControlResponsePoller) Position() int64 { return p.position }

// RecordingSignal of the last recording signal, or codecs.RecordingSignal.NullValue.
func (p *ControlResponsePoller) RecordingSignal() codecs.RecordingSignalEnum {
	return p.recordingSignal
}

// EncodedChallenge of the last challenge, or nil.
func (p *ControlResponsePoller) EncodedChallenge() []byte { return p.encodedChallenge }

// WasChallenged reports whether the last polled message was a Challenge.
func (p *ControlResponsePoller) WasChallenged() bool { return p.encodedChallenge != nil }

// IsPollComplete reports whether a ControlResponse, Challenge or RecordingSignalEvent has been decoded.
func (p *ControlResponsePoller) IsPollComplete() bool { return p.isPollComplete }

// IsControlResponse reports whether the poll completed with a ControlResponse.
func (p *ControlResponsePoller) IsControlResponse() bool {
	return p.isPollComplete && p.templateId == int32(codecIds.controlResponse)
}

// IsRecordingSignal reports whether the poll completed with a RecordingSignalEvent.
func (p *ControlResponsePoller) IsRecordingSignal() bool {
	return p.isPollComplete && p.templateId == int32(codecIds.recordingSignalEvent)
}

func (p *ControlResponsePoller) String() string {
	return fmt.Sprintf("ControlResponsePoller{controlSessionId=%d, correlationId=%d, relevantId=%d, code=%d, "+
		"templateId=%d, version=%d, errorMessage=%q, recordingId=%d, subscriptionId=%d, position=%d, "+
		"recordingSignal=%d, isPollComplete=%t}",
		p.controlSessionId, p.correlationId, p.relevantId, p.code, p.templateId, p.version, p.errorMessage,
		p.recordingId, p.subscriptionId, p.position, p.recordingSignal, p.isPollComplete)
}

func (p *ControlResponsePoller) onFragment(buffer *atomic.Buffer, offset, length int32, _ *logbuffer.Header) term.ControlledPollAction {
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
		p.templateId = int32(codecIds.controlResponse)
		p.controlSessionId = p.controlResponse.ControlSessionId
		p.correlationId = p.controlResponse.CorrelationId
		p.relevantId = p.controlResponse.RelevantId
		p.code = p.controlResponse.Code
		p.version = p.controlResponse.Version
		p.errorMessage = string(p.controlResponse.ErrorMessage)
		p.isPollComplete = true
		return term.ControlledPollActionBreak

	case codecIds.challenge:
		if err := p.decoder.decode(&p.challenge); err != nil {
			p.err = err
			return term.ControlledPollActionBreak
		}
		p.templateId = int32(codecIds.challenge)
		p.controlSessionId = p.challenge.ControlSessionId
		p.correlationId = p.challenge.CorrelationId
		p.relevantId = aeron.NullValue
		p.code = codecs.ControlResponseCode.NullValue
		p.version = p.challenge.Version
		p.errorMessage = ""
		p.encodedChallenge = append(make([]byte, 0, len(p.challenge.EncodedChallenge)), p.challenge.EncodedChallenge...)
		p.isPollComplete = true
		return term.ControlledPollActionBreak

	case codecIds.recordingSignalEvent:
		if err := p.decoder.decode(&p.signalEvent); err != nil {
			p.err = err
			return term.ControlledPollActionBreak
		}
		p.templateId = int32(codecIds.recordingSignalEvent)
		p.controlSessionId = p.signalEvent.ControlSessionId
		p.correlationId = p.signalEvent.CorrelationId
		p.recordingId = p.signalEvent.RecordingId
		p.subscriptionId = p.signalEvent.SubscriptionId
		p.position = p.signalEvent.Position
		p.recordingSignal = p.signalEvent.Signal
		p.isPollComplete = true
		return term.ControlledPollActionBreak
	}
	return term.ControlledPollActionContinue
}
