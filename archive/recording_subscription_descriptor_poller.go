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

// RecordingSubscriptionDescriptorPoller is the Go port of
// io.aeron.archive.client.RecordingSubscriptionDescriptorPoller. It dispatches the subscription descriptors
// of one ListRecordingSubscriptions request to a RecordingSubscriptionDescriptorConsumer. It is not safe for
// concurrent use.
type RecordingSubscriptionDescriptorPoller struct {
	subscription            *aeron.Subscription
	fragmentLimit           int
	controlSessionId        int64
	errorHandler            func(error)
	recordingSignalConsumer RecordingSignalConsumer
	assembler               *aeron.ControlledFragmentAssembler
	decoder                 responseDecoder
	err                     error

	correlationId              int64
	remainingSubscriptionCount int32
	isDispatchComplete         bool
	consumer                   RecordingSubscriptionDescriptorConsumer

	controlResponse codecs.ControlResponse
	descriptor      codecs.RecordingSubscriptionDescriptor
	signalEvent     codecs.RecordingSignalEvent
}

// NewRecordingSubscriptionDescriptorPoller creates a poller; errorHandler and recordingSignalConsumer may be nil.
// A fragmentLimit <= 0 uses the default of 10.
func NewRecordingSubscriptionDescriptorPoller(
	subscription *aeron.Subscription,
	errorHandler func(error),
	recordingSignalConsumer RecordingSignalConsumer,
	controlSessionId int64,
	fragmentLimit int,
) *RecordingSubscriptionDescriptorPoller {
	if fragmentLimit <= 0 {
		fragmentLimit = defaultPollerFragmentLimit
	}
	p := &RecordingSubscriptionDescriptorPoller{
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

// Reset prepares the poller for a new request expecting up to subscriptionCount descriptors.
func (p *RecordingSubscriptionDescriptorPoller) Reset(correlationId int64, subscriptionCount int32,
	consumer RecordingSubscriptionDescriptorConsumer) {
	p.correlationId = correlationId
	p.remainingSubscriptionCount = subscriptionCount
	p.consumer = consumer
	p.isDispatchComplete = false
}

// Poll the subscription. A matching ERROR response is returned as an *ArchiveException.
func (p *RecordingSubscriptionDescriptorPoller) Poll() (int, error) {
	p.isDispatchComplete = false
	p.err = nil
	n := p.subscription.ControlledPoll(p.assembler.OnFragment, p.fragmentLimit)
	return n, p.err
}

// Subscription returns the subscription being polled.
func (p *RecordingSubscriptionDescriptorPoller) Subscription() *aeron.Subscription {
	return p.subscription
}

// ControlSessionId this poller filters on.
func (p *RecordingSubscriptionDescriptorPoller) ControlSessionId() int64 { return p.controlSessionId }

// CorrelationId of the current request.
func (p *RecordingSubscriptionDescriptorPoller) CorrelationId() int64 { return p.correlationId }

// IsDispatchComplete reports whether the requested count was reached or the archive reported no more subscriptions.
func (p *RecordingSubscriptionDescriptorPoller) IsDispatchComplete() bool {
	return p.isDispatchComplete
}

// RemainingSubscriptionCount is the number of descriptors still expected.
func (p *RecordingSubscriptionDescriptorPoller) RemainingSubscriptionCount() int32 {
	return p.remainingSubscriptionCount
}

func (p *RecordingSubscriptionDescriptorPoller) onFragment(buffer *atomic.Buffer, offset, length int32, _ *logbuffer.Header) term.ControlledPollAction {
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
			codecs.ControlResponseCode.SUBSCRIPTION_UNKNOWN, p.errorHandler, &p.err); done {
			p.isDispatchComplete = p.err == nil
			return action
		}
	case codecIds.recordingSubscriptionDescriptor:
		if err := p.decoder.decode(&p.descriptor); err != nil {
			p.err = err
			return term.ControlledPollActionBreak
		}
		d := &p.descriptor
		if d.ControlSessionId == p.controlSessionId && d.CorrelationId == p.correlationId {
			if p.consumer != nil {
				p.consumer.OnSubscriptionDescriptor(d.ControlSessionId, d.CorrelationId, d.SubscriptionId,
					d.StreamId, string(d.StrippedChannel))
			}
			p.remainingSubscriptionCount--
			if p.remainingSubscriptionCount <= 0 {
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
