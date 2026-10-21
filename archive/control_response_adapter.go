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
	"github.com/lirm/aeron-go/archive/codecs"
)

// ControlResponseAdapter is the Go port of io.aeron.archive.client.ControlResponseAdapter. It dispatches
// every control response, recording descriptor and recording signal on the subscription to listeners
// without filtering, in the style of the cluster EgressAdapter. It is not safe for concurrent use.
type ControlResponseAdapter struct {
	subscription            *aeron.Subscription
	fragmentLimit           int
	listener                ControlResponseListener
	recordingSignalConsumer RecordingSignalConsumer
	assembler               *aeron.FragmentAssembler
	decoder                 responseDecoder
	err                     error

	controlResponse codecs.ControlResponse
	signalEvent     codecs.RecordingSignalEvent
}

// NewControlResponseAdapter creates an adapter. recordingSignalConsumer may be nil to ignore signals.
// A fragmentLimit <= 0 uses the default of 10.
func NewControlResponseAdapter(
	listener ControlResponseListener,
	recordingSignalConsumer RecordingSignalConsumer,
	subscription *aeron.Subscription,
	fragmentLimit int,
) *ControlResponseAdapter {
	if fragmentLimit <= 0 {
		fragmentLimit = defaultPollerFragmentLimit
	}
	a := &ControlResponseAdapter{
		subscription:            subscription,
		fragmentLimit:           fragmentLimit,
		listener:                listener,
		recordingSignalConsumer: recordingSignalConsumer,
		decoder:                 newResponseDecoder(false),
	}
	a.assembler = aeron.NewFragmentAssembler(a.onFragment, aeron.DefaultFragmentAssemblyBufferLength)
	return a
}

// Poll the subscription and dispatch messages to the listeners. The error is the first decoding failure,
// if any; dispatch continues past failures.
func (a *ControlResponseAdapter) Poll() (int, error) {
	a.err = nil
	n := a.subscription.Poll(a.assembler.OnFragment, a.fragmentLimit)
	return n, a.err
}

func dispatchSignal(s *codecs.RecordingSignalEvent, consumer RecordingSignalConsumer) {
	consumer.OnSignal(s.ControlSessionId, s.CorrelationId, s.RecordingId, s.SubscriptionId, s.Position, s.Signal)
}

func (a *ControlResponseAdapter) setErr(err error) {
	if a.err == nil {
		a.err = err
	}
}

func (a *ControlResponseAdapter) onFragment(buffer *atomic.Buffer, offset, length int32, _ *logbuffer.Header) {
	if err := a.decoder.wrap(buffer, offset, length); err != nil {
		a.setErr(err)
		return
	}
	switch a.decoder.templateId() {
	case codecIds.controlResponse:
		if err := a.decoder.decode(&a.controlResponse); err != nil {
			a.setErr(err)
			return
		}
		r := &a.controlResponse
		a.listener.OnResponse(r.ControlSessionId, r.CorrelationId, r.RelevantId, r.Code, string(r.ErrorMessage))
	case codecIds.recordingDescriptor:
		d := new(codecs.RecordingDescriptor)
		if err := a.decoder.decode(d); err != nil {
			a.setErr(err)
			return
		}
		a.listener.OnRecordingDescriptor(d)
	case codecIds.recordingSignalEvent:
		if err := a.decoder.decode(&a.signalEvent); err != nil {
			a.setErr(err)
			return
		}
		if a.recordingSignalConsumer != nil {
			dispatchSignal(&a.signalEvent, a.recordingSignalConsumer)
		}
	}
}
