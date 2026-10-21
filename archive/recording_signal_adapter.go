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

// RecordingSignalAdapter is the Go port of io.aeron.archive.client.RecordingSignalAdapter. Each poll
// dispatches at most one ControlResponse (to the ControlEventListener) or RecordingSignalEvent (to the
// RecordingSignalConsumer) for its control session; other sessions' messages are skipped. It is not safe
// for concurrent use.
type RecordingSignalAdapter struct {
	subscription            *aeron.Subscription
	fragmentLimit           int
	controlSessionId        int64
	controlEventListener    ControlEventListener
	recordingSignalConsumer RecordingSignalConsumer
	assembler               *aeron.ControlledFragmentAssembler
	decoder                 responseDecoder
	err                     error
	isDone                  bool

	controlResponse codecs.ControlResponse
	signalEvent     codecs.RecordingSignalEvent
}

// NewRecordingSignalAdapter creates an adapter; either listener may be nil. A fragmentLimit <= 0 uses the
// default of 10.
func NewRecordingSignalAdapter(
	controlSessionId int64,
	controlEventListener ControlEventListener,
	recordingSignalConsumer RecordingSignalConsumer,
	subscription *aeron.Subscription,
	fragmentLimit int,
) *RecordingSignalAdapter {
	if fragmentLimit <= 0 {
		fragmentLimit = defaultPollerFragmentLimit
	}
	a := &RecordingSignalAdapter{
		subscription:            subscription,
		fragmentLimit:           fragmentLimit,
		controlSessionId:        controlSessionId,
		controlEventListener:    controlEventListener,
		recordingSignalConsumer: recordingSignalConsumer,
		decoder:                 newResponseDecoder(false),
	}
	a.assembler = aeron.NewControlledFragmentAssembler(a.onFragment, aeron.DefaultFragmentAssemblyBufferLength)
	return a
}

// Poll the subscription for the next control response or recording signal on the session.
func (a *RecordingSignalAdapter) Poll() (int, error) {
	a.isDone = false
	a.err = nil
	n := a.subscription.ControlledPoll(a.assembler.OnFragment, a.fragmentLimit)
	return n, a.err
}

// IsDone reports whether the last poll dispatched a message.
func (a *RecordingSignalAdapter) IsDone() bool { return a.isDone }

func (a *RecordingSignalAdapter) onFragment(buffer *atomic.Buffer, offset, length int32, _ *logbuffer.Header) term.ControlledPollAction {
	if a.isDone {
		return term.ControlledPollActionAbort
	}
	if err := a.decoder.wrap(buffer, offset, length); err != nil {
		a.err = err
		return term.ControlledPollActionBreak
	}
	switch a.decoder.templateId() {
	case codecIds.controlResponse:
		if err := a.decoder.decode(&a.controlResponse); err != nil {
			a.err = err
			return term.ControlledPollActionBreak
		}
		r := &a.controlResponse
		if r.ControlSessionId == a.controlSessionId {
			if a.controlEventListener != nil {
				a.controlEventListener.OnResponse(r.ControlSessionId, r.CorrelationId, r.RelevantId, r.Code,
					string(r.ErrorMessage))
			}
			a.isDone = true
			return term.ControlledPollActionBreak
		}
	case codecIds.recordingSignalEvent:
		if err := a.decoder.decode(&a.signalEvent); err != nil {
			a.err = err
			return term.ControlledPollActionBreak
		}
		if a.signalEvent.ControlSessionId == a.controlSessionId {
			if a.recordingSignalConsumer != nil {
				dispatchSignal(&a.signalEvent, a.recordingSignalConsumer)
			}
			a.isDone = true
			return term.ControlledPollActionBreak
		}
	}
	return term.ControlledPollActionContinue
}
