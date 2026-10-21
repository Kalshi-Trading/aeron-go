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

// RecordingEventsPoller is the Go port of io.aeron.archive.client.RecordingEventsPoller. Each poll consumes
// at most one recording started/progress/stopped event from the recording events subscription and exposes
// its fields. It is not safe for concurrent use.
type RecordingEventsPoller struct {
	subscription *aeron.Subscription
	decoder      responseDecoder
	err          error

	started  codecs.RecordingStarted
	progress codecs.RecordingProgress
	stopped  codecs.RecordingStopped

	templateId             int32
	isPollComplete         bool
	recordingId            int64
	recordingStartPosition int64
	recordingPosition      int64
	recordingStopPosition  int64
}

// NewRecordingEventsPoller creates a poller for a recording events subscription.
func NewRecordingEventsPoller(subscription *aeron.Subscription) *RecordingEventsPoller {
	return &RecordingEventsPoller{
		subscription:           subscription,
		decoder:                newResponseDecoder(false),
		templateId:             aeron.NullValue,
		recordingId:            aeron.NullValue,
		recordingStartPosition: aeron.NullValue,
		recordingPosition:      aeron.NullValue,
		recordingStopPosition:  aeron.NullValue,
	}
}

// Poll for a single recording event.
func (p *RecordingEventsPoller) Poll() (int, error) {
	if p.isPollComplete {
		p.isPollComplete = false
		p.templateId = aeron.NullValue
	}
	p.err = nil
	n := p.subscription.ControlledPoll(p.onFragment, 1)
	return n, p.err
}

// IsPollComplete reports whether the last poll decoded a recording event.
func (p *RecordingEventsPoller) IsPollComplete() bool { return p.isPollComplete }

// TemplateId of the last event, or aeron.NullValue.
func (p *RecordingEventsPoller) TemplateId() int32 { return p.templateId }

// RecordingId of the last event.
func (p *RecordingEventsPoller) RecordingId() int64 { return p.recordingId }

// RecordingStartPosition of the last event.
func (p *RecordingEventsPoller) RecordingStartPosition() int64 { return p.recordingStartPosition }

// RecordingPosition of the last event (the stop position for a stopped event).
func (p *RecordingEventsPoller) RecordingPosition() int64 { return p.recordingPosition }

// RecordingStopPosition of the last event, aeron.NullValue unless stopped.
func (p *RecordingEventsPoller) RecordingStopPosition() int64 { return p.recordingStopPosition }

// IsRecordingStarted reports whether the last event was RecordingStarted.
func (p *RecordingEventsPoller) IsRecordingStarted() bool {
	return p.templateId == int32(codecIds.recordingStarted)
}

// IsRecordingProgress reports whether the last event was RecordingProgress.
func (p *RecordingEventsPoller) IsRecordingProgress() bool {
	return p.templateId == int32(codecIds.recordingProgress)
}

// IsRecordingStopped reports whether the last event was RecordingStopped.
func (p *RecordingEventsPoller) IsRecordingStopped() bool {
	return p.templateId == int32(codecIds.recordingStopped)
}

func (p *RecordingEventsPoller) String() string {
	return fmt.Sprintf("RecordingEventsPoller{isPollComplete=%t, templateId=%d, recordingId=%d, "+
		"recordingStartPosition=%d, recordingPosition=%d, recordingStopPosition=%d}",
		p.isPollComplete, p.templateId, p.recordingId, p.recordingStartPosition, p.recordingPosition,
		p.recordingStopPosition)
}

func (p *RecordingEventsPoller) onFragment(buffer *atomic.Buffer, offset, length int32, _ *logbuffer.Header) term.ControlledPollAction {
	if p.isPollComplete {
		return term.ControlledPollActionAbort
	}
	if err := p.decoder.wrap(buffer, offset, length); err != nil {
		p.err = err
		return term.ControlledPollActionBreak
	}
	switch p.decoder.templateId() {
	case codecIds.recordingStarted:
		if err := p.decoder.decode(&p.started); err != nil {
			p.err = err
			return term.ControlledPollActionBreak
		}
		p.templateId = int32(codecIds.recordingStarted)
		p.recordingId = p.started.RecordingId
		p.recordingStartPosition = p.started.StartPosition
		p.recordingPosition = p.started.StartPosition
		p.recordingStopPosition = aeron.NullValue
	case codecIds.recordingProgress:
		if err := p.decoder.decode(&p.progress); err != nil {
			p.err = err
			return term.ControlledPollActionBreak
		}
		p.templateId = int32(codecIds.recordingProgress)
		p.recordingId = p.progress.RecordingId
		p.recordingStartPosition = p.progress.StartPosition
		p.recordingPosition = p.progress.Position
		p.recordingStopPosition = aeron.NullValue
	case codecIds.recordingStopped:
		if err := p.decoder.decode(&p.stopped); err != nil {
			p.err = err
			return term.ControlledPollActionBreak
		}
		p.templateId = int32(codecIds.recordingStopped)
		p.recordingId = p.stopped.RecordingId
		p.recordingStartPosition = p.stopped.StartPosition
		p.recordingStopPosition = p.stopped.StopPosition
		p.recordingPosition = p.stopped.StopPosition
	default:
		return term.ControlledPollActionContinue
	}
	p.isPollComplete = true
	return term.ControlledPollActionBreak
}
