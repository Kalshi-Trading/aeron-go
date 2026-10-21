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

// RecordingEventsAdapter is the Go port of io.aeron.archive.client.RecordingEventsAdapter: it polls the
// archive recording events channel and dispatches started/progress/stopped events to a
// RecordingEventsListener. The adapter owned by an Archive (Archive.Events) additionally invokes the
// legacy ArchiveListeners.RecordingEvent*Listener callbacks. It is not safe for concurrent use.
type RecordingEventsAdapter struct {
	Subscription *aeron.Subscription
	Enabled      bool
	archive      *Archive // link to parent, nil for standalone adapters

	listener      RecordingEventsListener
	fragmentLimit int
	decoder       responseDecoder
	err           error
	handler       FragmentHandlerWithListeners
	onFragmentFn  func(*atomic.Buffer, int32, int32, *logbuffer.Header)

	started  codecs.RecordingStarted
	progress codecs.RecordingProgress
	stopped  codecs.RecordingStopped
}

// FragmentHandlerWithListeners provides a FragmentHandler with ArchiveListeners
type FragmentHandlerWithListeners func(listeners *ArchiveListeners, buffer *atomic.Buffer, offset int32, length int32, header *logbuffer.Header)

// NewRecordingEventsAdapter creates a standalone adapter for a recording events subscription. A
// fragmentLimit <= 0 uses the default of 10.
func NewRecordingEventsAdapter(listener RecordingEventsListener, subscription *aeron.Subscription, fragmentLimit int) *RecordingEventsAdapter {
	if fragmentLimit <= 0 {
		fragmentLimit = defaultPollerFragmentLimit
	}
	return &RecordingEventsAdapter{
		Subscription:  subscription,
		Enabled:       subscription != nil,
		listener:      listener,
		fragmentLimit: fragmentLimit,
		decoder:       newResponseDecoder(false),
	}
}

// SetListener sets the RecordingEventsListener events are dispatched to.
func (rea *RecordingEventsAdapter) SetListener(listener RecordingEventsListener) {
	rea.listener = listener
}

// Poll the subscription and dispatch events to the listener. The error is the first decoding failure, if
// any; dispatch continues past failures and unknown templates are skipped.
func (rea *RecordingEventsAdapter) Poll() (int, error) {
	limit := rea.fragmentLimit
	if limit <= 0 {
		limit = defaultPollerFragmentLimit
	}
	return rea.poll(nil, limit)
}

// PollWithContext polls the subscription. If handler is nil the builtin handler dispatches to the
// RecordingEventsListener (if set) and the Archive's legacy listener callbacks; otherwise handler is called
// with the Archive's listeners. A fragmentLimit of 0 polls a single fragment.
//
// Deprecated: use Poll or Archive.RecordingEventsPoll.
func (rea *RecordingEventsAdapter) PollWithContext(handler FragmentHandlerWithListeners, fragmentLimit int) int {
	if fragmentLimit == 0 {
		fragmentLimit = 1
	}
	n, err := rea.poll(handler, fragmentLimit)
	if err != nil {
		rea.onError(err)
	}
	return n
}

func (rea *RecordingEventsAdapter) poll(handler FragmentHandlerWithListeners, fragmentLimit int) (int, error) {
	if rea.onFragmentFn == nil {
		rea.onFragmentFn = rea.onFragment
	}
	if rea.decoder.marshaller == nil {
		rea.decoder = newResponseDecoder(false)
	}
	if rea.archive != nil && rea.archive.Options != nil {
		rea.decoder.rangeChecking = rea.archive.Options.RangeChecking
	}
	rea.handler = handler
	rea.err = nil
	n := rea.Subscription.Poll(rea.onFragmentFn, fragmentLimit)
	return n, rea.err
}

func (rea *RecordingEventsAdapter) onError(err error) {
	if rea.archive != nil && rea.archive.Listeners != nil && rea.archive.Listeners.ErrorListener != nil {
		rea.archive.Listeners.ErrorListener(err)
	}
}

func (rea *RecordingEventsAdapter) legacyListeners() *ArchiveListeners {
	if rea.archive == nil {
		return nil
	}
	return rea.archive.Listeners
}

func (rea *RecordingEventsAdapter) setErr(err error) {
	if rea.err == nil {
		rea.err = err
	}
}

func (rea *RecordingEventsAdapter) onFragment(buffer *atomic.Buffer, offset int32, length int32, header *logbuffer.Header) {
	if rea.handler != nil {
		rea.handler(rea.legacyListeners(), buffer, offset, length, header)
		return
	}
	if err := rea.decoder.wrap(buffer, offset, length); err != nil {
		rea.setErr(err)
		return
	}
	legacy := rea.legacyListeners()

	switch rea.decoder.templateId() {
	case codecIds.recordingStarted:
		if err := rea.decoder.decode(&rea.started); err != nil {
			rea.setErr(err)
			return
		}
		e := &rea.started
		if rea.listener != nil {
			rea.listener.OnStart(e.RecordingId, e.StartPosition, e.SessionId, e.StreamId, string(e.Channel),
				string(e.SourceIdentity))
		}
		if legacy != nil && legacy.RecordingEventStartedListener != nil {
			legacy.RecordingEventStartedListener(&codecs.RecordingStarted{
				RecordingId:    e.RecordingId,
				StartPosition:  e.StartPosition,
				SessionId:      e.SessionId,
				StreamId:       e.StreamId,
				Channel:        append([]uint8(nil), e.Channel...),
				SourceIdentity: append([]uint8(nil), e.SourceIdentity...),
			})
		}

	case codecIds.recordingProgress:
		if err := rea.decoder.decode(&rea.progress); err != nil {
			rea.setErr(err)
			return
		}
		e := rea.progress
		if rea.listener != nil {
			rea.listener.OnProgress(e.RecordingId, e.StartPosition, e.Position)
		}
		if legacy != nil && legacy.RecordingEventProgressListener != nil {
			legacy.RecordingEventProgressListener(&e)
		}

	case codecIds.recordingStopped:
		if err := rea.decoder.decode(&rea.stopped); err != nil {
			rea.setErr(err)
			return
		}
		e := rea.stopped
		if rea.listener != nil {
			rea.listener.OnStop(e.RecordingId, e.StartPosition, e.StopPosition)
		}
		if legacy != nil && legacy.RecordingEventStoppedListener != nil {
			legacy.RecordingEventStoppedListener(&e)
		}
	}
}
