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

import "github.com/lirm/aeron-go/archive/codecs"

// RecordingSignalConsumer is the Go counterpart of io.aeron.archive.client.RecordingSignalConsumer and
// receives recording signals on a control session.
type RecordingSignalConsumer interface {
	OnSignal(controlSessionId, correlationId, recordingId, subscriptionId, position int64,
		signal codecs.RecordingSignalEnum)
}

// RecordingSignalConsumerFunc adapts a function to RecordingSignalConsumer.
type RecordingSignalConsumerFunc func(controlSessionId, correlationId, recordingId, subscriptionId, position int64,
	signal codecs.RecordingSignalEnum)

// OnSignal calls f.
func (f RecordingSignalConsumerFunc) OnSignal(controlSessionId, correlationId, recordingId, subscriptionId,
	position int64, signal codecs.RecordingSignalEnum) {
	f(controlSessionId, correlationId, recordingId, subscriptionId, position, signal)
}

// RecordingEventsListener is the Go counterpart of io.aeron.archive.client.RecordingEventsListener and
// receives events published on the archive recording events channel.
type RecordingEventsListener interface {
	OnStart(recordingId, startPosition int64, sessionId, streamId int32, channel, sourceIdentity string)
	OnProgress(recordingId, startPosition, position int64)
	OnStop(recordingId, startPosition, stopPosition int64)
}

// RecordingEventsListenerFuncs adapts optional functions to RecordingEventsListener. Nil functions are ignored.
type RecordingEventsListenerFuncs struct {
	Start    func(recordingId, startPosition int64, sessionId, streamId int32, channel, sourceIdentity string)
	Progress func(recordingId, startPosition, position int64)
	Stop     func(recordingId, startPosition, stopPosition int64)
}

// OnStart calls l.Start if set.
func (l RecordingEventsListenerFuncs) OnStart(recordingId, startPosition int64, sessionId, streamId int32,
	channel, sourceIdentity string) {
	if l.Start != nil {
		l.Start(recordingId, startPosition, sessionId, streamId, channel, sourceIdentity)
	}
}

// OnProgress calls l.Progress if set.
func (l RecordingEventsListenerFuncs) OnProgress(recordingId, startPosition, position int64) {
	if l.Progress != nil {
		l.Progress(recordingId, startPosition, position)
	}
}

// OnStop calls l.Stop if set.
func (l RecordingEventsListenerFuncs) OnStop(recordingId, startPosition, stopPosition int64) {
	if l.Stop != nil {
		l.Stop(recordingId, startPosition, stopPosition)
	}
}

// RecordingDescriptorConsumer and RecordingSubscriptionDescriptorConsumer (archive_catalog.go) are the Go
// counterparts of the Java consumers and receive a freshly decoded descriptor.

// ControlEventListener is the Go counterpart of io.aeron.archive.client.ControlEventListener.
type ControlEventListener interface {
	OnResponse(controlSessionId, correlationId, relevantId int64, code codecs.ControlResponseCodeEnum,
		errorMessage string)
}

// ControlEventListenerFunc adapts a function to ControlEventListener.
type ControlEventListenerFunc func(controlSessionId, correlationId, relevantId int64,
	code codecs.ControlResponseCodeEnum, errorMessage string)

// OnResponse calls f.
func (f ControlEventListenerFunc) OnResponse(controlSessionId, correlationId, relevantId int64,
	code codecs.ControlResponseCodeEnum, errorMessage string) {
	f(controlSessionId, correlationId, relevantId, code, errorMessage)
}

// ControlResponseListener is the Go counterpart of io.aeron.archive.client.ControlResponseListener: a
// listener for control responses and recording descriptors on a control session.
type ControlResponseListener interface {
	ControlEventListener
	OnRecordingDescriptor(descriptor *codecs.RecordingDescriptor)
}

// recordingSignalListenerConsumer bridges the legacy ArchiveListeners.RecordingSignalListener callback.
func recordingSignalListenerConsumer(listeners *ArchiveListeners) RecordingSignalConsumer {
	return RecordingSignalConsumerFunc(func(controlSessionId, correlationId, recordingId, subscriptionId,
		position int64, signal codecs.RecordingSignalEnum) {
		if listeners != nil && listeners.RecordingSignalListener != nil {
			listeners.RecordingSignalListener(&codecs.RecordingSignalEvent{
				ControlSessionId: controlSessionId,
				CorrelationId:    correlationId,
				RecordingId:      recordingId,
				SubscriptionId:   subscriptionId,
				Position:         position,
				Signal:           signal,
			})
		}
	})
}
