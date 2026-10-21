// Copyright 2016 Stanislav Liberman
// Copyright 2022 Talos, Inc.
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

package aeron

import (
	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/logbuffer"
	"github.com/lirm/aeron-go/aeron/logbuffer/term"
)

// ControlledFragmentAssembler sits in a chain-of-responsibility pattern and reassembles fragmented
// messages so that the next handler in the chain only sees whole messages. It mirrors
// io.aeron.ControlledFragmentAssembler.
//
// Unfragmented messages are delegated without copy. Fragmented messages are copied to a per-session
// BufferBuilder for reassembly before delegation. If the delegate returns ControlledPollActionAbort for
// a reassembled message, the last fragment is rolled back so it can be redelivered on the next poll.
//
// The Header passed to the delegate for a reassembled message is that of the first fragment, completed
// to describe the whole message (see BufferBuilder.CompleteHeader).
//
// Session based buffers are allocated and grown as necessary based on the length of messages to be
// assembled. A ControlledFragmentAssembler is not safe for concurrent polling.
type ControlledFragmentAssembler struct {
	delegate              term.ControlledFragmentHandler
	initialBufferLength   int32
	builderBySessionIdMap map[int32]*BufferBuilder
	pendingFrees          sessionFreeQueue
}

// NewControlledFragmentAssembler constructs an adapter to reassemble message fragments and delegate on whole messages.
// initialBufferLength is the initial capacity of each session's buffer; 0 allocates lazily.
func NewControlledFragmentAssembler(delegate term.ControlledFragmentHandler, initialBufferLength int32) *ControlledFragmentAssembler {
	return &ControlledFragmentAssembler{
		delegate:              delegate,
		initialBufferLength:   initialBufferLength,
		builderBySessionIdMap: make(map[int32]*BufferBuilder),
	}
}

// Delegate is the handler to which whole messages are delegated.
func (f *ControlledFragmentAssembler) Delegate() term.ControlledFragmentHandler {
	return f.delegate
}

// Clear removes all existing session buffers. Must be called from the polling goroutine.
func (f *ControlledFragmentAssembler) Clear() {
	f.pendingFrees.drain(f.builderBySessionIdMap)
	for k := range f.builderBySessionIdMap {
		delete(f.builderBySessionIdMap, k)
	}
}

// FreeSessionBuffer frees the buffer for a session, returning true if one existed. Must be called from
// the polling goroutine; use OnUnavailableImage from other goroutines.
func (f *ControlledFragmentAssembler) FreeSessionBuffer(sessionID int32) bool {
	f.pendingFrees.drain(f.builderBySessionIdMap)
	_, ok := f.builderBySessionIdMap[sessionID]
	delete(f.builderBySessionIdMap, sessionID)
	return ok
}

// IsSessionAssembling reports whether a partially assembled message is held for the session.
func (f *ControlledFragmentAssembler) IsSessionAssembling(sessionID int32) bool {
	f.pendingFrees.drain(f.builderBySessionIdMap)
	builder, ok := f.builderBySessionIdMap[sessionID]
	return ok && builder.nextTermOffset != NullValue
}

// OnUnavailableImage schedules the buffer for the image's session to be freed by the polling goroutine
// on its next call to OnFragment, FreeSessionBuffer, IsSessionAssembling or Clear. It is safe to call
// from any goroutine and has the UnavailableImageHandler signature.
func (f *ControlledFragmentAssembler) OnUnavailableImage(image Image) {
	f.pendingFrees.add(image.SessionID())
}

// OnFragment reassembles and forwards whole messages to the delegate.
func (f *ControlledFragmentAssembler) OnFragment(
	buffer *atomic.Buffer,
	offset int32,
	length int32,
	header *logbuffer.Header) term.ControlledPollAction {
	if f.pendingFrees.hasPending() {
		f.pendingFrees.drain(f.builderBySessionIdMap)
	}

	flags := header.Flags()
	var action term.ControlledPollAction = term.ControlledPollActionContinue

	if (flags & unfragmented) == unfragmented {
		action = f.delegate(buffer, offset, length, header)
	} else if (flags & beginFrag) == beginFrag {
		builder := getBufferBuilder(f.builderBySessionIdMap, header.SessionID(), f.initialBufferLength)
		builder.Reset().
			CaptureHeader(header).
			Append(buffer, offset, length).
			SetNextTermOffset(header.NextTermOffset())
	} else if builder, ok := f.builderBySessionIdMap[header.SessionID()]; ok {
		action = onControlledFragment(f.delegate, builder, buffer, offset, length, header, flags)
	}

	return action
}

func onControlledFragment(
	delegate term.ControlledFragmentHandler,
	builder *BufferBuilder,
	buffer *atomic.Buffer,
	offset int32,
	length int32,
	header *logbuffer.Header,
	flags uint8) term.ControlledPollAction {
	var action term.ControlledPollAction = term.ControlledPollActionContinue
	if header.TermOffset() != builder.nextTermOffset {
		builder.Reset()
		return action
	}

	limit := builder.Limit()
	builder.Append(buffer, offset, length)
	if (flags & endFrag) == endFrag {
		action = delegate(builder.Buffer(), 0, builder.Limit(), builder.CompleteHeader(header))
		if action == term.ControlledPollActionAbort {
			builder.limit = limit
		} else {
			builder.Reset()
		}
	} else {
		builder.SetNextTermOffset(header.NextTermOffset())
	}
	return action
}
