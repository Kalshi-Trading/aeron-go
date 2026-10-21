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
	"sync"
	syncatomic "sync/atomic"

	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/logbuffer"
	"github.com/lirm/aeron-go/aeron/logbuffer/term"
)

const (
	DefaultFragmentAssemblyBufferLength = int32(4096)

	beginFrag    uint8 = 0x80
	endFrag      uint8 = 0x40
	unfragmented uint8 = 0x80 | 0x40
)

// FragmentAssembler sits in a chain-of-responsibility pattern and reassembles fragmented messages
// so that the next handler in the chain only sees whole messages. It mirrors io.aeron.FragmentAssembler.
//
// Unfragmented messages are delegated without copy. Fragmented messages are copied to a per-session
// BufferBuilder for reassembly before delegation. If a fragment is missing (the term offset of a
// fragment is not the one expected) the partial message is discarded.
//
// The Header passed to the delegate for a reassembled message is that of the first fragment, completed
// to describe the whole message (see BufferBuilder.CompleteHeader): flags are BEGIN|END, FrameLength is
// the header length plus the whole message length, and Position() is the position after the last
// fragment.
//
// Session based buffers are allocated and grown as necessary based on the length of messages to be
// assembled. Use OnUnavailableImage as (or from) the subscription's UnavailableImageHandler to free them
// when images go away. A FragmentAssembler is not safe for concurrent polling.
type FragmentAssembler struct {
	delegate              term.FragmentHandler
	initialBufferLength   int32
	builderBySessionIdMap map[int32]*BufferBuilder
	pendingFrees          sessionFreeQueue
}

// NewFragmentAssembler constructs an adapter to reassemble message fragments and delegate on whole messages.
// initialBufferLength is the initial capacity of each session's buffer; 0 allocates lazily.
func NewFragmentAssembler(delegate term.FragmentHandler, initialBufferLength int32) *FragmentAssembler {
	return &FragmentAssembler{
		delegate:              delegate,
		initialBufferLength:   initialBufferLength,
		builderBySessionIdMap: make(map[int32]*BufferBuilder),
	}
}

// Delegate is the handler to which whole messages are delegated.
func (f *FragmentAssembler) Delegate() term.FragmentHandler {
	return f.delegate
}

// Clear removes all existing session buffers. Must be called from the polling goroutine.
func (f *FragmentAssembler) Clear() {
	f.pendingFrees.drain(f.builderBySessionIdMap)
	for k := range f.builderBySessionIdMap {
		delete(f.builderBySessionIdMap, k)
	}
}

// FreeSessionBuffer frees the buffer for a session, returning true if one existed. Must be called from
// the polling goroutine; use OnUnavailableImage from other goroutines.
func (f *FragmentAssembler) FreeSessionBuffer(sessionID int32) bool {
	f.pendingFrees.drain(f.builderBySessionIdMap)
	_, ok := f.builderBySessionIdMap[sessionID]
	delete(f.builderBySessionIdMap, sessionID)
	return ok
}

// IsSessionAssembling reports whether a partially assembled message is held for the session.
func (f *FragmentAssembler) IsSessionAssembling(sessionID int32) bool {
	f.pendingFrees.drain(f.builderBySessionIdMap)
	builder, ok := f.builderBySessionIdMap[sessionID]
	return ok && builder.nextTermOffset != NullValue
}

// OnUnavailableImage schedules the buffer for the image's session to be freed by the polling goroutine
// on its next call to OnFragment, FreeSessionBuffer, IsSessionAssembling or Clear. It is safe to call
// from any goroutine and has the UnavailableImageHandler signature.
func (f *FragmentAssembler) OnUnavailableImage(image Image) {
	f.pendingFrees.add(image.SessionID())
}

// OnFragment reassembles and forwards whole messages to the delegate.
func (f *FragmentAssembler) OnFragment(
	buffer *atomic.Buffer,
	offset int32,
	length int32,
	header *logbuffer.Header) {
	if f.pendingFrees.hasPending() {
		f.pendingFrees.drain(f.builderBySessionIdMap)
	}

	flags := header.Flags()
	if (flags & unfragmented) == unfragmented {
		f.delegate(buffer, offset, length, header)
	} else if (flags & beginFrag) == beginFrag {
		builder := getBufferBuilder(f.builderBySessionIdMap, header.SessionId(), f.initialBufferLength)
		builder.Reset().
			CaptureHeader(header).
			Append(buffer, offset, length).
			SetNextTermOffset(header.NextTermOffset())
	} else if builder, ok := f.builderBySessionIdMap[header.SessionId()]; ok {
		if header.TermOffset() == builder.nextTermOffset {
			builder.Append(buffer, offset, length)
			if (flags & endFrag) == endFrag {
				f.delegate(builder.Buffer(), 0, builder.Limit(), builder.CompleteHeader(header))
				builder.Reset()
			} else {
				builder.SetNextTermOffset(header.NextTermOffset())
			}
		} else {
			builder.Reset()
		}
	}
}

func getBufferBuilder(builders map[int32]*BufferBuilder, sessionID int32, initialBufferLength int32) *BufferBuilder {
	builder, ok := builders[sessionID]
	if !ok {
		builder = newAssemblerBufferBuilder(initialBufferLength)
		builders[sessionID] = builder
	}
	return builder
}

// newAssemblerBufferBuilder clamps out of range initial lengths rather than failing, as the assembler
// constructors do not return errors; the buffer grows as needed regardless.
func newAssemblerBufferBuilder(initialBufferLength int32) *BufferBuilder {
	if initialBufferLength < 0 {
		initialBufferLength = 0
	}
	builder, _ := NewBufferBuilder(initialBufferLength)
	return builder
}

// sessionFreeQueue hands session ids from the conductor goroutine (image unavailable callbacks) to
// the polling goroutine, keeping the per-fragment cost to a single atomic load.
type sessionFreeQueue struct {
	pending syncatomic.Int32
	lock    sync.Mutex
	ids     []int32
}

func (q *sessionFreeQueue) add(sessionID int32) {
	q.lock.Lock()
	q.ids = append(q.ids, sessionID)
	q.pending.Store(1)
	q.lock.Unlock()
}

func (q *sessionFreeQueue) hasPending() bool {
	return q.pending.Load() != 0
}

func (q *sessionFreeQueue) drain(builders map[int32]*BufferBuilder) {
	if !q.hasPending() {
		return
	}
	q.lock.Lock()
	for _, id := range q.ids {
		delete(builders, id)
	}
	q.ids = q.ids[:0]
	q.pending.Store(0)
	q.lock.Unlock()
}
