// Copyright 2026 Kalshi Trading.
// Copyright 2026 Adaptive Financial Consulting Limited (Java reference implementation).
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

package persistentsubscription

import (
	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/logbuffer"
	"github.com/lirm/aeron-go/aeron/logbuffer/term"
)

const (
	beginFragFlag    uint8 = 0x80
	endFragFlag      uint8 = 0x40
	unfragmentedFlag       = beginFragFlag | endFragFlag
	frameAlignment   int32 = 32
	dataHeaderLength int32 = 32
)

// messageBuilder accumulates the fragments of a message. Unlike aeron.FragmentAssembler it is not keyed by session
// id: a single builder spans the replay and live images (which have different session ids but identical term
// offsets) so a message split across the replay->live switch is reassembled correctly, like Java's
// ImageFragmentAssembler.
type messageBuilder struct {
	data           []byte
	nextTermOffset int32
	active         bool
}

func (b *messageBuilder) reset() {
	b.data = b.data[:0]
	b.active = false
}

func alignedFrameEnd(termOffset int32, length int32) int32 {
	return termOffset + ((length + dataHeaderLength + frameAlignment - 1) &^ (frameAlignment - 1))
}

// append handles a fragment; returns true when a complete message is available in b.data.
func (b *messageBuilder) append(buffer *atomic.Buffer, offset int32, length int32, header *logbuffer.Header) bool {
	flags := header.Flags()
	termOffset := header.Offset()
	if flags&beginFragFlag == beginFragFlag {
		b.data = append(b.data[:0], buffer.GetBytesArray(offset, length)...)
		b.active = true
		b.nextTermOffset = alignedFrameEnd(termOffset, length)
		return false
	}
	if !b.active {
		return false
	}
	if termOffset != b.nextTermOffset {
		b.reset()
		return false
	}
	b.data = append(b.data, buffer.GetBytesArray(offset, length)...)
	b.nextTermOffset = alignedFrameEnd(termOffset, length)
	return flags&endFragFlag == endFragFlag
}

type imageFragmentAssembler struct {
	delegate func(buffer *atomic.Buffer, offset int32, length int32, header *logbuffer.Header)
	builder  messageBuilder
}

func (a *imageFragmentAssembler) onFragment(buffer *atomic.Buffer, offset int32, length int32,
	header *logbuffer.Header) {
	if header.Flags()&unfragmentedFlag == unfragmentedFlag {
		a.builder.reset()
		a.delegate(buffer, offset, length, header)
		return
	}
	if a.builder.append(buffer, offset, length, header) {
		msg := atomic.MakeBuffer(a.builder.data, len(a.builder.data))
		a.delegate(msg, 0, int32(len(a.builder.data)), header)
		a.builder.reset()
	}
}

type imageControlledFragmentAssembler struct {
	delegate func(buffer *atomic.Buffer, offset int32, length int32, header *logbuffer.Header) term.ControlledPollAction
	builder  messageBuilder
}

func (a *imageControlledFragmentAssembler) onFragment(buffer *atomic.Buffer, offset int32, length int32,
	header *logbuffer.Header) term.ControlledPollAction {
	if header.Flags()&unfragmentedFlag == unfragmentedFlag {
		a.builder.reset()
		return a.delegate(buffer, offset, length, header)
	}
	priorData, priorNext, priorActive := len(a.builder.data), a.builder.nextTermOffset, a.builder.active
	if !a.builder.append(buffer, offset, length, header) {
		return term.ControlledPollActionContinue
	}
	msg := atomic.MakeBuffer(a.builder.data, len(a.builder.data))
	action := a.delegate(msg, 0, int32(len(a.builder.data)), header)
	if action == term.ControlledPollActionAbort {
		// The final fragment will be redelivered: restore the builder to its state before it was appended.
		a.builder.data = a.builder.data[:priorData]
		a.builder.nextTermOffset, a.builder.active = priorNext, priorActive
	} else {
		a.builder.reset()
	}
	return action
}
