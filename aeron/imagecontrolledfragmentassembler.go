// Copyright 2026 Kalshi Trading
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

// ImageControlledFragmentAssembler reassembles fragmented messages from a single Image (session) for
// controlled polling. It mirrors io.aeron.ImageControlledFragmentAssembler. If the delegate returns
// ControlledPollActionAbort for a reassembled message, the last fragment is rolled back so it can be
// redelivered on the next poll.
type ImageControlledFragmentAssembler struct {
	delegate term.ControlledFragmentHandler
	builder  *BufferBuilder
}

// NewImageControlledFragmentAssembler constructs an assembler for a single Image. initialBufferLength
// is the initial capacity of the reassembly buffer; 0 allocates lazily.
func NewImageControlledFragmentAssembler(
	delegate term.ControlledFragmentHandler, initialBufferLength int32) *ImageControlledFragmentAssembler {
	return &ImageControlledFragmentAssembler{delegate: delegate, builder: newAssemblerBufferBuilder(initialBufferLength)}
}

// Delegate is the handler to which whole messages are delegated.
func (f *ImageControlledFragmentAssembler) Delegate() term.ControlledFragmentHandler {
	return f.delegate
}

// BufferBuilder is the buffer used for reassembly.
func (f *ImageControlledFragmentAssembler) BufferBuilder() *BufferBuilder {
	return f.builder
}

// IsAssembling reports whether a partially assembled message is held.
func (f *ImageControlledFragmentAssembler) IsAssembling() bool {
	return f.builder.nextTermOffset != NullValue
}

// OnFragment reassembles and forwards whole messages to the delegate.
func (f *ImageControlledFragmentAssembler) OnFragment(
	buffer *atomic.Buffer,
	offset int32,
	length int32,
	header *logbuffer.Header) term.ControlledPollAction {
	flags := header.Flags()

	if (flags & unfragmented) == unfragmented {
		return f.delegate(buffer, offset, length, header)
	}
	if (flags & beginFrag) == beginFrag {
		f.builder.Reset().
			CaptureHeader(header).
			Append(buffer, offset, length).
			SetNextTermOffset(header.NextTermOffset())
		return term.ControlledPollActionContinue
	}
	return onControlledFragment(f.delegate, f.builder, buffer, offset, length, header, flags)
}
