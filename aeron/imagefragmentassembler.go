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

// ImageFragmentAssembler reassembles fragmented messages from a single Image (session) so that the
// delegate only sees whole messages. It mirrors io.aeron.ImageFragmentAssembler and is the cheaper
// choice when polling an Image directly, as no per-session lookup is needed.
//
// The Header passed to the delegate for a reassembled message is that of the first fragment, completed
// to describe the whole message (see BufferBuilder.CompleteHeader).
type ImageFragmentAssembler struct {
	delegate term.FragmentHandler
	builder  *BufferBuilder
}

// NewImageFragmentAssembler constructs an assembler for a single Image. initialBufferLength is the
// initial capacity of the reassembly buffer; 0 allocates lazily.
func NewImageFragmentAssembler(delegate term.FragmentHandler, initialBufferLength int32) *ImageFragmentAssembler {
	return &ImageFragmentAssembler{delegate: delegate, builder: newAssemblerBufferBuilder(initialBufferLength)}
}

// Delegate is the handler to which whole messages are delegated.
func (f *ImageFragmentAssembler) Delegate() term.FragmentHandler {
	return f.delegate
}

// BufferBuilder is the buffer used for reassembly.
func (f *ImageFragmentAssembler) BufferBuilder() *BufferBuilder {
	return f.builder
}

// IsAssembling reports whether a partially assembled message is held.
func (f *ImageFragmentAssembler) IsAssembling() bool {
	return f.builder.nextTermOffset != NullValue
}

// OnFragment reassembles and forwards whole messages to the delegate.
func (f *ImageFragmentAssembler) OnFragment(
	buffer *atomic.Buffer,
	offset int32,
	length int32,
	header *logbuffer.Header) {
	flags := header.Flags()
	builder := f.builder

	if (flags & unfragmented) == unfragmented {
		f.delegate(buffer, offset, length, header)
	} else if (flags & beginFrag) == beginFrag {
		builder.Reset().
			CaptureHeader(header).
			Append(buffer, offset, length).
			SetNextTermOffset(header.NextTermOffset())
	} else if header.TermOffset() == builder.nextTermOffset {
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
