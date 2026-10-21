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
	"bytes"
	"fmt"
	"io"

	"github.com/lirm/aeron-go/aeron"
	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/archive/codecs"
)

// defaultPollerFragmentLimit matches the Java poller FRAGMENT_LIMIT.
const defaultPollerFragmentLimit = 10

var archiveSchemaId = (&codecs.ControlResponse{}).SbeSchemaId()

type sbeDecoder interface {
	Decode(m *codecs.SbeGoMarshaller, r io.Reader, actingVersion uint16, blockLength uint16, doRangeCheck bool) error
}

// responseDecoder holds per-poller decoding state so that pollers share nothing and can be used
// from different goroutines (one goroutine per poller).
type responseDecoder struct {
	marshaller    *codecs.SbeGoMarshaller
	buf           bytes.Buffer
	header        codecs.SbeGoMessageHeader
	rangeChecking bool
}

func newResponseDecoder(rangeChecking bool) responseDecoder {
	return responseDecoder{marshaller: codecs.NewSbeGoMarshaller(), rangeChecking: rangeChecking}
}

// wrap copies the message and decodes its header, verifying the archive schema id.
func (d *responseDecoder) wrap(buffer *atomic.Buffer, offset, length int32) error {
	if d.marshaller == nil {
		d.marshaller = codecs.NewSbeGoMarshaller()
	}
	d.buf.Reset()
	buffer.WriteBytes(&d.buf, offset, length)
	if err := d.header.Decode(d.marshaller, &d.buf); err != nil {
		return NewArchiveException("failed to decode message header: "+err.Error(), ErrorCodeGeneric, aeron.NullValue)
	}
	if d.header.SchemaId != archiveSchemaId {
		return NewArchiveException(
			fmt.Sprintf("expected schemaId=%d, actual=%d", archiveSchemaId, d.header.SchemaId),
			ErrorCodeGeneric, aeron.NullValue)
	}
	return nil
}

func (d *responseDecoder) templateId() uint16 {
	return d.header.TemplateId
}

func (d *responseDecoder) decode(msg sbeDecoder) error {
	if err := msg.Decode(d.marshaller, &d.buf, d.header.Version, d.header.BlockLength, d.rangeChecking); err != nil {
		return NewArchiveException(
			fmt.Sprintf("failed to decode templateId=%d: %s", d.header.TemplateId, err),
			ErrorCodeGeneric, aeron.NullValue)
	}
	return nil
}
