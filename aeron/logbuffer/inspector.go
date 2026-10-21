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

package logbuffer

import (
	"bufio"
	"fmt"
	"io"
	"time"

	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/protocol"
)

// InspectOptions controls the output of Inspect, mirroring the io.aeron.samples.LogInspector options.
type InspectOptions struct {
	MessageDumpLimit  int32 // max bytes dumped per message; <= 0 means unlimited
	ASCII             bool  // dump payloads as ASCII instead of hex
	SkipDefaultHeader bool
	ScanOverZeroes    bool
}

// Inspect writes a dump of the log buffer in the same format as io.aeron.samples.LogInspector.
func Inspect(w io.Writer, lb *LogBuffers, opts InspectOptions) error {
	out := bufio.NewWriter(w)
	limit := opts.MessageDumpLimit
	if limit <= 0 {
		limit = 1<<31 - 1
	}
	meta := lb.Meta()
	termLength := lb.TermLength()
	initialTermID := meta.InitialTermID()
	sep := "======================================================================"

	fmt.Fprintln(out, sep)
	fmt.Fprintf(out, "%s Inspection dump for %s\n", time.Now().Format("Mon Jan 02 15:04:05 MST 2006"), lb.FileName())
	fmt.Fprintln(out, sep)
	fmt.Fprintf(out, "   Is connected: %t\n", meta.IsConnected())
	fmt.Fprintf(out, "Initial term id: %d\n", initialTermID)
	fmt.Fprintf(out, "     Term count: %d\n", meta.ActiveTermCount())
	fmt.Fprintf(out, "   Active index: %d\n", IndexByTermCount(int64(meta.ActiveTermCount())))
	fmt.Fprintf(out, "    Term length: %d\n", termLength)
	fmt.Fprintf(out, "     MTU length: %d\n", meta.MtuLength())
	fmt.Fprintf(out, "      Page size: %d\n", meta.PageSize())
	fmt.Fprintf(out, "   EOS position: %d\n", meta.EndOfStreamPosition())
	fmt.Fprintln(out)

	var d protocol.DataHeaderFlyweight
	if !opts.SkipDefaultHeader {
		d.Wrap(meta.DefaultFrameHeaderBuffer(), 0)
		fmt.Fprintf(out, "default %s\n", d.String())
	}
	fmt.Fprintln(out)

	shift := PositionBitsToShift(termLength)
	for i := int32(0); i < PartitionCount; i++ {
		rawTail := meta.RawTailVolatile(i)
		termOffset := rawTail & 0xFFFFFFFF
		termID := TermID(rawTail)
		offset := TermOffset(rawTail, int64(termLength))
		fmt.Fprintf(out, "Index %d Term Meta Data termOffset=%d termId=%d rawTail=%d position=%d\n",
			i, termOffset, termID, rawTail, ComputePosition(termID, offset, shift, initialTermID))
	}

	hdrLen := DataFrameHeader.Length
	for i := 0; i < PartitionCount; i++ {
		fmt.Fprintln(out)
		fmt.Fprintln(out, sep)
		fmt.Fprintf(out, "Index %d Term Data\n\n", i)
		term := lb.Buffer(i)
		offset := int32(0)
		for offset < termLength {
			d.Wrap(term, offset)
			fmt.Fprintf(out, "%d: %s\n", offset, d.String())
			frameLength := d.FrameLength()
			if frameLength < hdrLen {
				if frameLength == 0 && opts.ScanOverZeroes {
					offset += FrameAlignment
					continue
				}
				fmt.Fprintln(out, formatBytes(term, offset+hdrLen, min32(termLength-(offset+hdrLen), limit), opts.ASCII))
				break
			}
			fmt.Fprintln(out, formatBytes(term, offset+hdrLen, min32(frameLength-hdrLen, limit), opts.ASCII))
			offset += alignFrame(frameLength)
		}
	}
	return out.Flush()
}

func alignFrame(v int32) int32 { return (v + FrameAlignment - 1) &^ (FrameAlignment - 1) }

func min32(a, b int32) int32 {
	if a < b {
		return a
	}
	return b
}

func formatBytes(buf *atomic.Buffer, offset, length int32, ascii bool) string {
	if length <= 0 {
		return ""
	}
	b := buf.GetBytesArray(offset, length)
	if ascii {
		for i := range b {
			if b[i] >= 0x80 {
				b[i] = 0
			}
		}
		return string(b)
	}
	const hex = "0123456789ABCDEF"
	chars := make([]byte, len(b)*2)
	for i, v := range b {
		chars[i*2] = hex[v>>4]
		chars[i*2+1] = hex[v&0x0F]
	}
	return string(chars)
}
