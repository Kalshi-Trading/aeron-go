/*
Copyright 2026 Kalshi Trading

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Package errorlog reads and writes the distinct error log format used by the Aeron media driver, archive and
// cluster (org.agrona.concurrent.errors.DistinctErrorLog / ErrorLogReader).
//
// Record layout (each record aligned to RecordAlignment):
//
//	0                   1                   2                   3
//	0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
//	+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//	|R|                         Length                              |
//	+-+-------------------------------------------------------------+
//	|R|                    Observation Count                        |
//	+-+-------------------------------------------------------------+
//	|R|                Last Observation Timestamp                   |
//	|                                                               |
//	+-+-------------------------------------------------------------+
//	|R|               First Observation Timestamp                   |
//	|                                                               |
//	+---------------------------------------------------------------+
//	|                     Encoded Error (UTF-8)                    ...
//	+---------------------------------------------------------------+
package errorlog

import (
	"fmt"
	"io"
	"time"

	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/util"
)

// Record field offsets.
const (
	LengthOffset                    int32 = 0
	ObservationCountOffset                = util.SizeOfInt32
	LastObservationTimestampOffset        = ObservationCountOffset + util.SizeOfInt32
	FirstObservationTimestampOffset       = LastObservationTimestampOffset + util.SizeOfInt64
	EncodedErrorOffset                    = FirstObservationTimestampOffset + util.SizeOfInt64
	RecordAlignment                 int32 = util.SizeOfInt64
)

// TimestampFormat is the layout used by PrintErrorLog, equivalent to Java's "yyyy-MM-dd HH:mm:ss.SSSZ".
const TimestampFormat = "2006-01-02 15:04:05.000-0700"

// Observation is one distinct error in the log.
type Observation struct {
	ObservationCount            int32
	FirstObservationTimestampMs int64
	LastObservationTimestampMs  int64
	EncodedError                string
}

// ErrorConsumer is called for each distinct error read.
type ErrorConsumer func(observationCount int32, firstObservationTimestampMs, lastObservationTimestampMs int64,
	encodedError string)

// HasErrors reports whether the buffer contains at least one error record.
func HasErrors(buffer *atomic.Buffer) bool {
	return buffer.Capacity() >= util.SizeOfInt32 && buffer.GetInt32Volatile(LengthOffset) > 0
}

// Read calls consumer for every distinct error in the log and returns the number read.
func Read(buffer *atomic.Buffer, consumer ErrorConsumer) int {
	return ReadSince(buffer, consumer, 0)
}

// ReadSince calls consumer for every distinct error whose last observation is at or after sinceTimestampMs.
func ReadSince(buffer *atomic.Buffer, consumer ErrorConsumer, sinceTimestampMs int64) int {
	entries := 0
	offset := int32(0)
	capacity := buffer.Capacity()
	for offset <= capacity-EncodedErrorOffset {
		length := buffer.GetInt32Volatile(offset + LengthOffset)
		if length > capacity-offset {
			length = capacity - offset
		}
		if length <= 0 {
			break
		}
		lastTs := buffer.GetInt64Volatile(offset + LastObservationTimestampOffset)
		if lastTs >= sinceTimestampMs {
			entries++
			consumer(
				buffer.GetInt32Volatile(offset+ObservationCountOffset),
				buffer.GetInt64(offset+FirstObservationTimestampOffset),
				lastTs,
				string(buffer.GetBytesArray(offset+EncodedErrorOffset, length-EncodedErrorOffset)))
		}
		offset += util.AlignInt32(length, RecordAlignment)
	}
	return entries
}

// ReadAll returns all distinct errors in the log.
func ReadAll(buffer *atomic.Buffer) []Observation {
	var out []Observation
	Read(buffer, func(count int32, first, last int64, encoded string) {
		out = append(out, Observation{count, first, last, encoded})
	})
	return out
}

func formatMs(ms int64) string {
	return time.UnixMilli(ms).Format(TimestampFormat)
}

// PrintErrorLog writes the log to w in the same format as Java CommonContext.printErrorLog and returns the number of
// distinct errors.
func PrintErrorLog(buffer *atomic.Buffer, w io.Writer) int {
	if !HasErrors(buffer) {
		fmt.Fprintf(w, "\n0 distinct errors observed\n")
		return 0
	}
	count := Read(buffer, func(observationCount int32, first, last int64, encodedError string) {
		fmt.Fprintf(w, "\n%d observations from %s to %s for:\n%s\n",
			observationCount, formatMs(first), formatMs(last), encodedError)
	})
	fmt.Fprintf(w, "\n%d distinct errors observed.\n", count)
	return count
}
