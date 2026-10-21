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

// Package lossreport reads the media driver loss report (loss-report.dat), mirroring
// io.aeron.driver.reports.LossReport / LossReportReader.
//
// Entry layout (each entry aligned to EntryAlignment):
//
//	0                   1                   2                   3
//	0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
//	+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//	|R|                    Observation Count                        |
//	|                                                               |
//	+-+-------------------------------------------------------------+
//	|R|                     Total Bytes Lost                        |
//	|                                                               |
//	+---------------------------------------------------------------+
//	|                 First Observation Timestamp                   |
//	|                                                               |
//	+---------------------------------------------------------------+
//	|                  Last Observation Timestamp                   |
//	|                                                               |
//	+---------------------------------------------------------------+
//	|                          Session ID                           |
//	+---------------------------------------------------------------+
//	|                           Stream ID                           |
//	+---------------------------------------------------------------+
//	|          Channel (int32 length + US-ASCII, 4-byte aligned)   ...
//	+---------------------------------------------------------------+
//	|          Source (int32 length + US-ASCII)                    ...
//	+---------------------------------------------------------------+
package lossreport

import (
	"fmt"
	"io"
	"path/filepath"
	"time"

	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/util"
	"github.com/lirm/aeron-go/aeron/util/memmap"
)

// FileName is the name of the loss report file in the Aeron directory.
const FileName = "loss-report.dat"

// Entry field offsets.
const (
	ObservationCountOffset int32 = 0
	TotalBytesLostOffset         = ObservationCountOffset + util.SizeOfInt64
	FirstObservationOffset       = TotalBytesLostOffset + util.SizeOfInt64
	LastObservationOffset        = FirstObservationOffset + util.SizeOfInt64
	SessionIDOffset              = LastObservationOffset + util.SizeOfInt64
	StreamIDOffset               = SessionIDOffset + util.SizeOfInt32
	ChannelOffset                = StreamIDOffset + util.SizeOfInt32
	EntryAlignment               = util.CacheLineLength
)

// CSVHeader is the header line printed by the Java LossStat tool.
const CSVHeader = "#OBSERVATION_COUNT,TOTAL_BYTES_LOST,FIRST_OBSERVATION,LAST_OBSERVATION,SESSION_ID,STREAM_ID,CHANNEL,SOURCE"

// Entry is one loss report entry (one per image that observed loss).
type Entry struct {
	ObservationCount            int64
	TotalBytesLost              int64
	FirstObservationTimestampMs int64
	LastObservationTimestampMs  int64
	SessionID                   int32
	StreamID                    int32
	Channel                     string
	Source                      string
}

// EntryConsumer is called for each entry read.
type EntryConsumer func(entry *Entry)

// Path returns the path of the loss report in aeronDir.
func Path(aeronDir string) string {
	return filepath.Join(aeronDir, FileName)
}

// Read calls consumer for each entry in buffer and returns the number of entries read.
func Read(buffer *atomic.Buffer, consumer EntryConsumer) int {
	capacity := buffer.Capacity()
	records := 0
	offset := int32(0)
	var entry Entry
	for offset+ChannelOffset+util.SizeOfInt32 <= capacity {
		observationCount := buffer.GetInt64Volatile(offset + ObservationCountOffset)
		if observationCount <= 0 {
			break
		}
		channelLength := buffer.GetInt32(offset + ChannelOffset)
		sourceOffset := offset + ChannelOffset + util.AlignInt32(util.SizeOfInt32+channelLength, util.SizeOfInt32)
		if channelLength < 0 || sourceOffset+util.SizeOfInt32 > capacity {
			break
		}
		sourceLength := buffer.GetInt32(sourceOffset)
		if sourceLength < 0 || sourceOffset+util.SizeOfInt32+sourceLength > capacity {
			break
		}
		records++
		entry = Entry{
			ObservationCount:            observationCount,
			TotalBytesLost:              buffer.GetInt64Volatile(offset + TotalBytesLostOffset),
			FirstObservationTimestampMs: buffer.GetInt64(offset + FirstObservationOffset),
			LastObservationTimestampMs:  buffer.GetInt64Volatile(offset + LastObservationOffset),
			SessionID:                   buffer.GetInt32(offset + SessionIDOffset),
			StreamID:                    buffer.GetInt32(offset + StreamIDOffset),
			Channel:                     string(buffer.GetBytesArray(offset+ChannelOffset+util.SizeOfInt32, channelLength)),
			Source:                      string(buffer.GetBytesArray(sourceOffset+util.SizeOfInt32, sourceLength)),
		}
		consumer(&entry)
		recordLength := sourceOffset + util.SizeOfInt32 + sourceLength - offset
		offset += util.AlignInt32(recordLength, EntryAlignment)
	}
	return records
}

// ReadAll returns a copy of every entry in buffer.
func ReadAll(buffer *atomic.Buffer) []Entry {
	var out []Entry
	Read(buffer, func(e *Entry) { out = append(out, *e) })
	return out
}

// TimestampFormat matches Java's "yyyy-MM-dd HH:mm:ss.SSSZ".
const TimestampFormat = "2006-01-02 15:04:05.000-0700"

// CSVEntryConsumer returns a consumer writing entries as CSV in the format of Java LossReportReader.defaultEntryConsumer.
func CSVEntryConsumer(w io.Writer) EntryConsumer {
	return func(e *Entry) {
		fmt.Fprintf(w, "%d,%d,%s,%s,%d,%d,%s,%s\n",
			e.ObservationCount, e.TotalBytesLost,
			time.UnixMilli(e.FirstObservationTimestampMs).Format(TimestampFormat),
			time.UnixMilli(e.LastObservationTimestampMs).Format(TimestampFormat),
			e.SessionID, e.StreamID, e.Channel, e.Source)
	}
}

// Report is a mapped loss report file.
type Report struct {
	mmap   *memmap.File
	buffer *atomic.Buffer
}

// Open maps the loss report in aeronDir read-only style (the file is shared with the driver).
func Open(aeronDir string) (*Report, error) {
	return MapFile(Path(aeronDir))
}

// MapFile maps the loss report at path.
func MapFile(path string) (*Report, error) {
	m, err := memmap.MapExisting(path, 0, 0)
	if err != nil {
		return nil, err
	}
	return &Report{mmap: m, buffer: atomic.MakeBuffer(m.GetMemoryPtr(), m.GetMemorySize())}, nil
}

// Buffer returns the mapped buffer.
func (r *Report) Buffer() *atomic.Buffer { return r.buffer }

// Read calls consumer for each entry.
func (r *Report) Read(consumer EntryConsumer) int { return Read(r.buffer, consumer) }

// Entries returns a copy of all entries.
func (r *Report) Entries() []Entry { return ReadAll(r.buffer) }

// Close unmaps the file.
func (r *Report) Close() error {
	if r.mmap == nil {
		return nil
	}
	err := r.mmap.Close()
	r.mmap = nil
	return err
}
