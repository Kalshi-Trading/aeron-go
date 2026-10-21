/*
Copyright 2016-2018 Stanislav Liberman

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

package logbuffer

import (
	"fmt"

	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/flyweight"
	"github.com/lirm/aeron-go/aeron/util"
)

const (
	Clean                   int32 = 0
	NeedsCleaning                 = 1
	PartitionCount                = 3
	LogMetaDataSectionIndex       = PartitionCount

	TermMinLength     int32 = 64 * 1024
	TermMaxLength     int32 = 1024 * 1024 * 1024
	PageMinSize       int32 = 4 * 1024
	PageMaxSize       int32 = 1024 * 1024 * 1024
	LogMetaDataLength       = PageMinSize
)

/* LogBufferMetaData is the flyweight for LogBuffer meta data
 * <pre>
 *   0                   1                   2                   3
 *   0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
 *  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 *  |                       Tail Counter 0                          |
 *  |                                                               |
 *  +---------------------------------------------------------------+
 *  |                       Tail Counter 1                          |
 *  |                                                               |
 *  +---------------------------------------------------------------+
 *  |                       Tail Counter 2                          |
 *  |                                                               |
 *  +---------------------------------------------------------------+
 *  |                      Active Term Count                        |
 *  +---------------------------------------------------------------+
 *  |                     Cache Line Padding                       ...
 * ...                                                              |
 *  +---------------------------------------------------------------+
 *  |                    End of Stream Position                     |
 *  |                                                               |
 *  +---------------------------------------------------------------+
 *  |                        Is Connected                           |
 *  +---------------------------------------------------------------+
 *  |                      Cache Line Padding                      ...
 * ...                                                              |
 *  +---------------------------------------------------------------+
 *  |                 Registration / Correlation ID                 |
 *  |                                                               |
 *  +---------------------------------------------------------------+
 *  |                        Initial Term Id                        |
 *  +---------------------------------------------------------------+
 *  |                  Default Frame Header Length                  |
 *  +---------------------------------------------------------------+
 *  |                          MTU Length                           |
 *  +---------------------------------------------------------------+
 *  |                         Term Length                           |
 *  +---------------------------------------------------------------+
 *  |                          Page Size                            |
 *  +---------------------------------------------------------------+
 *  |                      Cache Line Padding                      ...
 * ...                                                              |
 *  +---------------------------------------------------------------+
 *  |                     Default Frame Header                     ...
 * ...                                                              |
 *  +---------------------------------------------------------------+
 * </pre>
 */
type LogBufferMetaData struct {
	flyweight.FWBase

	TailCounter             []flyweight.Int64Field // 0, 8, 16
	ActiveTermCountOff      flyweight.Int32Field   // 24
	EndOfStreamPosOff       flyweight.Int64Field   // 128
	IsConnectedFld          flyweight.Int32Field   // 136
	ActiveTransportCountOff flyweight.Int32Field   // 140
	CorrelationId           flyweight.Int64Field   // 256
	InitTermID              flyweight.Int32Field   // 264
	DefaultFrameHdrLen      flyweight.Int32Field   // 268
	MTULen                  flyweight.Int32Field   // 272
	TermLen                 flyweight.Int32Field   // 276
	PageSizeFld             flyweight.Int32Field   // 280
	DefaultFrameHeader      flyweight.RawDataField // 320

	buf    *atomic.Buffer
	offset int32
}

// Wrap wraps the metadata flyweight around buf at offset. Fields are wrapped at the offsets defined by
// io.aeron.logbuffer.LogBufferDescriptor (see the Log*Offset constants).
func (m *LogBufferMetaData) Wrap(buf *atomic.Buffer, offset int) flyweight.Flyweight {
	m.buf = buf
	m.offset = int32(offset)
	at := func(o int32) int { return offset + int(o) }
	m.TailCounter = make([]flyweight.Int64Field, PartitionCount)
	for i := int32(0); i < PartitionCount; i++ {
		m.TailCounter[i].Wrap(buf, at(tailOffset(i)))
	}
	m.ActiveTermCountOff.Wrap(buf, at(LogActiveTermCountOffset))
	m.EndOfStreamPosOff.Wrap(buf, at(LogEndOfStreamPositionOffset))
	m.IsConnectedFld.Wrap(buf, at(LogIsConnectedOffset))
	m.ActiveTransportCountOff.Wrap(buf, at(LogActiveTransportCount))
	m.CorrelationId.Wrap(buf, at(LogCorrelationIDOffset))
	m.InitTermID.Wrap(buf, at(LogInitialTermIDOffset))
	m.DefaultFrameHdrLen.Wrap(buf, at(LogDefaultFrameHeaderLengthOffset))
	m.MTULen.Wrap(buf, at(LogMtuLengthOffset))
	m.TermLen.Wrap(buf, at(LogTermLengthOffset))
	m.PageSizeFld.Wrap(buf, at(LogPageSizeOffset))
	m.DefaultFrameHeader.Wrap(buf, at(LogDefaultFrameHeaderOffset), DataFrameHeader.Length)

	m.SetSize(int(LogMetaDataLength))
	return m
}

// ActiveTransportCount returns the count of active transports for the Image.
func (m *LogBufferMetaData) ActiveTransportCount() int32 {
	return m.ActiveTransportCountOff.Get()
}

// CheckTermLength checks the term length is a power of two within [TermMinLength, TermMaxLength].
func CheckTermLength(termLength int32) error {
	if termLength < TermMinLength {
		return fmt.Errorf("term length less than min length of %d: length=%d", TermMinLength, termLength)
	}
	if termLength > TermMaxLength {
		return fmt.Errorf("term length more than max length of %d: length=%d", TermMaxLength, termLength)
	}
	if !util.IsPowerOfTwo(int64(termLength)) {
		return fmt.Errorf("term length not a power of 2: length=%d", termLength)
	}
	return nil
}

// CheckPageSize checks the page size is a power of two within [PageMinSize, PageMaxSize].
func CheckPageSize(pageSize int32) error {
	if pageSize < PageMinSize {
		return fmt.Errorf("page size less than min size of %d: page size=%d", PageMinSize, pageSize)
	}
	if pageSize > PageMaxSize {
		return fmt.Errorf("page size more than max size of %d: page size=%d", PageMaxSize, pageSize)
	}
	if !util.IsPowerOfTwo(int64(pageSize)) {
		return fmt.Errorf("page size not a power of 2: page size=%d", pageSize)
	}
	return nil
}

// TermID extracts the term id from a raw tail value.
func TermID(rawTail int64) int32 {
	return int32(rawTail >> 32)
}

// RotateLog rotates the log to the next term, initialising the tail of the next partition and incrementing the
// active term count. Returns true if this caller won the CAS on the active term count.
func RotateLog(logMetaDataBuffer *LogBufferMetaData, currentTermCount int32, currentTermId int32) bool {
	nextTermId := currentTermId + 1
	nextTermCount := currentTermCount + 1
	nextIndex := IndexByTermCount(int64(nextTermCount))
	expectedTermId := nextTermId - PartitionCount
	newRawTail := PackTail(nextTermId, 0)

	for {
		rawTail := logMetaDataBuffer.RawTailVolatile(nextIndex)
		if expectedTermId != TermID(rawTail) {
			break
		}
		if logMetaDataBuffer.CasRawTail(nextIndex, rawTail, newRawTail) {
			break
		}
	}
	return logMetaDataBuffer.CasActiveTermCount(currentTermCount, nextTermCount)
}
