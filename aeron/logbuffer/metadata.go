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
	"fmt"

	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/util"
)

// Log buffer types stored at LogTypeOffset.
const (
	LogBufferTypeConcurrentPublication int8 = 0
	LogBufferTypeExclusivePublication  int8 = 1
	LogBufferTypePublicationImage      int8 = 2
)

// LogDefaultFrameHeaderMaxLength is the space reserved in the metadata for the default frame header.
const LogDefaultFrameHeaderMaxLength int32 = util.CacheLineLength * 2

// Offsets of fields in the log metadata section, matching io.aeron.logbuffer.LogBufferDescriptor.
const (
	logPaddingSize int32 = util.CacheLineLength

	TermTailCountersOffset   int32 = 0
	LogActiveTermCountOffset int32 = TermTailCountersOffset + 8*PartitionCount

	LogEndOfStreamPositionOffset int32 = logPaddingSize * 2
	LogIsConnectedOffset         int32 = LogEndOfStreamPositionOffset + 8
	LogActiveTransportCount      int32 = LogIsConnectedOffset + 4

	LogCorrelationIDOffset               int32 = logPaddingSize * 4
	LogInitialTermIDOffset               int32 = LogCorrelationIDOffset + 8
	LogDefaultFrameHeaderLengthOffset    int32 = LogInitialTermIDOffset + 4
	LogMtuLengthOffset                   int32 = LogDefaultFrameHeaderLengthOffset + 4
	LogTermLengthOffset                  int32 = LogMtuLengthOffset + 4
	LogPageSizeOffset                    int32 = LogTermLengthOffset + 4
	LogPublicationWindowLengthOffset     int32 = LogPageSizeOffset + 4
	LogReceiverWindowLengthOffset        int32 = LogPublicationWindowLengthOffset + 4
	LogSocketSndbufLengthOffset          int32 = LogReceiverWindowLengthOffset + 4
	LogOsDefaultSocketSndbufLengthOffset int32 = LogSocketSndbufLengthOffset + 4
	LogOsMaxSocketSndbufLengthOffset     int32 = LogOsDefaultSocketSndbufLengthOffset + 4
	LogSocketRcvbufLengthOffset          int32 = LogOsMaxSocketSndbufLengthOffset + 4
	LogOsDefaultSocketRcvbufLengthOffset int32 = LogSocketRcvbufLengthOffset + 4
	LogOsMaxSocketRcvbufLengthOffset     int32 = LogOsDefaultSocketRcvbufLengthOffset + 4
	LogMaxResendOffset                   int32 = LogOsMaxSocketRcvbufLengthOffset + 4

	LogDefaultFrameHeaderOffset             int32 = logPaddingSize * 5
	LogEntityTagOffset                      int32 = LogDefaultFrameHeaderOffset + LogDefaultFrameHeaderMaxLength
	LogResponseCorrelationIDOffset          int32 = LogEntityTagOffset + 8
	LogLingerTimeoutNsOffset                int32 = LogResponseCorrelationIDOffset + 8
	LogUntetheredWindowLimitTimeoutNsOffset int32 = LogLingerTimeoutNsOffset + 8
	LogUntetheredRestingTimeoutNsOffset     int32 = LogUntetheredWindowLimitTimeoutNsOffset + 8
	LogGroupOffset                          int32 = LogUntetheredRestingTimeoutNsOffset + 8
	LogIsResponseOffset                     int32 = LogGroupOffset + 1
	LogRejoinOffset                         int32 = LogIsResponseOffset + 1
	LogReliableOffset                       int32 = LogRejoinOffset + 1
	LogSparseOffset                         int32 = LogReliableOffset + 1
	LogSignalEosOffset                      int32 = LogSparseOffset + 1
	LogSpiesSimulateConnectionOffset        int32 = LogSignalEosOffset + 1
	LogTetherOffset                         int32 = LogSpiesSimulateConnectionOffset + 1
	LogIsPublicationRevokedOffset           int32 = LogTetherOffset + 1
	LogTypeOffset                           int32 = LogIsPublicationRevokedOffset + 1
	LogUntetheredLingerTimeoutNsOffset      int32 = LogTypeOffset + 1 + 2
)

// Buffer returns the underlying metadata buffer.
func (m *LogBufferMetaData) Buffer() *atomic.Buffer { return m.buf }

func (m *LogBufferMetaData) i32(o int32) int32       { return m.buf.GetInt32(m.offset + o) }
func (m *LogBufferMetaData) setI32(o int32, v int32) { m.buf.PutInt32(m.offset+o, v) }
func (m *LogBufferMetaData) i64(o int32) int64       { return m.buf.GetInt64(m.offset + o) }
func (m *LogBufferMetaData) setI64(o int32, v int64) { m.buf.PutInt64(m.offset+o, v) }
func (m *LogBufferMetaData) b(o int32) bool          { return m.buf.GetUInt8(m.offset+o) == 1 }
func (m *LogBufferMetaData) setB(o int32, v bool) {
	var x uint8
	if v {
		x = 1
	}
	m.buf.PutUInt8(m.offset+o, x)
}

func tailOffset(partitionIndex int32) int32 { return TermTailCountersOffset + partitionIndex*8 }

// RawTail returns the raw tail (term id << 32 | term offset) of a partition.
func (m *LogBufferMetaData) RawTail(partitionIndex int32) int64 {
	return m.i64(tailOffset(partitionIndex))
}

// SetRawTail sets the raw tail of a partition.
func (m *LogBufferMetaData) SetRawTail(partitionIndex int32, rawTail int64) {
	m.setI64(tailOffset(partitionIndex), rawTail)
}

// RawTailVolatile returns the raw tail of a partition with volatile semantics.
func (m *LogBufferMetaData) RawTailVolatile(partitionIndex int32) int64 {
	return m.buf.GetInt64Volatile(m.offset + tailOffset(partitionIndex))
}

// SetRawTailVolatile sets the raw tail of a partition with ordered semantics.
func (m *LogBufferMetaData) SetRawTailVolatile(partitionIndex int32, rawTail int64) {
	m.buf.PutInt64Ordered(m.offset+tailOffset(partitionIndex), rawTail)
}

// CasRawTail compare-and-sets the raw tail of a partition.
func (m *LogBufferMetaData) CasRawTail(partitionIndex int32, expected, update int64) bool {
	return m.buf.CompareAndSetInt64(m.offset+tailOffset(partitionIndex), expected, update)
}

// ActiveRawTailVolatile returns the raw tail of the active partition.
func (m *LogBufferMetaData) ActiveRawTailVolatile() int64 {
	return m.RawTailVolatile(IndexByTermCount(int64(m.ActiveTermCount())))
}

// InitialiseTailWithTermID sets the tail of a partition to the start of the given term.
func (m *LogBufferMetaData) InitialiseTailWithTermID(partitionIndex int32, termID int32) {
	m.SetRawTail(partitionIndex, PackTail(termID, 0))
}

// ActiveTermCount returns the count of terms that have been used, with volatile semantics.
func (m *LogBufferMetaData) ActiveTermCount() int32 {
	return m.buf.GetInt32Volatile(m.offset + LogActiveTermCountOffset)
}

// SetActiveTermCountOrdered sets the active term count with ordered semantics.
func (m *LogBufferMetaData) SetActiveTermCountOrdered(termCount int32) {
	m.buf.PutInt32Ordered(m.offset+LogActiveTermCountOffset, termCount)
}

// CasActiveTermCount compare-and-sets the active term count.
func (m *LogBufferMetaData) CasActiveTermCount(expected, update int32) bool {
	return m.buf.CompareAndSetInt32(m.offset+LogActiveTermCountOffset, expected, update)
}

// EndOfStreamPosition returns the end of stream position, with volatile semantics.
func (m *LogBufferMetaData) EndOfStreamPosition() int64 {
	return m.buf.GetInt64Volatile(m.offset + LogEndOfStreamPositionOffset)
}

// SetEndOfStreamPosition sets the end of stream position with ordered semantics.
func (m *LogBufferMetaData) SetEndOfStreamPosition(position int64) {
	m.buf.PutInt64Ordered(m.offset+LogEndOfStreamPositionOffset, position)
}

// IsConnected returns true if the log has at least one connected subscriber (volatile read).
func (m *LogBufferMetaData) IsConnected() bool {
	return m.buf.GetInt32Volatile(m.offset+LogIsConnectedOffset) == 1
}

// SetIsConnected sets the connected flag with ordered semantics.
func (m *LogBufferMetaData) SetIsConnected(connected bool) {
	var v int32
	if connected {
		v = 1
	}
	m.buf.PutInt32Ordered(m.offset+LogIsConnectedOffset, v)
}

// SetActiveTransportCount sets the active transport count with ordered semantics.
func (m *LogBufferMetaData) SetActiveTransportCount(count int32) {
	m.buf.PutInt32Ordered(m.offset+LogActiveTransportCount, count)
}

func (m *LogBufferMetaData) CorrelationID() int64     { return m.i64(LogCorrelationIDOffset) }
func (m *LogBufferMetaData) SetCorrelationID(v int64) { m.setI64(LogCorrelationIDOffset, v) }
func (m *LogBufferMetaData) InitialTermID() int32     { return m.i32(LogInitialTermIDOffset) }
func (m *LogBufferMetaData) SetInitialTermID(v int32) { m.setI32(LogInitialTermIDOffset, v) }
func (m *LogBufferMetaData) DefaultFrameHeaderLength() int32 {
	return m.i32(LogDefaultFrameHeaderLengthOffset)
}
func (m *LogBufferMetaData) MtuLength() int32      { return m.i32(LogMtuLengthOffset) }
func (m *LogBufferMetaData) SetMtuLength(v int32)  { m.setI32(LogMtuLengthOffset, v) }
func (m *LogBufferMetaData) TermLength() int32     { return m.i32(LogTermLengthOffset) }
func (m *LogBufferMetaData) SetTermLength(v int32) { m.setI32(LogTermLengthOffset, v) }
func (m *LogBufferMetaData) PageSize() int32       { return m.i32(LogPageSizeOffset) }
func (m *LogBufferMetaData) SetPageSize(v int32)   { m.setI32(LogPageSizeOffset, v) }
func (m *LogBufferMetaData) PublicationWindowLength() int32 {
	return m.i32(LogPublicationWindowLengthOffset)
}
func (m *LogBufferMetaData) SetPublicationWindowLength(v int32) {
	m.setI32(LogPublicationWindowLengthOffset, v)
}
func (m *LogBufferMetaData) ReceiverWindowLength() int32 { return m.i32(LogReceiverWindowLengthOffset) }
func (m *LogBufferMetaData) SetReceiverWindowLength(v int32) {
	m.setI32(LogReceiverWindowLengthOffset, v)
}
func (m *LogBufferMetaData) SocketSndbufLength() int32     { return m.i32(LogSocketSndbufLengthOffset) }
func (m *LogBufferMetaData) SetSocketSndbufLength(v int32) { m.setI32(LogSocketSndbufLengthOffset, v) }
func (m *LogBufferMetaData) OsDefaultSocketSndbufLength() int32 {
	return m.i32(LogOsDefaultSocketSndbufLengthOffset)
}
func (m *LogBufferMetaData) SetOsDefaultSocketSndbufLength(v int32) {
	m.setI32(LogOsDefaultSocketSndbufLengthOffset, v)
}
func (m *LogBufferMetaData) OsMaxSocketSndbufLength() int32 {
	return m.i32(LogOsMaxSocketSndbufLengthOffset)
}
func (m *LogBufferMetaData) SetOsMaxSocketSndbufLength(v int32) {
	m.setI32(LogOsMaxSocketSndbufLengthOffset, v)
}
func (m *LogBufferMetaData) SocketRcvbufLength() int32     { return m.i32(LogSocketRcvbufLengthOffset) }
func (m *LogBufferMetaData) SetSocketRcvbufLength(v int32) { m.setI32(LogSocketRcvbufLengthOffset, v) }
func (m *LogBufferMetaData) OsDefaultSocketRcvbufLength() int32 {
	return m.i32(LogOsDefaultSocketRcvbufLengthOffset)
}
func (m *LogBufferMetaData) SetOsDefaultSocketRcvbufLength(v int32) {
	m.setI32(LogOsDefaultSocketRcvbufLengthOffset, v)
}
func (m *LogBufferMetaData) OsMaxSocketRcvbufLength() int32 {
	return m.i32(LogOsMaxSocketRcvbufLengthOffset)
}
func (m *LogBufferMetaData) SetOsMaxSocketRcvbufLength(v int32) {
	m.setI32(LogOsMaxSocketRcvbufLengthOffset, v)
}
func (m *LogBufferMetaData) MaxResend() int32     { return m.i32(LogMaxResendOffset) }
func (m *LogBufferMetaData) SetMaxResend(v int32) { m.setI32(LogMaxResendOffset, v) }
func (m *LogBufferMetaData) EntityTag() int64     { return m.i64(LogEntityTagOffset) }
func (m *LogBufferMetaData) SetEntityTag(v int64) { m.setI64(LogEntityTagOffset, v) }
func (m *LogBufferMetaData) ResponseCorrelationID() int64 {
	return m.i64(LogResponseCorrelationIDOffset)
}
func (m *LogBufferMetaData) SetResponseCorrelationID(v int64) {
	m.setI64(LogResponseCorrelationIDOffset, v)
}
func (m *LogBufferMetaData) LingerTimeoutNs() int64     { return m.i64(LogLingerTimeoutNsOffset) }
func (m *LogBufferMetaData) SetLingerTimeoutNs(v int64) { m.setI64(LogLingerTimeoutNsOffset, v) }
func (m *LogBufferMetaData) UntetheredWindowLimitTimeoutNs() int64 {
	return m.i64(LogUntetheredWindowLimitTimeoutNsOffset)
}
func (m *LogBufferMetaData) SetUntetheredWindowLimitTimeoutNs(v int64) {
	m.setI64(LogUntetheredWindowLimitTimeoutNsOffset, v)
}
func (m *LogBufferMetaData) UntetheredRestingTimeoutNs() int64 {
	return m.i64(LogUntetheredRestingTimeoutNsOffset)
}
func (m *LogBufferMetaData) SetUntetheredRestingTimeoutNs(v int64) {
	m.setI64(LogUntetheredRestingTimeoutNsOffset, v)
}

// UntetheredLingerTimeoutNs is not 8 byte aligned in the Java layout, so it is read byte-wise.
func (m *LogBufferMetaData) UntetheredLingerTimeoutNs() int64 {
	b := m.buf.GetBytesArray(m.offset+LogUntetheredLingerTimeoutNsOffset, 8)
	var v uint64
	for i := 7; i >= 0; i-- {
		v = v<<8 | uint64(b[i])
	}
	return int64(v)
}

func (m *LogBufferMetaData) SetUntetheredLingerTimeoutNs(v int64) {
	b := make([]byte, 8)
	for i := 0; i < 8; i++ {
		b[i] = byte(uint64(v) >> (8 * i))
	}
	m.buf.PutBytesArray(m.offset+LogUntetheredLingerTimeoutNsOffset, &b, 0, 8)
}

func (m *LogBufferMetaData) Group() bool          { return m.b(LogGroupOffset) }
func (m *LogBufferMetaData) SetGroup(v bool)      { m.setB(LogGroupOffset, v) }
func (m *LogBufferMetaData) IsResponse() bool     { return m.b(LogIsResponseOffset) }
func (m *LogBufferMetaData) SetIsResponse(v bool) { m.setB(LogIsResponseOffset, v) }
func (m *LogBufferMetaData) Rejoin() bool         { return m.b(LogRejoinOffset) }
func (m *LogBufferMetaData) SetRejoin(v bool)     { m.setB(LogRejoinOffset, v) }
func (m *LogBufferMetaData) Reliable() bool       { return m.b(LogReliableOffset) }
func (m *LogBufferMetaData) SetReliable(v bool)   { m.setB(LogReliableOffset, v) }
func (m *LogBufferMetaData) Sparse() bool         { return m.b(LogSparseOffset) }
func (m *LogBufferMetaData) SetSparse(v bool)     { m.setB(LogSparseOffset, v) }
func (m *LogBufferMetaData) SignalEos() bool      { return m.b(LogSignalEosOffset) }
func (m *LogBufferMetaData) SetSignalEos(v bool)  { m.setB(LogSignalEosOffset, v) }
func (m *LogBufferMetaData) SpiesSimulateConnection() bool {
	return m.b(LogSpiesSimulateConnectionOffset)
}
func (m *LogBufferMetaData) SetSpiesSimulateConnection(v bool) {
	m.setB(LogSpiesSimulateConnectionOffset, v)
}
func (m *LogBufferMetaData) Tether() bool     { return m.b(LogTetherOffset) }
func (m *LogBufferMetaData) SetTether(v bool) { m.setB(LogTetherOffset, v) }

// IsPublicationRevoked returns true if the publication was revoked
func (m *LogBufferMetaData) IsPublicationRevoked() bool {
	return m.b(LogIsPublicationRevokedOffset)
}

// SetIsPublicationRevoked sets the revoked flag.
func (m *LogBufferMetaData) SetIsPublicationRevoked(v bool) { m.setB(LogIsPublicationRevokedOffset, v) }

// LogType returns one of the LogBufferType* values.
func (m *LogBufferMetaData) LogType() int8     { return int8(m.buf.GetUInt8(m.offset + LogTypeOffset)) }
func (m *LogBufferMetaData) SetLogType(v int8) { m.buf.PutInt8(m.offset+LogTypeOffset, v) }

// DefaultFrameHeaderBuffer returns a copy of the default frame header stored in the metadata.
func (m *LogBufferMetaData) DefaultFrameHeaderBuffer() *atomic.Buffer {
	b := make([]byte, DataFrameHeader.Length)
	m.buf.GetBytes(m.offset+LogDefaultFrameHeaderOffset, b)
	return atomic.MakeBuffer(b)
}

// StoreDefaultFrameHeader stores defaultHeader (which must be DataFrameHeader.Length long) in the metadata.
func (m *LogBufferMetaData) StoreDefaultFrameHeader(defaultHeader *atomic.Buffer) error {
	if defaultHeader.Capacity() != DataFrameHeader.Length {
		return fmt.Errorf("default header length not equal to HEADER_LENGTH: length=%d", defaultHeader.Capacity())
	}
	m.setI32(LogDefaultFrameHeaderLengthOffset, DataFrameHeader.Length)
	m.buf.PutBytes(m.offset+LogDefaultFrameHeaderOffset, defaultHeader, 0, DataFrameHeader.Length)
	return nil
}

// ApplyDefaultHeader copies the default frame header into termBuffer at termOffset.
func (m *LogBufferMetaData) ApplyDefaultHeader(termBuffer *atomic.Buffer, termOffset int32) {
	termBuffer.PutBytes(termOffset, m.buf, m.offset+LogDefaultFrameHeaderOffset, DataFrameHeader.Length)
}
