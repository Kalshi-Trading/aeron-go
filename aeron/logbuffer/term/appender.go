/*
Copyright 2016 Stanislav Liberman

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

package term

import (
	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/logbuffer"
	"github.com/lirm/aeron-go/aeron/util"
)

const (
	// AppenderTripped is returned when the end of the term has been reached and the term was padded to its end.
	// The caller should rotate the log and retry.
	AppenderTripped int64 = -1

	// AppenderFailed is retained for API compatibility. Appenders no longer return it: an append that starts
	// beyond the end of the term (another writer already tripped it) also reports AppenderTripped.
	AppenderFailed int64 = -2

	beginFrag    uint8 = 0x80
	endFrag      uint8 = 0x40
	unfragmented uint8 = 0x80 | 0x40
)

// DefaultReservedValueSupplier is a ReservedValueSupplier that always returns 0. Passing nil as a
// ReservedValueSupplier is equivalent and avoids the call; the reserved value is then left as zero.
var DefaultReservedValueSupplier ReservedValueSupplier = func(termBuffer *atomic.Buffer, termOffset int32, length int32) int64 {
	return 0
}

// ReservedValueSupplier is called after a frame payload has been copied into the term but before the frame is
// committed, to supply the 64-bit reserved value of the data frame header (io.aeron.ReservedValueSupplier).
type ReservedValueSupplier func(termBuffer *atomic.Buffer, termOffset int32, length int32) int64

// Appender appends frames to one term partition of a concurrent publication's log buffer. The tail counter is
// advanced with an atomic get-and-add so that it is safe to use from multiple goroutines (and processes).
type Appender struct {
	termBuffer   *atomic.Buffer
	metaBuffer   *atomic.Buffer
	tailOffset   int32
	headerWriter headerWriter
}

// MakeAppender is the factory function for term Appenders
func MakeAppender(logBuffers *logbuffer.LogBuffers, partitionIndex int) *Appender {
	appender := new(Appender)
	appender.termBuffer = logBuffers.Buffer(partitionIndex)
	appender.metaBuffer = logBuffers.Buffer(logbuffer.LogMetaDataSectionIndex)
	appender.tailOffset = int32(partitionIndex * 8)
	appender.headerWriter.fill(logBuffers.Meta().DefaultFrameHeader.Get())
	return appender
}

// TermBuffer returns the term partition this appender writes to.
func (appender *Appender) TermBuffer() *atomic.Buffer {
	return appender.termBuffer
}

// RawTail is a volatile read of the raw value of the tail counter (term id in the upper 32 bits, offset in lower).
func (appender *Appender) RawTail() int64 {
	return appender.metaBuffer.GetInt64Volatile(appender.tailOffset)
}

// SetRawTail sets the raw value of the tail. It should not be used outside of testing
func (appender *Appender) SetRawTail(v int64) {
	appender.metaBuffer.PutInt64Ordered(appender.tailOffset, v)
}

// SetTailTermID sets the tail to the start of the given term. It should not be used outside of testing.
func (appender *Appender) SetTailTermID(termID int32) {
	appender.SetRawTail(int64(termID) << 32)
}

func (appender *Appender) getAndAddRawTail(alignedLength int32) int64 {
	return appender.metaBuffer.GetAndAddInt64(appender.tailOffset, int64(alignedLength))
}

// Claim reserves length bytes of payload in the term and wraps claim around the frame for zero copy publication.
// The frame must be completed with claim.Commit() or claim.Abort().
func (appender *Appender) Claim(length int32, claim *logbuffer.Claim) (resultingOffset int64, termID int32) {
	frameLength := length + logbuffer.DataFrameHeader.Length
	alignedLength := util.AlignInt32(frameLength, logbuffer.FrameAlignment)
	rawTail := appender.getAndAddRawTail(alignedLength)
	termLength := appender.termBuffer.Capacity()

	termID = logbuffer.TermID(rawTail)
	termOffset := rawTail & 0xFFFFFFFF
	resultingOffset = termOffset + int64(alignedLength)
	if resultingOffset > int64(termLength) {
		return handleEndOfLogCondition(termID, appender.termBuffer, termOffset, &appender.headerWriter, termLength), termID
	}

	offset := int32(termOffset)
	appender.headerWriter.write(appender.termBuffer, offset, frameLength, termID)
	claim.Wrap(appender.termBuffer, offset, frameLength)
	return resultingOffset, termID
}

// AppendUnfragmentedMessage appends an unfragmented message in a single frame to the term
func (appender *Appender) AppendUnfragmentedMessage(srcBuffer *atomic.Buffer, srcOffset int32, length int32,
	reservedValueSupplier ReservedValueSupplier) (resultingOffset int64, termID int32) {

	frameLength := length + logbuffer.DataFrameHeader.Length
	alignedLength := util.AlignInt32(frameLength, logbuffer.FrameAlignment)
	rawTail := appender.getAndAddRawTail(alignedLength)
	termLength := appender.termBuffer.Capacity()

	termID = logbuffer.TermID(rawTail)
	termOffset := rawTail & 0xFFFFFFFF
	resultingOffset = termOffset + int64(alignedLength)
	if resultingOffset > int64(termLength) {
		return handleEndOfLogCondition(termID, appender.termBuffer, termOffset, &appender.headerWriter, termLength), termID
	}

	offset := int32(termOffset)
	appender.headerWriter.write(appender.termBuffer, offset, frameLength, termID)
	appender.termBuffer.PutBytes(offset+logbuffer.DataFrameHeader.Length, srcBuffer, srcOffset, length)
	putReservedValue(appender.termBuffer, offset, frameLength, reservedValueSupplier)
	appender.termBuffer.PutInt32Ordered(offset, frameLength)
	return resultingOffset, termID
}

// AppendUnfragmentedMessage2 appends the given pair of buffers as an unfragmented message in a single frame to the term
func (appender *Appender) AppendUnfragmentedMessage2(
	srcBufferOne *atomic.Buffer, srcOffsetOne int32, lengthOne int32,
	srcBufferTwo *atomic.Buffer, srcOffsetTwo int32, lengthTwo int32,
	reservedValueSupplier ReservedValueSupplier,
) (resultingOffset int64, termID int32) {

	frameLength := lengthOne + lengthTwo + logbuffer.DataFrameHeader.Length
	alignedLength := util.AlignInt32(frameLength, logbuffer.FrameAlignment)
	rawTail := appender.getAndAddRawTail(alignedLength)
	termLength := appender.termBuffer.Capacity()

	termID = logbuffer.TermID(rawTail)
	termOffset := rawTail & 0xFFFFFFFF
	resultingOffset = termOffset + int64(alignedLength)
	if resultingOffset > int64(termLength) {
		return handleEndOfLogCondition(termID, appender.termBuffer, termOffset, &appender.headerWriter, termLength), termID
	}

	offset := int32(termOffset)
	dataOffset := offset + logbuffer.DataFrameHeader.Length
	appender.headerWriter.write(appender.termBuffer, offset, frameLength, termID)
	appender.termBuffer.PutBytes(dataOffset, srcBufferOne, srcOffsetOne, lengthOne)
	appender.termBuffer.PutBytes(dataOffset+lengthOne, srcBufferTwo, srcOffsetTwo, lengthTwo)
	putReservedValue(appender.termBuffer, offset, frameLength, reservedValueSupplier)
	appender.termBuffer.PutInt32Ordered(offset, frameLength)
	return resultingOffset, termID
}

// AppendUnfragmentedMessageVectors appends the concatenation of vectors (of total length) in a single frame.
func (appender *Appender) AppendUnfragmentedMessageVectors(vectors []DirectBufferVector, length int32,
	reservedValueSupplier ReservedValueSupplier) (resultingOffset int64, termID int32) {

	frameLength := length + logbuffer.DataFrameHeader.Length
	alignedLength := util.AlignInt32(frameLength, logbuffer.FrameAlignment)
	rawTail := appender.getAndAddRawTail(alignedLength)
	termLength := appender.termBuffer.Capacity()

	termID = logbuffer.TermID(rawTail)
	termOffset := rawTail & 0xFFFFFFFF
	resultingOffset = termOffset + int64(alignedLength)
	if resultingOffset > int64(termLength) {
		return handleEndOfLogCondition(termID, appender.termBuffer, termOffset, &appender.headerWriter, termLength), termID
	}

	offset := int32(termOffset)
	appender.headerWriter.write(appender.termBuffer, offset, frameLength, termID)
	writeVectors(appender.termBuffer, offset+logbuffer.DataFrameHeader.Length, vectors)
	putReservedValue(appender.termBuffer, offset, frameLength, reservedValueSupplier)
	appender.termBuffer.PutInt32Ordered(offset, frameLength)
	return resultingOffset, termID
}

// AppendFragmentedMessage appends a message greater than frame length as a batch of fragments
func (appender *Appender) AppendFragmentedMessage(srcBuffer *atomic.Buffer, srcOffset int32, length int32,
	maxPayloadLength int32, reservedValueSupplier ReservedValueSupplier) (resultingOffset int64, termID int32) {

	framedLength := ComputeFragmentedFrameLength(length, maxPayloadLength)
	rawTail := appender.getAndAddRawTail(framedLength)
	termLength := appender.termBuffer.Capacity()

	termID = logbuffer.TermID(rawTail)
	termOffset := rawTail & 0xFFFFFFFF
	resultingOffset = termOffset + int64(framedLength)
	if resultingOffset > int64(termLength) {
		return handleEndOfLogCondition(termID, appender.termBuffer, termOffset, &appender.headerWriter, termLength), termID
	}

	fragmentSingleSource(appender.termBuffer, &appender.headerWriter, int32(termOffset), termID,
		srcBuffer, srcOffset, length, maxPayloadLength, reservedValueSupplier)
	return resultingOffset, termID
}

// AppendFragmentedMessage2 appends the given pair of buffers (with combined length greater than max frame length)
// as a batch of fragments
func (appender *Appender) AppendFragmentedMessage2(
	srcBufferOne *atomic.Buffer, srcOffsetOne int32, lengthOne int32,
	srcBufferTwo *atomic.Buffer, srcOffsetTwo int32, lengthTwo int32,
	maxPayloadLength int32, reservedValueSupplier ReservedValueSupplier,
) (resultingOffset int64, termID int32) {
	length := lengthOne + lengthTwo
	framedLength := ComputeFragmentedFrameLength(length, maxPayloadLength)
	rawTail := appender.getAndAddRawTail(framedLength)
	termLength := appender.termBuffer.Capacity()

	termID = logbuffer.TermID(rawTail)
	termOffset := rawTail & 0xFFFFFFFF
	resultingOffset = termOffset + int64(framedLength)
	if resultingOffset > int64(termLength) {
		return handleEndOfLogCondition(termID, appender.termBuffer, termOffset, &appender.headerWriter, termLength), termID
	}

	fragmentTwoSources(appender.termBuffer, &appender.headerWriter, int32(termOffset), termID,
		srcBufferOne, srcOffsetOne, lengthOne, srcBufferTwo, srcOffsetTwo, lengthTwo,
		maxPayloadLength, reservedValueSupplier)
	return resultingOffset, termID
}

// AppendFragmentedMessageVectors appends the concatenation of vectors (of total length greater than the max payload
// length) as a batch of fragments.
func (appender *Appender) AppendFragmentedMessageVectors(vectors []DirectBufferVector, length int32,
	maxPayloadLength int32, reservedValueSupplier ReservedValueSupplier) (resultingOffset int64, termID int32) {

	framedLength := ComputeFragmentedFrameLength(length, maxPayloadLength)
	rawTail := appender.getAndAddRawTail(framedLength)
	termLength := appender.termBuffer.Capacity()

	termID = logbuffer.TermID(rawTail)
	termOffset := rawTail & 0xFFFFFFFF
	resultingOffset = termOffset + int64(framedLength)
	if resultingOffset > int64(termLength) {
		return handleEndOfLogCondition(termID, appender.termBuffer, termOffset, &appender.headerWriter, termLength), termID
	}

	fragmentVectors(appender.termBuffer, &appender.headerWriter, int32(termOffset), termID,
		vectors, length, maxPayloadLength, reservedValueSupplier)
	return resultingOffset, termID
}

// handleEndOfLogCondition pads the remainder of the term (if this writer was the one to cross its end) and reports
// AppenderTripped so the publication rotates the log.
func handleEndOfLogCondition(termID int32, termBuffer *atomic.Buffer, termOffset int64, header *headerWriter,
	termLength int32) int64 {
	if termOffset < int64(termLength) {
		offset := int32(termOffset)
		paddingLength := termLength - offset
		header.write(termBuffer, offset, paddingLength, termID)
		logbuffer.SetFrameType(termBuffer, offset, logbuffer.DataFrameHeader.TypePad)
		termBuffer.PutInt32Ordered(offset, paddingLength)
	}
	return AppenderTripped
}

func minInt32(v1, v2 int32) int32 {
	if v1 < v2 {
		return v1
	}
	return v2
}
