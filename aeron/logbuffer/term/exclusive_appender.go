/*
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

// ExclusiveAppender appends frames to one term partition of an exclusive (single writer) publication. The caller
// tracks the term id and offset; the tail counter is published with an ordered store before the frame is written so
// that the driver and readers observe the reservation, as in Java ExclusivePublication.
//
// All append methods return the resulting term offset after the append, or AppenderTripped (-1) when the term was
// padded to its end and the caller must rotate the log.
type ExclusiveAppender struct {
	termBuffer   *atomic.Buffer
	metaBuffer   *atomic.Buffer
	tailOffset   int32
	headerWriter headerWriter
}

// MakeExclusiveAppender creates an ExclusiveAppender for the given term partition.
func MakeExclusiveAppender(logBuffers *logbuffer.LogBuffers, partitionIndex int) *ExclusiveAppender {
	appender := new(ExclusiveAppender)
	appender.termBuffer = logBuffers.Buffer(partitionIndex)
	appender.metaBuffer = logBuffers.Buffer(logbuffer.LogMetaDataSectionIndex)
	appender.tailOffset = int32(partitionIndex * 8)
	appender.headerWriter.fill(logBuffers.Meta().DefaultFrameHeader.Get())
	return appender
}

// TermBuffer returns the term partition this appender writes to.
func (appender *ExclusiveAppender) TermBuffer() *atomic.Buffer {
	return appender.termBuffer
}

// RawTail is a volatile read of the raw tail counter of this partition.
func (appender *ExclusiveAppender) RawTail() int64 {
	return appender.metaBuffer.GetInt64Volatile(appender.tailOffset)
}

func (appender *ExclusiveAppender) putRawTailOrdered(termID int32, termOffset int32) {
	appender.metaBuffer.PutInt64Ordered(appender.tailOffset, int64(termID)<<32|int64(uint32(termOffset)))
}

func (appender *ExclusiveAppender) reserve(termID, termOffset, alignedLength int32) (int32, bool) {
	termLength := appender.termBuffer.Capacity()
	resultingOffset := int64(termOffset) + int64(alignedLength)
	appender.putRawTailOrdered(termID, int32(resultingOffset))
	if resultingOffset > int64(termLength) {
		handleEndOfLogCondition(termID, appender.termBuffer, int64(termOffset), &appender.headerWriter, termLength)
		return int32(AppenderTripped), false
	}
	return int32(resultingOffset), true
}

// Claim reserves a frame for length bytes of payload and wraps claim around it.
func (appender *ExclusiveAppender) Claim(termID, termOffset, length int32, claim *logbuffer.Claim) int32 {
	frameLength := length + logbuffer.DataFrameHeader.Length
	alignedLength := util.AlignInt32(frameLength, logbuffer.FrameAlignment)
	resultingOffset, ok := appender.reserve(termID, termOffset, alignedLength)
	if ok {
		appender.headerWriter.write(appender.termBuffer, termOffset, frameLength, termID)
		claim.Wrap(appender.termBuffer, termOffset, frameLength)
	}
	return resultingOffset
}

// AppendPadding appends a padding frame whose payload is length bytes.
func (appender *ExclusiveAppender) AppendPadding(termID, termOffset, length int32) int32 {
	frameLength := length + logbuffer.DataFrameHeader.Length
	alignedLength := util.AlignInt32(frameLength, logbuffer.FrameAlignment)
	resultingOffset, ok := appender.reserve(termID, termOffset, alignedLength)
	if ok {
		appender.headerWriter.write(appender.termBuffer, termOffset, frameLength, termID)
		logbuffer.SetFrameType(appender.termBuffer, termOffset, logbuffer.DataFrameHeader.TypePad)
		appender.termBuffer.PutInt32Ordered(termOffset, frameLength)
	}
	return resultingOffset
}

// AppendUnfragmentedMessage appends a message in a single frame.
func (appender *ExclusiveAppender) AppendUnfragmentedMessage(termID, termOffset int32, src *atomic.Buffer,
	srcOffset, length int32, supplier ReservedValueSupplier) int32 {
	frameLength := length + logbuffer.DataFrameHeader.Length
	alignedLength := util.AlignInt32(frameLength, logbuffer.FrameAlignment)
	resultingOffset, ok := appender.reserve(termID, termOffset, alignedLength)
	if ok {
		appender.headerWriter.write(appender.termBuffer, termOffset, frameLength, termID)
		appender.termBuffer.PutBytes(termOffset+logbuffer.DataFrameHeader.Length, src, srcOffset, length)
		putReservedValue(appender.termBuffer, termOffset, frameLength, supplier)
		appender.termBuffer.PutInt32Ordered(termOffset, frameLength)
	}
	return resultingOffset
}

// AppendUnfragmentedMessage2 appends two buffers as a message in a single frame.
func (appender *ExclusiveAppender) AppendUnfragmentedMessage2(termID, termOffset int32,
	srcOne *atomic.Buffer, offsetOne, lengthOne int32, srcTwo *atomic.Buffer, offsetTwo, lengthTwo int32,
	supplier ReservedValueSupplier) int32 {
	frameLength := lengthOne + lengthTwo + logbuffer.DataFrameHeader.Length
	alignedLength := util.AlignInt32(frameLength, logbuffer.FrameAlignment)
	resultingOffset, ok := appender.reserve(termID, termOffset, alignedLength)
	if ok {
		dataOffset := termOffset + logbuffer.DataFrameHeader.Length
		appender.headerWriter.write(appender.termBuffer, termOffset, frameLength, termID)
		appender.termBuffer.PutBytes(dataOffset, srcOne, offsetOne, lengthOne)
		appender.termBuffer.PutBytes(dataOffset+lengthOne, srcTwo, offsetTwo, lengthTwo)
		putReservedValue(appender.termBuffer, termOffset, frameLength, supplier)
		appender.termBuffer.PutInt32Ordered(termOffset, frameLength)
	}
	return resultingOffset
}

// AppendUnfragmentedMessageVectors appends the concatenation of vectors (total length bytes) in a single frame.
func (appender *ExclusiveAppender) AppendUnfragmentedMessageVectors(termID, termOffset int32,
	vectors []DirectBufferVector, length int32, supplier ReservedValueSupplier) int32 {
	frameLength := length + logbuffer.DataFrameHeader.Length
	alignedLength := util.AlignInt32(frameLength, logbuffer.FrameAlignment)
	resultingOffset, ok := appender.reserve(termID, termOffset, alignedLength)
	if ok {
		appender.headerWriter.write(appender.termBuffer, termOffset, frameLength, termID)
		writeVectors(appender.termBuffer, termOffset+logbuffer.DataFrameHeader.Length, vectors)
		putReservedValue(appender.termBuffer, termOffset, frameLength, supplier)
		appender.termBuffer.PutInt32Ordered(termOffset, frameLength)
	}
	return resultingOffset
}

// AppendFragmentedMessage appends a message larger than maxPayloadLength as a batch of fragments.
func (appender *ExclusiveAppender) AppendFragmentedMessage(termID, termOffset int32, src *atomic.Buffer,
	srcOffset, length, maxPayloadLength int32, supplier ReservedValueSupplier) int32 {
	framedLength := ComputeFragmentedFrameLength(length, maxPayloadLength)
	resultingOffset, ok := appender.reserve(termID, termOffset, framedLength)
	if ok {
		fragmentSingleSource(appender.termBuffer, &appender.headerWriter, termOffset, termID,
			src, srcOffset, length, maxPayloadLength, supplier)
	}
	return resultingOffset
}

// AppendFragmentedMessage2 appends two buffers (combined length larger than maxPayloadLength) as fragments.
func (appender *ExclusiveAppender) AppendFragmentedMessage2(termID, termOffset int32,
	srcOne *atomic.Buffer, offsetOne, lengthOne int32, srcTwo *atomic.Buffer, offsetTwo, lengthTwo int32,
	maxPayloadLength int32, supplier ReservedValueSupplier) int32 {
	framedLength := ComputeFragmentedFrameLength(lengthOne+lengthTwo, maxPayloadLength)
	resultingOffset, ok := appender.reserve(termID, termOffset, framedLength)
	if ok {
		fragmentTwoSources(appender.termBuffer, &appender.headerWriter, termOffset, termID,
			srcOne, offsetOne, lengthOne, srcTwo, offsetTwo, lengthTwo, maxPayloadLength, supplier)
	}
	return resultingOffset
}

// AppendFragmentedMessageVectors appends the concatenation of vectors as a batch of fragments.
func (appender *ExclusiveAppender) AppendFragmentedMessageVectors(termID, termOffset int32,
	vectors []DirectBufferVector, length, maxPayloadLength int32, supplier ReservedValueSupplier) int32 {
	framedLength := ComputeFragmentedFrameLength(length, maxPayloadLength)
	resultingOffset, ok := appender.reserve(termID, termOffset, framedLength)
	if ok {
		fragmentVectors(appender.termBuffer, &appender.headerWriter, termOffset, termID,
			vectors, length, maxPayloadLength, supplier)
	}
	return resultingOffset
}

// AppendBlock copies a block of pre-formatted, committed frames into the term. The first frame's length is written
// last with an ordered store so the block becomes visible atomically.
func (appender *ExclusiveAppender) AppendBlock(termID, termOffset int32, src *atomic.Buffer,
	srcOffset, length int32) int32 {
	resultingOffset := termOffset + length
	hdrLength := logbuffer.DataFrameHeader.Length
	term := appender.termBuffer

	appender.putRawTailOrdered(termID, resultingOffset)
	term.PutBytes(termOffset+hdrLength, src, srcOffset+hdrLength, length-hdrLength)
	term.PutInt64(termOffset+24, src.GetInt64(srcOffset+24))
	term.PutInt64(termOffset+16, src.GetInt64(srcOffset+16))
	term.PutInt64(termOffset+8, src.GetInt64(srcOffset+8))
	term.PutInt64Ordered(termOffset, src.GetInt64(srcOffset))
	return resultingOffset
}
