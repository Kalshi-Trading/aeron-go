/*
Copyright 2016-2018 Stanislav Liberman
Copyright 2022 Steven Stern

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

package aeron

import (
	"fmt"

	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/logbuffer"
	"github.com/lirm/aeron-go/aeron/logbuffer/term"
)

// ExclusivePublication is a publication for a single writer (io.aeron.ExclusivePublication). It is not safe for
// concurrent use; in exchange it avoids atomic read-modify-write on the tail and supports OfferBlock and
// AppendPadding. Its initial position may be set with the channel parameters init-term-id, term-id and
// term-offset (see ChannelUriStringBuilder.InitialPosition).
type ExclusivePublication struct {
	publicationBase

	termOffset           int32
	termID               int32
	activePartitionIndex int32
	termBeginPosition    int64

	appenders [logbuffer.PartitionCount]*term.ExclusiveAppender
}

// NewExclusivePublication creates an ExclusivePublication over the log buffers, starting at the position recorded
// in the log's active term tail.
func NewExclusivePublication(logBuffers *logbuffer.LogBuffers) *ExclusivePublication {
	pub := new(ExclusivePublication)
	pub.init(logBuffers)
	for i := 0; i < logbuffer.PartitionCount; i++ {
		pub.appenders[i] = term.MakeExclusiveAppender(logBuffers, i)
	}

	termCount := pub.activeTermCount()
	index := termCount % logbuffer.PartitionCount
	rawTail := pub.rawTailVolatile(index)
	pub.activePartitionIndex = index
	pub.termID = logbuffer.TermID(rawTail)
	pub.termOffset = int32(rawTail & 0xFFFFFFFF)
	pub.termBeginPosition = computeTermBeginPosition(pub.termID, pub.positionBitsToShift, pub.initialTermID)
	return pub
}

// TermID returns the term id the publication is currently writing to.
func (pub *ExclusivePublication) TermID() int32 {
	return pub.termID
}

// TermOffset returns the offset in the current term at which the next message will be written.
func (pub *ExclusivePublication) TermOffset() int32 {
	return pub.termOffset
}

// Position returns the position the publication has reached, or PublicationClosed.
func (pub *ExclusivePublication) Position() int64 {
	if pub.IsClosed() {
		return PublicationClosed
	}
	return pub.termBeginPosition + int64(pub.termOffset)
}

// AvailableWindow returns the number of bytes that can be offered before back pressure, or PublicationClosed.
func (pub *ExclusivePublication) AvailableWindow() int64 {
	if pub.IsClosed() {
		return PublicationClosed
	}
	return pub.pubLimit.get() - (pub.termBeginPosition + int64(pub.termOffset))
}

// RevokeOnClose marks the publication to be revoked when Close is called. See Revoke.
func (pub *ExclusivePublication) RevokeOnClose() {
	pub.revokeOnClose = true
}

// Revoke immediately revokes and closes the publication. The driver disposes of the log without lingering and
// subscriber images go unavailable without draining, so use it only when subscribers have all the data they need.
func (pub *ExclusivePublication) Revoke() error {
	if pub.IsClosed() {
		return nil
	}
	pub.revokeOnClose = true
	return pub.Close()
}

func (pub *ExclusivePublication) newPosition(resultingOffset int32) int64 {
	if resultingOffset > 0 {
		pub.termOffset = resultingOffset
		return pub.termBeginPosition + int64(resultingOffset)
	}
	if pub.termBeginPosition+int64(pub.termBufferLength) >= pub.maxPossiblePosition {
		return MaxPositionExceeded
	}
	pub.rotateTerm()
	return AdminAction
}

func (pub *ExclusivePublication) rotateTerm() {
	nextTermID := pub.termID + 1
	nextTermCount := nextTermID - pub.initialTermID
	nextIndex := nextPartitionIndex(pub.activePartitionIndex)

	pub.activePartitionIndex = nextIndex
	pub.termOffset = 0
	pub.termID = nextTermID
	pub.termBeginPosition += int64(pub.termBufferLength)

	pub.logMetaData.PutInt64(termTailCountersOffset+nextIndex*8, int64(nextTermID)<<32)
	pub.logMetaData.PutInt32Ordered(logActiveTermCountOffset, nextTermCount)
}

// Offer a message of length bytes from buffer at offset, fragmenting if larger than MaxPayloadLength.
// See Publication.Offer for result codes.
func (pub *ExclusivePublication) Offer(buffer *atomic.Buffer, offset int32, length int32,
	reservedValueSupplier term.ReservedValueSupplier) int64 {
	if pub.IsClosed() {
		return PublicationClosed
	}
	limit := pub.pubLimit.get()
	position := pub.termBeginPosition + int64(pub.termOffset)
	if position >= limit {
		return pub.backPressureStatus(position, length)
	}

	appender := pub.appenders[pub.activePartitionIndex]
	var result int32
	if length <= pub.maxPayloadLength {
		checkPositiveLength(length)
		result = appender.AppendUnfragmentedMessage(pub.termID, pub.termOffset, buffer, offset, length,
			reservedValueSupplier)
	} else {
		pub.checkForMaxMessageLength(length)
		result = appender.AppendFragmentedMessage(pub.termID, pub.termOffset, buffer, offset, length,
			pub.maxPayloadLength, reservedValueSupplier)
	}
	return pub.newPosition(result)
}

// Offer2 offers the concatenation of two buffers as one message.
func (pub *ExclusivePublication) Offer2(
	bufferOne *atomic.Buffer, offsetOne int32, lengthOne int32,
	bufferTwo *atomic.Buffer, offsetTwo int32, lengthTwo int32,
	reservedValueSupplier term.ReservedValueSupplier,
) int64 {
	if pub.IsClosed() {
		return PublicationClosed
	}
	length := validateAndComputeLength(lengthOne, lengthTwo)
	limit := pub.pubLimit.get()
	position := pub.termBeginPosition + int64(pub.termOffset)
	if position >= limit {
		return pub.backPressureStatus(position, length)
	}

	appender := pub.appenders[pub.activePartitionIndex]
	var result int32
	if length <= pub.maxPayloadLength {
		result = appender.AppendUnfragmentedMessage2(pub.termID, pub.termOffset,
			bufferOne, offsetOne, lengthOne, bufferTwo, offsetTwo, lengthTwo, reservedValueSupplier)
	} else {
		pub.checkForMaxMessageLength(length)
		result = appender.AppendFragmentedMessage2(pub.termID, pub.termOffset,
			bufferOne, offsetOne, lengthOne, bufferTwo, offsetTwo, lengthTwo, pub.maxPayloadLength,
			reservedValueSupplier)
	}
	return pub.newPosition(result)
}

// OfferVectors offers the concatenation of vectors as one message.
func (pub *ExclusivePublication) OfferVectors(vectors []term.DirectBufferVector,
	reservedValueSupplier term.ReservedValueSupplier) int64 {
	if pub.IsClosed() {
		return PublicationClosed
	}
	length, err := term.ValidateAndComputeLength(vectors)
	if err != nil {
		panic(err)
	}
	limit := pub.pubLimit.get()
	position := pub.termBeginPosition + int64(pub.termOffset)
	if position >= limit {
		return pub.backPressureStatus(position, length)
	}

	appender := pub.appenders[pub.activePartitionIndex]
	var result int32
	if length <= pub.maxPayloadLength {
		result = appender.AppendUnfragmentedMessageVectors(pub.termID, pub.termOffset, vectors, length,
			reservedValueSupplier)
	} else {
		pub.checkForMaxMessageLength(length)
		result = appender.AppendFragmentedMessageVectors(pub.termID, pub.termOffset, vectors, length,
			pub.maxPayloadLength, reservedValueSupplier)
	}
	return pub.newPosition(result)
}

// TryClaim reserves length bytes (at most MaxPayloadLength) for zero-copy publication. The claim must be
// completed with Commit() or Abort().
func (pub *ExclusivePublication) TryClaim(length int32, bufferClaim *logbuffer.Claim) int64 {
	pub.checkForMaxPayloadLength(length)
	if pub.IsClosed() {
		return PublicationClosed
	}
	limit := pub.pubLimit.get()
	position := pub.termBeginPosition + int64(pub.termOffset)
	if position >= limit {
		return pub.backPressureStatus(position, length)
	}
	checkPositiveLength(length)
	result := pub.appenders[pub.activePartitionIndex].Claim(pub.termID, pub.termOffset, length, bufferClaim)
	return pub.newPosition(result)
}

// AppendPadding appends a padding frame with length bytes of payload, advancing the stream position without
// delivering a message. length may be up to the framed length of a MaxMessageLength message.
func (pub *ExclusivePublication) AppendPadding(length int32) int64 {
	maxFramedLength := term.ComputeFragmentedFrameLength(pub.maxMessageLength, pub.maxPayloadLength)
	if length > maxFramedLength {
		panic(fmt.Sprintf("padding exceeds maxFramedLength of %d, length=%d", maxFramedLength, length))
	}
	if pub.IsClosed() {
		return PublicationClosed
	}
	checkPositiveLength(length)
	limit := pub.pubLimit.get()
	position := pub.termBeginPosition + int64(pub.termOffset)
	if position >= limit {
		return pub.backPressureStatus(position, length)
	}
	result := pub.appenders[pub.activePartitionIndex].AppendPadding(pub.termID, pub.termOffset, length)
	return pub.newPosition(result)
}

// OfferBlock offers a block of pre-formatted, aligned frames (e.g. replicated from another log) into the current
// term. The first frame must be a data frame at this publication's current term offset, session, stream and term
// id, and the block must fit in the remainder of the term. Panics on invalid input (Java throws
// IllegalArgumentException).
func (pub *ExclusivePublication) OfferBlock(buffer *atomic.Buffer, offset int32, length int32) int64 {
	if pub.IsClosed() {
		return PublicationClosed
	}
	if pub.termOffset >= pub.termBufferLength {
		pub.rotateTerm()
	}
	limit := pub.pubLimit.get()
	position := pub.termBeginPosition + int64(pub.termOffset)
	if position >= limit {
		return pub.backPressureStatus(position, length)
	}
	pub.checkBlockLength(length)
	pub.checkFirstFrame(buffer, offset)
	result := pub.appenders[pub.activePartitionIndex].AppendBlock(pub.termID, pub.termOffset, buffer, offset, length)
	return pub.newPosition(result)
}

func (pub *ExclusivePublication) checkBlockLength(length int32) {
	remaining := pub.termBufferLength - pub.termOffset
	if length > remaining {
		panic(fmt.Sprintf("invalid block length %d, remaining space in term is %d", length, remaining))
	}
}

func (pub *ExclusivePublication) checkFirstFrame(buffer *atomic.Buffer, offset int32) {
	hdr := &logbuffer.DataFrameHeader
	frameType := buffer.GetUInt16(offset + hdr.TypeFieldOffset)
	blockTermOffset := buffer.GetInt32(offset + hdr.TermOffsetFieldOffset)
	blockSessionID := buffer.GetInt32(offset + hdr.SessionIDFieldOffset)
	blockStreamID := buffer.GetInt32(offset + hdr.StreamIDFieldOffset)
	blockTermID := buffer.GetInt32(offset + hdr.TermIDFieldOffset)
	if blockTermOffset != pub.termOffset || blockSessionID != pub.sessionID || blockStreamID != pub.streamID ||
		blockTermID != pub.termID || frameType != hdr.TypeData {
		panic(fmt.Sprintf("improperly formatted block: termOffset=%d (expected=%d), sessionId=%d (expected=%d),"+
			" streamId=%d (expected=%d), termId=%d (expected=%d), frameType=%d (expected=%d)",
			blockTermOffset, pub.termOffset, blockSessionID, pub.sessionID, blockStreamID, pub.streamID,
			blockTermID, pub.termID, frameType, hdr.TypeData))
	}
}
