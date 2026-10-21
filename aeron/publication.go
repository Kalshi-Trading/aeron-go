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
	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/logbuffer"
	"github.com/lirm/aeron-go/aeron/logbuffer/term"
)

// Offer/TryClaim result codes. Values match io.aeron.Publication.
const (
	// NotConnected indicates that the publication is not connected to a subscriber (NOT_CONNECTED).
	NotConnected int64 = -1
	// BackPressured indicates that the offer failed due to back pressure from subscribers (BACK_PRESSURED).
	BackPressured int64 = -2
	// AdminAction indicates that the offer failed due to an administration action (e.g. log rotation) and should be
	// retried (ADMIN_ACTION).
	AdminAction int64 = -3
	// PublicationClosed indicates that the Publication has been closed (CLOSED).
	PublicationClosed int64 = -4
	// MaxPositionExceeded indicates that the offer would exceed the maximum possible position for the stream
	// (MAX_POSITION_EXCEEDED).
	MaxPositionExceeded int64 = -5
)

// Publication is a concurrent publication (io.aeron.ConcurrentPublication): safe for use by multiple goroutines.
// Space in the log is reserved with an atomic add on the term tail.
type Publication struct {
	publicationBase

	appenders [logbuffer.PartitionCount]*term.Appender
}

// NewPublication is a factory method create new publications
func NewPublication(logBuffers *logbuffer.LogBuffers) *Publication {
	pub := new(Publication)
	pub.init(logBuffers)
	for i := 0; i < logbuffer.PartitionCount; i++ {
		pub.appenders[i] = term.MakeAppender(logBuffers, i)
	}
	return pub
}

// Position returns the current position to which the publication has advanced for this stream, or
// PublicationClosed if closed.
func (pub *Publication) Position() int64 {
	if pub.IsClosed() {
		return PublicationClosed
	}
	rawTail := pub.rawTailVolatile(pub.activeTermCount() % logbuffer.PartitionCount)
	termOffset := termOffsetOf(rawTail, pub.termBufferLength)
	return computeTermBeginPosition(logbuffer.TermID(rawTail), pub.positionBitsToShift, pub.initialTermID) +
		int64(termOffset)
}

// AvailableWindow returns the number of bytes that can be offered before back pressure, or PublicationClosed.
func (pub *Publication) AvailableWindow() int64 {
	if pub.IsClosed() {
		return PublicationClosed
	}
	return pub.pubLimit.get() - pub.Position()
}

func termOffsetOf(rawTail int64, termLength int32) int32 {
	tail := rawTail & 0xFFFFFFFF
	if tail > int64(termLength) {
		return termLength
	}
	return int32(tail)
}

// activeState reads the active term state. ok is false when the term count and tail are out of step, i.e. a
// rotation is in progress, and the caller must return AdminAction.
func (pub *Publication) activeState() (termCount int32, appender *term.Appender, position int64, ok bool) {
	termCount = pub.activeTermCount()
	index := termCount % logbuffer.PartitionCount
	appender = pub.appenders[index]
	rawTail := appender.RawTail()
	termID := logbuffer.TermID(rawTail)
	if termCount != termID-pub.initialTermID {
		return termCount, appender, 0, false
	}
	termOffset := termOffsetOf(rawTail, pub.termBufferLength)
	position = computeTermBeginPosition(termID, pub.positionBitsToShift, pub.initialTermID) + int64(termOffset)
	return termCount, appender, position, true
}

// newPosition converts an appender result into a stream position or an error code, rotating the log when the
// appender tripped the end of the term.
func (pub *Publication) newPosition(resultingOffset int64, termID int32) int64 {
	termBeginPosition := computeTermBeginPosition(termID, pub.positionBitsToShift, pub.initialTermID)
	if resultingOffset > 0 {
		return termBeginPosition + resultingOffset
	}
	if termBeginPosition+int64(pub.termBufferLength) >= pub.maxPossiblePosition {
		return MaxPositionExceeded
	}
	pub.rotateLog(termID-pub.initialTermID, termID)
	return AdminAction
}

// Offer a message of length bytes from buffer at offset, fragmenting it if larger than MaxPayloadLength.
// reservedValueSupplier may be nil. Returns the new stream position or a negative result code
// (NotConnected, BackPressured, AdminAction, PublicationClosed, MaxPositionExceeded).
// Panics if length is negative or exceeds MaxMessageLength (Java throws IllegalArgumentException).
func (pub *Publication) Offer(buffer *atomic.Buffer, offset int32, length int32,
	reservedValueSupplier term.ReservedValueSupplier) int64 {
	if pub.IsClosed() {
		return PublicationClosed
	}
	limit := pub.pubLimit.get()
	_, appender, position, ok := pub.activeState()
	if !ok {
		return AdminAction
	}
	if position >= limit {
		return pub.backPressureStatus(position, length)
	}

	var resultingOffset int64
	var termID int32
	if length <= pub.maxPayloadLength {
		checkPositiveLength(length)
		resultingOffset, termID = appender.AppendUnfragmentedMessage(buffer, offset, length, reservedValueSupplier)
	} else {
		pub.checkForMaxMessageLength(length)
		resultingOffset, termID = appender.AppendFragmentedMessage(buffer, offset, length, pub.maxPayloadLength,
			reservedValueSupplier)
	}
	return pub.newPosition(resultingOffset, termID)
}

// Offer2 offers the concatenation of two buffers as one message (Java offer(bufferOne, ..., bufferTwo, ...)).
func (pub *Publication) Offer2(
	bufferOne *atomic.Buffer, offsetOne int32, lengthOne int32,
	bufferTwo *atomic.Buffer, offsetTwo int32, lengthTwo int32,
	reservedValueSupplier term.ReservedValueSupplier,
) int64 {
	if pub.IsClosed() {
		return PublicationClosed
	}
	length := validateAndComputeLength(lengthOne, lengthTwo)
	limit := pub.pubLimit.get()
	_, appender, position, ok := pub.activeState()
	if !ok {
		return AdminAction
	}
	if position >= limit {
		return pub.backPressureStatus(position, length)
	}

	var resultingOffset int64
	var termID int32
	if length <= pub.maxPayloadLength {
		resultingOffset, termID = appender.AppendUnfragmentedMessage2(bufferOne, offsetOne, lengthOne,
			bufferTwo, offsetTwo, lengthTwo, reservedValueSupplier)
	} else {
		pub.checkForMaxMessageLength(length)
		resultingOffset, termID = appender.AppendFragmentedMessage2(bufferOne, offsetOne, lengthOne,
			bufferTwo, offsetTwo, lengthTwo, pub.maxPayloadLength, reservedValueSupplier)
	}
	return pub.newPosition(resultingOffset, termID)
}

// OfferVectors offers the concatenation of vectors as one message (Java offer(DirectBufferVector[], ...)).
// Panics if a vector is invalid or the total exceeds MaxMessageLength.
func (pub *Publication) OfferVectors(vectors []term.DirectBufferVector,
	reservedValueSupplier term.ReservedValueSupplier) int64 {
	if pub.IsClosed() {
		return PublicationClosed
	}
	length, err := term.ValidateAndComputeLength(vectors)
	if err != nil {
		panic(err)
	}
	limit := pub.pubLimit.get()
	_, appender, position, ok := pub.activeState()
	if !ok {
		return AdminAction
	}
	if position >= limit {
		return pub.backPressureStatus(position, length)
	}

	var resultingOffset int64
	var termID int32
	if length <= pub.maxPayloadLength {
		resultingOffset, termID = appender.AppendUnfragmentedMessageVectors(vectors, length, reservedValueSupplier)
	} else {
		pub.checkForMaxMessageLength(length)
		resultingOffset, termID = appender.AppendFragmentedMessageVectors(vectors, length, pub.maxPayloadLength,
			reservedValueSupplier)
	}
	return pub.newPosition(resultingOffset, termID)
}

// TryClaim reserves length bytes (at most MaxPayloadLength) in the log for zero-copy publication. On success the
// claim must be completed with Commit() or Abort(). Panics if length exceeds MaxPayloadLength.
func (pub *Publication) TryClaim(length int32, bufferClaim *logbuffer.Claim) int64 {
	pub.checkForMaxPayloadLength(length)
	if pub.IsClosed() {
		return PublicationClosed
	}
	limit := pub.pubLimit.get()
	_, appender, position, ok := pub.activeState()
	if !ok {
		return AdminAction
	}
	if position >= limit {
		return pub.backPressureStatus(position, length)
	}
	checkPositiveLength(length)
	resultingOffset, termID := appender.Claim(length, bufferClaim)
	return pub.newPosition(resultingOffset, termID)
}
