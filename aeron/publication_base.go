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
	"errors"
	"fmt"

	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/logbuffer"
	"github.com/lirm/aeron-go/aeron/util"
)

const (
	// logIsPublicationRevokedOffset is LogBufferDescriptor.LOG_IS_PUBLICATION_REVOKED_OFFSET (Aeron 1.48+).
	logIsPublicationRevokedOffset = logbuffer.LogIsPublicationRevokedOffset
	termTailCountersOffset        = logbuffer.TermTailCountersOffset
	logActiveTermCountOffset      = logbuffer.LogActiveTermCountOffset
	logIsConnectedOffset          = logbuffer.LogIsConnectedOffset
)

// publicationBase holds the state and accessors shared by Publication (concurrent) and ExclusivePublication,
// mirroring the abstract io.aeron.Publication.
type publicationBase struct {
	conductor                *ClientConductor
	channel                  string
	regID                    int64
	originalRegID            int64
	maxPossiblePosition      int64
	streamID                 int32
	sessionID                int32
	initialTermID            int32
	maxPayloadLength         int32
	maxMessageLength         int32
	positionBitsToShift      int32
	termBufferLength         int32
	pubLimit                 Position
	channelStatusIndicatorID int32
	revokeOnClose            bool

	isClosed    atomic.Bool
	metaData    *logbuffer.LogBufferMetaData
	logMetaData *atomic.Buffer
	termBuffers [logbuffer.PartitionCount]*atomic.Buffer
	logBuffers  *logbuffer.LogBuffers
}

func (pub *publicationBase) init(logBuffers *logbuffer.LogBuffers) {
	termBufferCapacity := logBuffers.Buffer(0).Capacity()
	pub.logBuffers = logBuffers
	pub.metaData = logBuffers.Meta()
	pub.logMetaData = logBuffers.Buffer(logbuffer.LogMetaDataSectionIndex)
	for i := 0; i < logbuffer.PartitionCount; i++ {
		pub.termBuffers[i] = logBuffers.Buffer(i)
	}
	pub.termBufferLength = termBufferCapacity
	pub.initialTermID = pub.metaData.InitTermID.Get()
	pub.maxPayloadLength = pub.metaData.MTULen.Get() - logbuffer.DataFrameHeader.Length
	pub.maxMessageLength = logbuffer.ComputeMaxMessageLength(termBufferCapacity)
	pub.positionBitsToShift = int32(util.NumberOfTrailingZeroes(uint32(termBufferCapacity)))
	pub.maxPossiblePosition = int64(termBufferCapacity) * (1 << 31)
	pub.isClosed.Set(false)
}

// ChannelStatusID returns the counter used to represent the channel status for this publication.
func (pub *publicationBase) ChannelStatusID() int32 {
	return pub.channelStatusIndicatorID
}

// RegistrationID returns the registration id.
func (pub *publicationBase) RegistrationID() int64 {
	return pub.regID
}

// OriginalRegistrationID returns the original registration id.
func (pub *publicationBase) OriginalRegistrationID() int64 {
	return pub.originalRegID
}

// IsOriginal return true if this instance is the first added otherwise false.
func (pub *publicationBase) IsOriginal() bool {
	return pub.originalRegID == pub.regID
}

// Channel returns the media address for delivery to the channel.
func (pub *publicationBase) Channel() string {
	return pub.channel
}

// StreamID returns Stream identity for scoping within the channel media address.
func (pub *publicationBase) StreamID() int32 {
	return pub.streamID
}

// SessionID returns the session id for this publication.
func (pub *publicationBase) SessionID() int32 {
	return pub.sessionID
}

// InitialTermID returns the initial term id assigned when this publication was created.
func (pub *publicationBase) InitialTermID() int32 {
	return pub.initialTermID
}

// TermBufferLength returns the length in bytes of each term partition in the log buffer.
func (pub *publicationBase) TermBufferLength() int32 {
	return pub.termBufferLength
}

// MaxPossiblePosition returns the maximum position the stream can reach given its term length.
func (pub *publicationBase) MaxPossiblePosition() int64 {
	return pub.maxPossiblePosition
}

// MaxMessageLength returns the maximum length of a message that can be offered (fragmented if necessary).
func (pub *publicationBase) MaxMessageLength() int32 {
	return pub.maxMessageLength
}

// MaxPayloadLength returns the maximum payload of a single frame, i.e. MTU - header. Larger messages are fragmented.
func (pub *publicationBase) MaxPayloadLength() int32 {
	return pub.maxPayloadLength
}

// PositionBitsToShift returns the number of bits to right shift a position to get a term count.
func (pub *publicationBase) PositionBitsToShift() int32 {
	return pub.positionBitsToShift
}

// IsConnected returns whether this publication has connected subscribers (as reported by the driver).
func (pub *publicationBase) IsConnected() bool {
	return !pub.IsClosed() && pub.logMetaData.GetInt32Volatile(logIsConnectedOffset) == 1
}

// IsClosed returns whether this Publication has been closed.
func (pub *publicationBase) IsClosed() bool {
	return pub.isClosed.Get()
}

// IsRevoked returns whether the publication's log has been marked revoked (Aeron 1.48+).
func (pub *publicationBase) IsRevoked() bool {
	return pub.logMetaData.GetUInt8(logIsPublicationRevokedOffset) == 1
}

// PositionLimit returns the position this publication may be offered up to before being back pressured, or
// PublicationClosed if closed.
func (pub *publicationBase) PositionLimit() int64 {
	if pub.IsClosed() {
		return PublicationClosed
	}
	return pub.pubLimit.get()
}

// PositionLimitID returns the counter id of the position limit counter.
func (pub *publicationBase) PositionLimitID() int32 {
	return pub.pubLimit.id
}

// AddDestination adds a destination to a manual multi-destination-cast (control-mode=manual) publication.
func (pub *publicationBase) AddDestination(endpointChannel string) error {
	if pub.IsClosed() {
		return errors.New("publication is closed")
	}
	return pub.conductor.AddDestination(pub.originalRegID, endpointChannel)
}

// RemoveDestination removes a destination from a manual multi-destination-cast publication.
func (pub *publicationBase) RemoveDestination(endpointChannel string) error {
	if pub.IsClosed() {
		return errors.New("publication is closed")
	}
	return pub.conductor.RemoveDestination(pub.originalRegID, endpointChannel)
}

// AsyncAddDestination sends an add destination command and returns its correlation id without waiting for a
// response from the driver.
func (pub *publicationBase) AsyncAddDestination(endpointChannel string) (int64, error) {
	if pub.IsClosed() {
		return 0, errors.New("publication is closed")
	}
	if err := pub.conductor.getDriverStatus(); err != nil {
		return 0, err
	}
	pub.conductor.adminLock.Lock()
	defer pub.conductor.adminLock.Unlock()
	return pub.conductor.driverProxy.AddDestination(pub.regID, endpointChannel)
}

// AsyncRemoveDestination sends a remove destination command and returns its correlation id without waiting for a
// response from the driver.
func (pub *publicationBase) AsyncRemoveDestination(endpointChannel string) (int64, error) {
	if pub.IsClosed() {
		return 0, errors.New("publication is closed")
	}
	if err := pub.conductor.getDriverStatus(); err != nil {
		return 0, err
	}
	pub.conductor.adminLock.Lock()
	defer pub.conductor.adminLock.Unlock()
	return pub.conductor.driverProxy.RemoveDestination(pub.regID, endpointChannel)
}

// Close releases this publication with the driver. Publications are reference counted by the driver and only truly
// closed when the last reference is removed.
func (pub *publicationBase) Close() error {
	if pub != nil && pub.isClosed.CompareAndSet(false, true) {
		if pub.conductor == nil {
			return nil
		}
		return pub.conductor.releasePublication(pub.regID, pub.revokeOnClose)
	}
	return nil
}

func (pub *publicationBase) activeTermCount() int32 {
	return pub.logMetaData.GetInt32Volatile(logActiveTermCountOffset)
}

func (pub *publicationBase) rawTailVolatile(partitionIndex int32) int64 {
	return pub.logMetaData.GetInt64Volatile(termTailCountersOffset + partitionIndex*8)
}

// rotateLog moves the log on to the next term. Safe to call concurrently: only one caller will win the CAS.
func (pub *publicationBase) rotateLog(currentTermCount, currentTermID int32) bool {
	nextTermID := currentTermID + 1
	nextTermCount := currentTermCount + 1
	nextIndex := nextTermCount % logbuffer.PartitionCount
	expectedTermID := nextTermID - logbuffer.PartitionCount
	tailOffset := termTailCountersOffset + nextIndex*8

	for {
		rawTail := pub.logMetaData.GetInt64Volatile(tailOffset)
		if expectedTermID != logbuffer.TermID(rawTail) {
			break
		}
		if pub.logMetaData.CompareAndSetInt64(tailOffset, rawTail, int64(nextTermID)<<32) {
			break
		}
	}
	return pub.logMetaData.CompareAndSetInt32(logActiveTermCountOffset, currentTermCount, nextTermCount)
}

func (pub *publicationBase) backPressureStatus(currentPosition int64, messageLength int32) int64 {
	if currentPosition+int64(util.AlignInt32(messageLength+logbuffer.DataFrameHeader.Length, logbuffer.FrameAlignment)) >=
		pub.maxPossiblePosition {
		return MaxPositionExceeded
	}
	if pub.logMetaData.GetInt32Volatile(logIsConnectedOffset) == 1 {
		return BackPressured
	}
	return NotConnected
}

func (pub *publicationBase) checkForMaxMessageLength(length int32) {
	if length > pub.maxMessageLength {
		panic(fmt.Sprintf("message exceeds maxMessageLength=%d, length=%d", pub.maxMessageLength, length))
	}
}

func (pub *publicationBase) checkForMaxPayloadLength(length int32) {
	if length > pub.maxPayloadLength {
		panic(fmt.Sprintf("claim exceeds maxPayloadLength=%d, length=%d", pub.maxPayloadLength, length))
	}
}

func checkPositiveLength(length int32) {
	if length < 0 {
		panic(fmt.Sprintf("invalid length=%d", length))
	}
}

func validateAndComputeLength(lengthOne, lengthTwo int32) int32 {
	checkPositiveLength(lengthOne)
	checkPositiveLength(lengthTwo)
	total := int64(lengthOne) + int64(lengthTwo)
	if total > int64(^uint32(0)>>1) {
		panic(fmt.Sprintf("lengthOne + lengthTwo exceeds MAX_VALUE: lengthOne=%d lengthTwo=%d", lengthOne, lengthTwo))
	}
	return int32(total)
}

func computeTermBeginPosition(activeTermID, positionBitsToShift, initialTermID int32) int64 {
	termCount := int64(activeTermID - initialTermID)
	return termCount << uint32(positionBitsToShift)
}

func nextPartitionIndex(currentIndex int32) int32 {
	return util.FastMod3(uint64(currentIndex) + 1)
}

// PublicationErrorString returns a human-readable name for a negative offer/claim result
// (Publication.errorString in Java).
func PublicationErrorString(position int64) string {
	switch position {
	case NotConnected:
		return "NOT_CONNECTED"
	case BackPressured:
		return "BACK_PRESSURED"
	case AdminAction:
		return "ADMIN_ACTION"
	case PublicationClosed:
		return "CLOSED"
	case MaxPositionExceeded:
		return "MAX_POSITION_EXCEEDED"
	}
	if position >= 0 {
		return "NONE"
	}
	return fmt.Sprintf("UNKNOWN(%d)", position)
}
