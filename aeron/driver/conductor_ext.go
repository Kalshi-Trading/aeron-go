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

package driver

import (
	"fmt"
	"net"

	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/util"
)

// Command and event type ids added to the Aeron control protocol after the original lirm/aeron-go port
// (io.aeron.command.ControlProtocolEvents 1.53.3).
const (
	CmdAddStaticCounter           int32 = 0x0F
	CmdRejectImage                int32 = 0x10
	CmdRemoveDestinationByID      int32 = 0x11
	CmdGetNextAvailableSessionID  int32 = 0x12
	EventOnStaticCounter          int32 = 0x0F0B
	EventOnPublicationError       int32 = 0x0F0C
	EventOnNextAvailableSessionID int32 = 0x0F0D
)

// PublicationErrorFrame is a decoded ON_PUBLICATION_ERROR event, sent by the driver when a publication receives an
// error frame from a remote receiver.
type PublicationErrorFrame struct {
	RegistrationID            int64
	DestinationRegistrationID int64
	SessionID                 int32
	StreamID                  int32
	ReceiverID                int64
	GroupTag                  int64
	ErrorCode                 int32
	ErrorMessage              string
	SourceAddress             *net.UDPAddr
}

func (f *PublicationErrorFrame) String() string {
	return fmt.Sprintf("PublicationErrorFrame{registrationId=%d, destinationRegistrationId=%d, sessionId=%d, "+
		"streamId=%d, receiverId=%d, groupTag=%d, sourceAddress=%v, errorCode=%d, errorMessage=%q}",
		f.RegistrationID, f.DestinationRegistrationID, f.SessionID, f.StreamID, f.ReceiverID, f.GroupTag,
		f.SourceAddress, f.ErrorCode, f.ErrorMessage)
}

// ExtendedListener receives the 1.53.3 driver events that are not part of Listener. A Listener passed to NewAdapter
// that also implements ExtendedListener receives them; otherwise they are ignored (as Java ignores unknown events).
type ExtendedListener interface {
	OnStaticCounter(correlationID int64, counterID int32)
	OnPublicationError(frame *PublicationErrorFrame)
	OnNextAvailableSessionID(correlationID int64, sessionID int32)
}

const (
	addressTypeIPv4 = 1
	addressTypeIPv6 = 2
)

// dispatchExtendedEvent decodes and dispatches events not handled by the original adapter switch. It returns false
// if the event type is unknown.
func dispatchExtendedEvent(listener Listener, msgTypeID int32, buffer *atomic.Buffer, offset int32, length int32) bool {
	ext, ok := listener.(ExtendedListener)
	switch msgTypeID {
	case EventOnStaticCounter:
		if ok && length >= 12 {
			ext.OnStaticCounter(buffer.GetInt64(offset), buffer.GetInt32(offset+8))
		}
	case EventOnNextAvailableSessionID:
		if ok && length >= 12 {
			ext.OnNextAvailableSessionID(buffer.GetInt64(offset), buffer.GetInt32(offset+8))
		}
	case EventOnPublicationError:
		if ok {
			if frame := decodePublicationError(buffer, offset, length); frame != nil {
				ext.OnPublicationError(frame)
			}
		}
	default:
		return false
	}
	return true
}

// Layout (PublicationErrorFrameFlyweight): registrationId i64, destinationRegistrationId i64, sessionId i32,
// streamId i32, receiverId i64, groupTag i64, addressType i16, port u16, address [16]byte, errorCode i32,
// errorMessage (i32 length + ascii).
func decodePublicationError(buffer *atomic.Buffer, offset int32, length int32) *PublicationErrorFrame {
	const errorMessageOffset = 8 + 8 + 4 + 4 + 8 + 8 + 2 + 2 + 16 + 4
	if length < errorMessageOffset+4 {
		return nil
	}
	f := &PublicationErrorFrame{
		RegistrationID:            buffer.GetInt64(offset),
		DestinationRegistrationID: buffer.GetInt64(offset + 8),
		SessionID:                 buffer.GetInt32(offset + 16),
		StreamID:                  buffer.GetInt32(offset + 20),
		ReceiverID:                buffer.GetInt64(offset + 24),
		GroupTag:                  buffer.GetInt64(offset + 32),
		ErrorCode:                 buffer.GetInt32(offset + 60),
	}
	addressType := int16(buffer.GetUInt16(offset + 40))
	port := int(buffer.GetUInt16(offset + 42))
	switch addressType {
	case addressTypeIPv4:
		f.SourceAddress = &net.UDPAddr{IP: net.IP(buffer.GetBytesArray(offset+44, 4)), Port: port}
	case addressTypeIPv6:
		f.SourceAddress = &net.UDPAddr{IP: net.IP(buffer.GetBytesArray(offset+44, 16)), Port: port}
	}
	msgLen := buffer.GetInt32(offset + errorMessageOffset)
	if msgLen > 0 && errorMessageOffset+4+msgLen <= length {
		f.ErrorMessage = string(buffer.GetBytesArray(offset+errorMessageOffset+4, msgLen))
	}
	return f
}

// RemoveDestinationByID sends REMOVE_DESTINATION_BY_ID to remove a destination from a multi-destination publication
// using the registration id returned when the destination was added.
func (driver *Proxy) RemoveDestinationByID(publicationRegistrationID int64, destinationRegistrationID int64) (int64, error) {
	correlationID := driver.toDriverCommandBuffer.NextCorrelationID()
	filler := func(buffer *atomic.Buffer, length *int) int32 {
		buffer.PutInt64(0, driver.clientID)
		buffer.PutInt64(8, correlationID)
		buffer.PutInt64(16, publicationRegistrationID)
		buffer.PutInt64(24, destinationRegistrationID)
		*length = 32
		return CmdRemoveDestinationByID
	}
	if err := driver.writeCommandToDriver(filler); err != nil {
		return 0, err
	}
	return correlationID, nil
}

// GetNextAvailableSessionID sends GET_NEXT_AVAILABLE_SESSION_ID for the given stream id.
func (driver *Proxy) GetNextAvailableSessionID(streamID int32) (int64, error) {
	correlationID := driver.toDriverCommandBuffer.NextCorrelationID()
	filler := func(buffer *atomic.Buffer, length *int) int32 {
		buffer.PutInt64(0, driver.clientID)
		buffer.PutInt64(8, correlationID)
		buffer.PutInt32(16, streamID)
		*length = 20
		return CmdGetNextAvailableSessionID
	}
	if err := driver.writeCommandToDriver(filler); err != nil {
		return 0, err
	}
	return correlationID, nil
}

// AddStaticCounter sends ADD_STATIC_COUNTER. Static counters are not owned by the client and are identified by
// (typeId, registrationId); an existing counter is returned by the driver if one matches.
func (driver *Proxy) AddStaticCounter(typeID int32, keyBuffer *atomic.Buffer, keyOffset int32, keyLength int32,
	label string, registrationID int64) (int64, error) {
	labelLength := int32(len(label))
	alignedKeyLength := util.AlignInt32(keyLength, 4)
	if 32+alignedKeyLength+4+labelLength > 512 {
		return 0, fmt.Errorf("static counter key/label too long: key=%d label=%d", keyLength, labelLength)
	}
	correlationID := driver.toDriverCommandBuffer.NextCorrelationID()
	filler := func(buffer *atomic.Buffer, length *int) int32 {
		buffer.PutInt64(0, driver.clientID)
		buffer.PutInt64(8, correlationID)
		buffer.PutInt64(16, registrationID)
		buffer.PutInt32(24, typeID)
		buffer.PutInt32(28, keyLength)
		if keyLength > 0 && keyBuffer != nil {
			buffer.PutBytes(32, keyBuffer, keyOffset, keyLength)
		}
		labelOffset := 32 + alignedKeyLength
		buffer.PutInt32(labelOffset, labelLength)
		if labelLength > 0 {
			labelBytes := []byte(label)
			buffer.PutBytesArray(labelOffset+4, &labelBytes, 0, labelLength)
		}
		*length = int(labelOffset + 4 + labelLength)
		return CmdAddStaticCounter
	}
	if err := driver.writeCommandToDriver(filler); err != nil {
		return 0, err
	}
	return correlationID, nil
}
