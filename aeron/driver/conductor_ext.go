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

// ExtendedListener receives the 1.53.3 driver events that are not part of Listener. A Listener passed to NewAdapter
// that also implements ExtendedListener receives them; otherwise they are ignored (as Java ignores unknown events).
type ExtendedListener interface {
	OnStaticCounter(correlationID int64, counterID int32)
	OnNextAvailableSessionID(correlationID int64, sessionID int32)
}

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
	default:
		return false
	}
	return true
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
