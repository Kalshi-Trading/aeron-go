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
	"github.com/lirm/aeron-go/aeron/atomic"
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
	OnNextAvailableSessionID(correlationID int64, sessionID int32)
}

// dispatchExtendedEvent decodes and dispatches events not handled by the original adapter switch. It returns false
// if the event type is unknown.
func dispatchExtendedEvent(listener Listener, msgTypeID int32, buffer *atomic.Buffer, offset int32, length int32) bool {
	ext, ok := listener.(ExtendedListener)
	switch msgTypeID {
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
