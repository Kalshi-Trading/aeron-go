/*
Copyright 2016 Stanislav Liberman
Copyright (C) 2022 Talos, Inc.

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
	"github.com/lirm/aeron-go/aeron/command"
	rb "github.com/lirm/aeron-go/aeron/ringbuffer"
)

// NullValue is used for unset ids (Aeron.NULL_VALUE in Java).
const NullValue int64 = -1

// Proxy sends commands to the media driver over the to-driver ring buffer (io.aeron.DriverProxy). Every method that
// expects a response returns the correlation id the driver will echo back.
type Proxy struct {
	toDriverCommandBuffer *rb.ManyToOne
	clientID              int64
}

// Init initializes the proxy, allocating a new client id from the driver's correlation counter.
func (driver *Proxy) Init(buffer *rb.ManyToOne) *Proxy {
	return driver.InitWithClientID(buffer, buffer.NextCorrelationID())
}

// InitWithClientID initializes the proxy with an existing client id.
func (driver *Proxy) InitWithClientID(buffer *rb.ManyToOne, clientID int64) *Proxy {
	driver.toDriverCommandBuffer = buffer
	driver.clientID = clientID
	logger.Infof("aeron clientID:%d", driver.clientID)
	return driver
}

// ClientID returns the client ID for this connection to the driver.
func (driver *Proxy) ClientID() int64 {
	return driver.clientID
}

// TimeOfLastDriverKeepalive gets the time (ms since epoch) of the last driver keepalive (consumer heartbeat).
func (driver *Proxy) TimeOfLastDriverKeepalive() int64 {
	return driver.toDriverCommandBuffer.ConsumerHeartbeatTime()
}

// NextCorrelationID generates the next correlation id that is unique for the connected Media Driver.
func (driver *Proxy) NextCorrelationID() int64 {
	return driver.toDriverCommandBuffer.NextCorrelationID()
}

// AddPublication sends ADD_PUBLICATION.
func (driver *Proxy) AddPublication(channel string, streamID int32) (int64, error) {
	return driver.addPublication(command.AddPublication, channel, streamID)
}

// AddExclusivePublication sends ADD_EXCLUSIVE_PUBLICATION.
func (driver *Proxy) AddExclusivePublication(channel string, streamID int32) (int64, error) {
	return driver.addPublication(command.AddExclusivePublication, channel, streamID)
}

func (driver *Proxy) addPublication(msgTypeID int32, channel string, streamID int32) (int64, error) {
	correlationID := driver.NextCorrelationID()
	logger.Debugf("driver.%s: clientId=%d correlationId=%d", command.TypeName(msgTypeID), driver.clientID,
		correlationID)
	return correlationID, driver.write(msgTypeID, command.ComputePublicationMessageLength(len(channel)),
		func(buffer *atomic.Buffer, index int32) {
			var m command.PublicationMessage
			m.Wrap(buffer, index)
			m.SetClientID(driver.clientID)
			m.SetCorrelationID(correlationID)
			m.SetStreamID(streamID)
			m.SetChannel(channel)
		})
}

// RemovePublication sends REMOVE_PUBLICATION, optionally revoking the publication.
func (driver *Proxy) RemovePublication(registrationID int64, revoke bool) (int64, error) {
	correlationID := driver.NextCorrelationID()
	logger.Debugf("driver.RemovePublication: clientId=%d correlationId=%d (regId=%d revoke=%v)",
		driver.clientID, correlationID, registrationID, revoke)
	return correlationID, driver.write(command.RemovePublication, command.RemovePublicationMessageLength,
		func(buffer *atomic.Buffer, index int32) {
			var m command.RemovePublicationMessage
			m.Wrap(buffer, index)
			m.SetClientID(driver.clientID)
			m.SetCorrelationID(correlationID)
			m.SetRegistrationID(registrationID)
			m.SetRevoke(revoke)
		})
}

// AddSubscription sends ADD_SUBSCRIPTION.
func (driver *Proxy) AddSubscription(channel string, streamID int32) (int64, error) {
	correlationID := driver.NextCorrelationID()
	logger.Debugf("driver.AddSubscription: correlationId=%d", correlationID)
	return correlationID, driver.write(command.AddSubscription, command.ComputeSubscriptionMessageLength(len(channel)),
		func(buffer *atomic.Buffer, index int32) {
			var m command.SubscriptionMessage
			m.Wrap(buffer, index)
			m.SetClientID(driver.clientID)
			m.SetCorrelationID(correlationID)
			m.SetRegistrationCorrelationID(NullValue)
			m.SetStreamID(streamID)
			m.SetChannel(channel)
		})
}

// RemoveSubscription sends REMOVE_SUBSCRIPTION.
func (driver *Proxy) RemoveSubscription(registrationID int64) (int64, error) {
	logger.Debugf("driver.RemoveSubscription: subId=%d", registrationID)
	return driver.remove(command.RemoveSubscription, registrationID)
}

// RemoveCounter sends REMOVE_COUNTER for a counter registration id.
func (driver *Proxy) RemoveCounter(registrationID int64) (int64, error) {
	logger.Debugf("driver.RemoveCounter: counter registrationId=%d", registrationID)
	return driver.remove(command.RemoveCounter, registrationID)
}

func (driver *Proxy) remove(msgTypeID int32, registrationID int64) (int64, error) {
	correlationID := driver.NextCorrelationID()
	return correlationID, driver.write(msgTypeID, command.RemoveMessageLength, func(buffer *atomic.Buffer, index int32) {
		var m command.RemoveMessage
		m.Wrap(buffer, index)
		m.SetClientID(driver.clientID)
		m.SetCorrelationID(correlationID)
		m.SetRegistrationID(registrationID)
	})
}

// AddDestination sends ADD_DESTINATION to add a destination to an existing Publication.
func (driver *Proxy) AddDestination(registrationID int64, channel string) (int64, error) {
	return driver.destination(command.AddDestination, registrationID, channel)
}

// RemoveDestination sends REMOVE_DESTINATION to remove a destination from an existing Publication.
func (driver *Proxy) RemoveDestination(registrationID int64, channel string) (int64, error) {
	return driver.destination(command.RemoveDestination, registrationID, channel)
}

// AddRcvDestination sends ADD_RCV_DESTINATION to add a destination to an existing MDS Subscription.
func (driver *Proxy) AddRcvDestination(registrationID int64, channel string) (int64, error) {
	return driver.destination(command.AddRcvDestination, registrationID, channel)
}

// RemoveRcvDestination sends REMOVE_RCV_DESTINATION to remove a destination from an existing MDS Subscription.
func (driver *Proxy) RemoveRcvDestination(registrationID int64, channel string) (int64, error) {
	return driver.destination(command.RemoveRcvDestination, registrationID, channel)
}

func (driver *Proxy) destination(msgTypeID int32, registrationID int64, channel string) (int64, error) {
	correlationID := driver.NextCorrelationID()
	logger.Debugf("driver.%s: clientID=%d registrationID=%d correlationID=%d channel=%s",
		command.TypeName(msgTypeID), driver.clientID, registrationID, correlationID, channel)
	return correlationID, driver.write(msgTypeID, command.ComputeDestinationMessageLength(len(channel)),
		func(buffer *atomic.Buffer, index int32) {
			var m command.DestinationMessage
			m.Wrap(buffer, index)
			m.SetClientID(driver.clientID)
			m.SetCorrelationID(correlationID)
			m.SetRegistrationCorrelationID(registrationID)
			m.SetChannel(channel)
		})
}

// RemoveDestinationByID sends REMOVE_DESTINATION_BY_ID to remove a destination from a Publication using the
// destination's registration id (the correlation id returned by AddDestination).
func (driver *Proxy) RemoveDestinationByID(publicationRegistrationID, destinationRegistrationID int64) (int64, error) {
	correlationID := driver.NextCorrelationID()
	return correlationID, driver.write(command.RemoveDestinationByID, command.DestinationByIDMessageLength,
		func(buffer *atomic.Buffer, index int32) {
			var m command.DestinationByIDMessage
			m.Wrap(buffer, index)
			m.SetClientID(driver.clientID)
			m.SetCorrelationID(correlationID)
			m.SetResourceRegistrationID(publicationRegistrationID)
			m.SetDestinationRegistrationID(destinationRegistrationID)
		})
}

// AddCounter sends ADD_COUNTER with the key and label copied from buffers (either may be nil when its length is 0).
func (driver *Proxy) AddCounter(typeID int32, keyBuffer *atomic.Buffer, keyOffset int32, keyLength int32,
	labelBuffer *atomic.Buffer, labelOffset int32, labelLength int32) (int64, error) {
	correlationID := driver.NextCorrelationID()
	length := command.ComputeCounterMessageLength(int(keyLength), int(labelLength))
	return correlationID, driver.write(command.AddCounter, length, func(buffer *atomic.Buffer, index int32) {
		var m command.CounterMessage
		m.Wrap(buffer, index)
		m.SetClientID(driver.clientID)
		m.SetCorrelationID(correlationID)
		m.SetTypeID(typeID)
		m.SetKeyBuffer(keyBuffer, keyOffset, keyLength)
		m.SetLabelBuffer(labelBuffer, labelOffset, labelLength)
	})
}

// AddCounterByLabel sends ADD_COUNTER with an empty key and the given label.
func (driver *Proxy) AddCounterByLabel(typeID int32, label string) (int64, error) {
	correlationID := driver.NextCorrelationID()
	return correlationID, driver.write(command.AddCounter, command.ComputeCounterMessageLength(0, len(label)),
		func(buffer *atomic.Buffer, index int32) {
			var m command.CounterMessage
			m.Wrap(buffer, index)
			m.SetClientID(driver.clientID)
			m.SetCorrelationID(correlationID)
			m.SetTypeID(typeID)
			m.SetKey(nil)
			m.SetLabel(label)
		})
}

// AddStaticCounter sends ADD_STATIC_COUNTER: a counter with a caller-chosen registration id that is not owned by
// (and outlives) the client. The driver answers with ON_STATIC_COUNTER.
func (driver *Proxy) AddStaticCounter(typeID int32, keyBuffer *atomic.Buffer, keyOffset int32, keyLength int32,
	labelBuffer *atomic.Buffer, labelOffset int32, labelLength int32, registrationID int64) (int64, error) {
	correlationID := driver.NextCorrelationID()
	length := command.ComputeStaticCounterMessageLength(int(keyLength), int(labelLength))
	return correlationID, driver.write(command.AddStaticCounter, length, func(buffer *atomic.Buffer, index int32) {
		var m command.StaticCounterMessage
		m.Wrap(buffer, index)
		m.SetClientID(driver.clientID)
		m.SetCorrelationID(correlationID)
		m.SetRegistrationID(registrationID)
		m.SetTypeID(typeID)
		m.SetKeyBuffer(keyBuffer, keyOffset, keyLength)
		m.SetLabelBuffer(labelBuffer, labelOffset, labelLength)
	})
}

// AddStaticCounterByLabel sends ADD_STATIC_COUNTER with an empty key and the given label.
func (driver *Proxy) AddStaticCounterByLabel(typeID int32, label string, registrationID int64) (int64, error) {
	correlationID := driver.NextCorrelationID()
	length := command.ComputeStaticCounterMessageLength(0, len(label))
	return correlationID, driver.write(command.AddStaticCounter, length, func(buffer *atomic.Buffer, index int32) {
		var m command.StaticCounterMessage
		m.Wrap(buffer, index)
		m.SetClientID(driver.clientID)
		m.SetCorrelationID(correlationID)
		m.SetRegistrationID(registrationID)
		m.SetTypeID(typeID)
		m.SetKey(nil)
		m.SetLabel(label)
	})
}

// ClientKeepalive sends CLIENT_KEEPALIVE. Java clients keep alive by updating their heartbeat counter instead, but
// the driver still accepts this command.
func (driver *Proxy) ClientKeepalive() error {
	return driver.correlated(command.ClientKeepalive)
}

// ClientClose sends CLIENT_CLOSE. As in Java the correlation id is NullValue and the driver does not respond.
func (driver *Proxy) ClientClose() error {
	logger.Debugf("driver.ClientClose: clientId=%d", driver.clientID)
	return driver.correlated(command.ClientClose)
}

func (driver *Proxy) correlated(msgTypeID int32) error {
	return driver.write(msgTypeID, command.CorrelatedMessageLength, func(buffer *atomic.Buffer, index int32) {
		var m command.CorrelatedMessage
		m.Wrap(buffer, index)
		m.SetClientID(driver.clientID)
		m.SetCorrelationID(NullValue)
	})
}

// TerminateDriver sends TERMINATE_DRIVER with an optional token, validated by the driver's termination validator.
// The driver does not respond.
func (driver *Proxy) TerminateDriver(token []byte) error {
	return driver.write(command.TerminateDriver, command.ComputeTerminateDriverMessageLength(len(token)),
		func(buffer *atomic.Buffer, index int32) {
			var m command.TerminateDriverMessage
			m.Wrap(buffer, index)
			m.SetClientID(driver.clientID)
			m.SetCorrelationID(NullValue)
			m.SetToken(token)
		})
}

// RejectImage sends REJECT_IMAGE for the image with the given correlation id at a position, with a reason.
func (driver *Proxy) RejectImage(imageCorrelationID int64, position int64, reason string) (int64, error) {
	correlationID := driver.NextCorrelationID()
	return correlationID, driver.write(command.RejectImage, command.ComputeRejectImageMessageLength(len(reason)),
		func(buffer *atomic.Buffer, index int32) {
			var m command.RejectImageMessage
			m.Wrap(buffer, index)
			m.SetClientID(driver.clientID)
			m.SetCorrelationID(correlationID)
			m.SetImageCorrelationID(imageCorrelationID)
			m.SetPosition(position)
			m.SetReason(reason)
		})
}

// NextAvailableSessionID sends GET_NEXT_AVAILABLE_SESSION_ID for a stream. The driver answers with
// ON_NEXT_AVAILABLE_SESSION_ID (or ON_ERROR).
func (driver *Proxy) NextAvailableSessionID(streamID int32) (int64, error) {
	correlationID := driver.NextCorrelationID()
	return correlationID, driver.write(command.GetNextAvailableSessionID,
		command.GetNextAvailableSessionIDMessageLength, func(buffer *atomic.Buffer, index int32) {
			var m command.GetNextAvailableSessionIDMessage
			m.Wrap(buffer, index)
			m.SetClientID(driver.clientID)
			m.SetCorrelationID(correlationID)
			m.SetStreamID(streamID)
		})
}

// write claims space in the to-driver ring buffer, encodes the command in place and commits it (as Java's
// tryClaim/commit), or returns an error when the ring buffer is full or the command exceeds MaxMsgLength (where Java
// throws IllegalArgumentException).
func (driver *Proxy) write(msgTypeID int32, length int32, fill func(buffer *atomic.Buffer, index int32)) error {
	if maxLength := driver.toDriverCommandBuffer.MaxMsgLength(); length > maxLength {
		return fmt.Errorf("failed to write %s command to driver: encoded length=%d exceeds maxMsgLength=%d",
			command.TypeName(msgTypeID), length, maxLength)
	}
	index := driver.toDriverCommandBuffer.TryClaim(msgTypeID, length)
	if index < 0 {
		return fmt.Errorf("failed to write %s command to driver", command.TypeName(msgTypeID))
	}
	committed := false
	defer func() {
		if !committed {
			driver.toDriverCommandBuffer.Abort(index)
		}
	}()
	fill(driver.toDriverCommandBuffer.Buffer(), index)
	driver.toDriverCommandBuffer.Commit(index)
	committed = true
	return nil
}
