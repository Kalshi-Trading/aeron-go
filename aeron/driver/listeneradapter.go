/*
Copyright 2016-2018 Stanislav Liberman

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
	"github.com/lirm/aeron-go/aeron/broadcast"
	"github.com/lirm/aeron-go/aeron/command"
	"github.com/lirm/aeron-go/aeron/logging"
)

var logger = logging.MustGetLogger("driver")

// Events are the media driver -> client message type ids. Prefer the command.On* constants.
var Events = struct {
	OnError                     int32
	OnAvailableImage            int32
	OnPublicationReady          int32
	OnOperationSuccess          int32
	OnUnavailableImage          int32
	OnExclusivePublicationReady int32
	OnSubscriptionReady         int32
	OnCounterReady              int32
	OnUnavailableCounter        int32
	OnClientTimeout             int32
	OnStaticCounter             int32
	OnPublicationError          int32
	OnNextAvailableSessionID    int32
}{
	OnError:                     command.OnError,
	OnAvailableImage:            command.OnAvailableImage,
	OnPublicationReady:          command.OnPublicationReady,
	OnOperationSuccess:          command.OnOperationSuccess,
	OnUnavailableImage:          command.OnUnavailableImage,
	OnExclusivePublicationReady: command.OnExclusivePublicationReady,
	OnSubscriptionReady:         command.OnSubscriptionReady,
	OnCounterReady:              command.OnCounterReady,
	OnUnavailableCounter:        command.OnUnavailableCounter,
	OnClientTimeout:             command.OnClientTimeout,
	OnStaticCounter:             command.OnStaticCounter,
	OnPublicationError:          command.OnPublicationError,
	OnNextAvailableSessionID:    command.OnNextAvailableSessionID,
}

type SubscriberPosition struct {
	indicatorID    int32
	registrationID int64
}

func (pos *SubscriberPosition) RegistrationID() int64 {
	return pos.registrationID
}

func (pos *SubscriberPosition) IndicatorID() int32 {
	return pos.indicatorID
}

// Listener receives decoded driver responses. The adapter does no correlation filtering: every message on the
// to-clients broadcast buffer is delivered, and the listener decides whether it concerns this client.
//
// A Listener may additionally implement NextAvailableSessionIDListener to receive ON_NEXT_AVAILABLE_SESSION_ID;
// it is dropped otherwise.
type Listener interface {
	OnNewPublication(streamID int32, sessionID int32, positionLimitCounterID int32, channelStatusIndicatorID int32,
		logFileName string, correlationID int64, registrationID int64)
	OnNewExclusivePublication(streamID int32, sessionID int32, positionLimitCounterID int32, channelStatusIndicatorID int32,
		logFileName string, correlationID int64, registrationID int64)
	OnAvailableImage(streamID int32, sessionID int32, logFilename string, sourceIdentity string,
		subscriberPositionID int32, subsRegID int64, correlationID int64)
	OnUnavailableImage(correlationID int64, subscriptionRegistrationID int64)
	OnOperationSuccess(correlationID int64)
	OnErrorResponse(offendingCommandCorrelationID int64, errorCode int32, errorMessage string)
	OnChannelEndpointError(correlationID int64, errorMessage string)
	OnSubscriptionReady(correlationID int64, channelStatusIndicatorID int32)
	OnAvailableCounter(correlationID int64, counterID int32)
	OnUnavailableCounter(correlationID int64, counterID int32)
	OnClientTimeout(clientID int64)
	// OnStaticCounter receives ON_STATIC_COUNTER responses to AddStaticCounter.
	OnStaticCounter(correlationID int64, counterID int32)
	// OnPublicationError receives ON_PUBLICATION_ERROR (an error frame received by a publication). It is broadcast to
	// all clients; decode with command.PublicationErrorFrame and filter on RegistrationID.
	OnPublicationError(buffer *atomic.Buffer, offset int32, length int32)
}

// NextAvailableSessionIDListener receives ON_NEXT_AVAILABLE_SESSION_ID responses to NextAvailableSessionID.
type NextAvailableSessionIDListener interface {
	OnNextAvailableSessionID(correlationID int64, nextSessionID int32)
}

// ListenerAdapter decodes the to-clients broadcast buffer and dispatches to a Listener (io.aeron.DriverEventsAdapter).
type ListenerAdapter struct {
	listener          Listener
	broadcastReceiver *broadcast.CopyReceiver

	errorResponse     command.ErrorResponse
	publicationReady  command.PublicationBuffersReady
	subscriptionReady command.SubscriptionReady
	imageReady        command.ImageBuffersReady
	operationSuccess  command.OperationSucceeded
	imageMessage      command.ImageMessage
	counterUpdate     command.CounterUpdate
	clientTimeout     command.ClientTimeout
	staticCounter     command.StaticCounter
	nextSessionID     command.NextAvailableSessionID
}

func NewAdapter(driverListener Listener, broadcastReceiver *broadcast.CopyReceiver) *ListenerAdapter {
	adapter := new(ListenerAdapter)
	adapter.listener = driverListener
	adapter.broadcastReceiver = broadcastReceiver

	return adapter
}

// ReceiveMessages polls the broadcast buffer, dispatching every available message, and returns the number received.
func (adapter *ListenerAdapter) ReceiveMessages() int {
	return adapter.broadcastReceiver.Receive(adapter.OnMessage)
}

// OnMessage decodes and dispatches a single driver response. Unknown message types are ignored, as in Java.
func (adapter *ListenerAdapter) OnMessage(msgTypeID int32, buffer *atomic.Buffer, offset int32, length int32) {
	if logger.IsEnabledFor(logging.DEBUG) {
		logger.Debugf("received %s", command.TypeName(msgTypeID))
	}
	listener := adapter.listener
	switch msgTypeID {
	case command.OnPublicationReady, command.OnExclusivePublicationReady:
		msg := adapter.publicationReady.Wrap(buffer, offset)
		onNew := listener.OnNewPublication
		if msgTypeID == command.OnExclusivePublicationReady {
			onNew = listener.OnNewExclusivePublication
		}
		onNew(msg.StreamID(), msg.SessionID(), msg.PublicationLimitCounterID(), msg.ChannelStatusIndicatorID(),
			msg.LogFileName(), msg.CorrelationID(), msg.RegistrationID())

	case command.OnSubscriptionReady:
		msg := adapter.subscriptionReady.Wrap(buffer, offset)
		listener.OnSubscriptionReady(msg.CorrelationID(), msg.ChannelStatusIndicatorID())

	case command.OnAvailableImage:
		msg := adapter.imageReady.Wrap(buffer, offset)
		listener.OnAvailableImage(msg.StreamID(), msg.SessionID(), msg.LogFileName(), msg.SourceIdentity(),
			msg.SubscriberPositionID(), msg.SubscriptionRegistrationID(), msg.CorrelationID())

	case command.OnOperationSuccess:
		msg := adapter.operationSuccess.Wrap(buffer, offset)
		listener.OnOperationSuccess(msg.CorrelationID())

	case command.OnUnavailableImage:
		msg := adapter.imageMessage.Wrap(buffer, offset)
		listener.OnUnavailableImage(msg.CorrelationID(), msg.SubscriptionRegistrationID())

	case command.OnError:
		msg := adapter.errorResponse.Wrap(buffer, offset)
		errorMessage := msg.ErrorMessage()
		if msg.ErrorCode() == command.ErrorCodeChannelEndpointError {
			listener.OnChannelEndpointError(msg.OffendingCommandCorrelationID(), errorMessage)
		} else {
			listener.OnErrorResponse(msg.OffendingCommandCorrelationID(), msg.ErrorCode(), errorMessage)
		}

	case command.OnCounterReady:
		msg := adapter.counterUpdate.Wrap(buffer, offset)
		listener.OnAvailableCounter(msg.CorrelationID(), msg.CounterID())

	case command.OnUnavailableCounter:
		msg := adapter.counterUpdate.Wrap(buffer, offset)
		listener.OnUnavailableCounter(msg.CorrelationID(), msg.CounterID())

	case command.OnClientTimeout:
		msg := adapter.clientTimeout.Wrap(buffer, offset)
		listener.OnClientTimeout(msg.ClientID())

	case command.OnStaticCounter:
		msg := adapter.staticCounter.Wrap(buffer, offset)
		listener.OnStaticCounter(msg.CorrelationID(), msg.CounterID())

	case command.OnPublicationError:
		listener.OnPublicationError(buffer, offset, length)

	case command.OnNextAvailableSessionID:
		if l, ok := listener.(NextAvailableSessionIDListener); ok {
			msg := adapter.nextSessionID.Wrap(buffer, offset)
			l.OnNextAvailableSessionID(msg.CorrelationID(), msg.NextSessionID())
		}

	default:
		logger.Debugf("ignoring unknown driver message type %d", msgTypeID)
	}
}
