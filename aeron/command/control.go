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

// Package command contains the client <-> media driver control protocol (io.aeron.command in Java): the message type
// ids from ControlProtocolEvents, the ErrorCode values, and flyweights over every command and response message.
package command

// Control protocol semantic version, matching ControlProtocolEvents in Aeron 1.53.3.
const (
	ControlProtocolMajorVersion int32 = 1
	ControlProtocolMinorVersion int32 = 0
	ControlProtocolPatchVersion int32 = 0
	// ControlProtocolSemanticVersion is the composed (major << 16 | minor << 8 | patch) protocol version.
	ControlProtocolSemanticVersion = ControlProtocolMajorVersion<<16 | ControlProtocolMinorVersion<<8 |
		ControlProtocolPatchVersion
)

// Clients to Media Driver
const (
	// AddPublication adds a Publication.
	AddPublication int32 = 0x01
	// RemovePublication removes a Publication (optionally revoking it).
	RemovePublication int32 = 0x02
	// AddExclusivePublication adds an ExclusivePublication.
	AddExclusivePublication int32 = 0x03
	// AddSubscription adds a Subscription.
	AddSubscription int32 = 0x04
	// RemoveSubscription removes a Subscription.
	RemoveSubscription int32 = 0x05
	// ClientKeepalive is a keepalive from a client.
	ClientKeepalive int32 = 0x06
	// AddDestination adds a destination to an existing Publication.
	AddDestination = 0x07
	// RemoveDestination removes a destination from an existing Publication.
	RemoveDestination = 0x08
	// AddCounter adds a Counter.
	AddCounter = 0x09
	// RemoveCounter removes a Counter.
	RemoveCounter = 0x0A
	// ClientClose closes indication from a client.
	ClientClose = 0x0B
	// AddRcvDestination adds a destination for an existing Subscription.
	AddRcvDestination = 0x0C
	// RemoveRcvDestination removes a destination for an existing Subscription.
	RemoveRcvDestination = 0x0D
	// TerminateDriver requests the driver to terminate.
	TerminateDriver int32 = 0x0E
	// AddStaticCounter adds a static Counter (one with a fixed registration id).
	AddStaticCounter int32 = 0x0F
	// RejectImage rejects an Image.
	RejectImage int32 = 0x10
	// RemoveDestinationByID removes a destination by its registration id.
	RemoveDestinationByID int32 = 0x11
	// GetNextAvailableSessionID requests the next available session id for a stream.
	GetNextAvailableSessionID int32 = 0x12
)

// Media Driver to Clients
const (
	// OnError is an error response to a command.
	OnError int32 = 0x0F01
	// OnAvailableImage notifies that a new Image is available for a Subscription.
	OnAvailableImage int32 = 0x0F02
	// OnPublicationReady notifies that a new Publication is ready.
	OnPublicationReady int32 = 0x0F03
	// OnOperationSuccess notifies that an operation succeeded.
	OnOperationSuccess int32 = 0x0F04
	// OnUnavailableImage notifies that an Image is no longer available.
	OnUnavailableImage int32 = 0x0F05
	// OnExclusivePublicationReady notifies that a new ExclusivePublication is ready.
	OnExclusivePublicationReady int32 = 0x0F06
	// OnSubscriptionReady notifies that a new Subscription is ready.
	OnSubscriptionReady int32 = 0x0F07
	// OnCounterReady notifies that a new Counter is ready.
	OnCounterReady int32 = 0x0F08
	// OnUnavailableCounter notifies that a Counter has been removed.
	OnUnavailableCounter int32 = 0x0F09
	// OnClientTimeout notifies that a client has timed out.
	OnClientTimeout int32 = 0x0F0A
	// OnStaticCounter notifies that a static Counter is ready.
	OnStaticCounter int32 = 0x0F0B
	// OnPublicationError notifies of an error frame received by a Publication.
	OnPublicationError int32 = 0x0F0C
	// OnNextAvailableSessionID responds to GetNextAvailableSessionID.
	OnNextAvailableSessionID int32 = 0x0F0D
)

// Error codes sent by the media driver in an ErrorResponse (io.aeron.ErrorCode). Untyped, as upstream lirm/aeron-go.
const (
	ErrorCodeUnknownCodeValue               = -1
	ErrorCodeUnused                         = 0
	ErrorCodeInvalidChannel                 = 1
	ErrorCodeUnknownSubscription            = 2
	ErrorCodeUnknownPublication             = 3
	ErrorCodeChannelEndpointError           = 4
	ErrorCodeUnknownCounter                 = 5
	ErrorCodeUnknownCommandTypeID           = 6
	ErrorCodeMalformedCommand               = 7
	ErrorCodeNotSupported                   = 8
	ErrorCodeUnknownHost                    = 9
	ErrorCodeResourceTemporarilyUnavailable = 10
	ErrorCodeGenericError                   = 11
	ErrorCodeStorageSpace                   = 12
	ErrorCodeImageRejected                  = 13
	ErrorCodePublicationRevoked             = 14
)

// ErrorCodeName returns the Java ErrorCode name for a code value, or "UNKNOWN_CODE_VALUE".
func ErrorCodeName(code int32) string {
	switch code {
	case ErrorCodeUnused:
		return "UNUSED"
	case ErrorCodeInvalidChannel:
		return "INVALID_CHANNEL"
	case ErrorCodeUnknownSubscription:
		return "UNKNOWN_SUBSCRIPTION"
	case ErrorCodeUnknownPublication:
		return "UNKNOWN_PUBLICATION"
	case ErrorCodeChannelEndpointError:
		return "CHANNEL_ENDPOINT_ERROR"
	case ErrorCodeUnknownCounter:
		return "UNKNOWN_COUNTER"
	case ErrorCodeUnknownCommandTypeID:
		return "UNKNOWN_COMMAND_TYPE_ID"
	case ErrorCodeMalformedCommand:
		return "MALFORMED_COMMAND"
	case ErrorCodeNotSupported:
		return "NOT_SUPPORTED"
	case ErrorCodeUnknownHost:
		return "UNKNOWN_HOST"
	case ErrorCodeResourceTemporarilyUnavailable:
		return "RESOURCE_TEMPORARILY_UNAVAILABLE"
	case ErrorCodeGenericError:
		return "GENERIC_ERROR"
	case ErrorCodeStorageSpace:
		return "STORAGE_SPACE"
	case ErrorCodeImageRejected:
		return "IMAGE_REJECTED"
	case ErrorCodePublicationRevoked:
		return "PUBLICATION_REVOKED"
	default:
		return "UNKNOWN_CODE_VALUE"
	}
}

// TypeName returns the ControlProtocolEvents name for a message type id, or "UNKNOWN".
func TypeName(msgTypeID int32) string {
	if name, ok := typeNames[msgTypeID]; ok {
		return name
	}
	return "UNKNOWN"
}

var typeNames = map[int32]string{
	AddPublication:              "ADD_PUBLICATION",
	RemovePublication:           "REMOVE_PUBLICATION",
	AddExclusivePublication:     "ADD_EXCLUSIVE_PUBLICATION",
	AddSubscription:             "ADD_SUBSCRIPTION",
	RemoveSubscription:          "REMOVE_SUBSCRIPTION",
	ClientKeepalive:             "CLIENT_KEEPALIVE",
	AddDestination:              "ADD_DESTINATION",
	RemoveDestination:           "REMOVE_DESTINATION",
	AddCounter:                  "ADD_COUNTER",
	RemoveCounter:               "REMOVE_COUNTER",
	ClientClose:                 "CLIENT_CLOSE",
	AddRcvDestination:           "ADD_RCV_DESTINATION",
	RemoveRcvDestination:        "REMOVE_RCV_DESTINATION",
	TerminateDriver:             "TERMINATE_DRIVER",
	AddStaticCounter:            "ADD_STATIC_COUNTER",
	RejectImage:                 "REJECT_IMAGE",
	RemoveDestinationByID:       "REMOVE_DESTINATION_BY_ID",
	GetNextAvailableSessionID:   "GET_NEXT_AVAILABLE_SESSION_ID",
	OnError:                     "ON_ERROR",
	OnAvailableImage:            "ON_AVAILABLE_IMAGE",
	OnPublicationReady:          "ON_PUBLICATION_READY",
	OnOperationSuccess:          "ON_OPERATION_SUCCESS",
	OnUnavailableImage:          "ON_UNAVAILABLE_IMAGE",
	OnExclusivePublicationReady: "ON_EXCLUSIVE_PUBLICATION_READY",
	OnSubscriptionReady:         "ON_SUBSCRIPTION_READY",
	OnCounterReady:              "ON_COUNTER_READY",
	OnUnavailableCounter:        "ON_UNAVAILABLE_COUNTER",
	OnClientTimeout:             "ON_CLIENT_TIMEOUT",
	OnStaticCounter:             "ON_STATIC_COUNTER",
	OnPublicationError:          "ON_PUBLICATION_ERROR",
	OnNextAvailableSessionID:    "ON_NEXT_AVAILABLE_SESSION_ID",
}
