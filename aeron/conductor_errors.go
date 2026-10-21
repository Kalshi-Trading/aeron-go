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

package aeron

import (
	"errors"
	"fmt"
)

// ErrClientClosed is returned by operations on an Aeron client that has been closed or is terminating.
var ErrClientClosed = errors.New("aeron client is closed")

// ErrUnknownRegistration is returned when polling a registration id this client never issued (or already consumed).
var ErrUnknownRegistration = errors.New("unknown registration id")

// RegistrationError is returned when the media driver rejects a command (io.aeron.exceptions.RegistrationException).
// Code is one of the command.ErrorCode* values.
type RegistrationError struct {
	CorrelationID int64
	Code          int32
	Message       string
}

func (e *RegistrationError) Error() string {
	return fmt.Sprintf("registration error: correlationId=%d errorCode=%d: %s", e.CorrelationID, e.Code, e.Message)
}

// DriverTimeoutError indicates the media driver has not responded or heart-beated within the driver timeout
// (io.aeron.exceptions.DriverTimeoutException). The client is terminated when this is raised by liveness checking.
type DriverTimeoutError struct {
	Message string
}

func (e *DriverTimeoutError) Error() string {
	return "driver timeout: " + e.Message
}

// ClientTimeoutError is raised when the driver reports that it has timed out this client
// (io.aeron.exceptions.ClientTimeoutException). The client is terminated.
type ClientTimeoutError struct {
	Message string
}

func (e *ClientTimeoutError) Error() string {
	return "client timeout: " + e.Message
}

// ConductorServiceTimeoutError is raised when the client conductor has not been serviced within the client liveness
// timeout (io.aeron.exceptions.ConductorServiceTimeoutException). The client is terminated.
type ConductorServiceTimeoutError struct {
	Message string
}

func (e *ConductorServiceTimeoutError) Error() string {
	return "conductor service timeout: " + e.Message
}

// ChannelEndpointError is reported to the error handler when a channel endpoint of an active publication or
// subscription fails (io.aeron.exceptions.ChannelEndpointException).
type ChannelEndpointError struct {
	StatusIndicatorID int32
	Message           string
}

func (e *ChannelEndpointError) Error() string {
	return fmt.Sprintf("channel endpoint error: statusIndicatorId=%d: %s", e.StatusIndicatorID, e.Message)
}
