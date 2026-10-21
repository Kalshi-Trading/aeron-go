// Copyright 2026 Kalshi Trading.
// Copyright 2026 Adaptive Financial Consulting Limited (Java reference implementation).
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package persistentsubscription

import (
	"errors"
	"fmt"
	"regexp"
	"strconv"

	"github.com/lirm/aeron-go/aeron/command"
	"github.com/lirm/aeron-go/archive/codecs"
)

// Reason a persistent subscription Error occurred. Mirrors PersistentSubscriptionException.Reason.
type Reason int

const (
	// ReasonGeneric is used when no more specific reason is available.
	ReasonGeneric Reason = iota
	// ReasonRecordingNotFound means no recording exists with the specified recording id.
	ReasonRecordingNotFound
	// ReasonStreamIDMismatch means the requested live stream id does not match the stream id of the recording.
	ReasonStreamIDMismatch
	// ReasonInvalidStartPosition means the requested start position is not available for the recording.
	ReasonInvalidStartPosition
)

func (r Reason) String() string {
	switch r {
	case ReasonGeneric:
		return "GENERIC"
	case ReasonRecordingNotFound:
		return "RECORDING_NOT_FOUND"
	case ReasonStreamIDMismatch:
		return "STREAM_ID_MISMATCH"
	case ReasonInvalidStartPosition:
		return "INVALID_START_POSITION"
	}
	return "UNKNOWN"
}

// Error is the Go equivalent of io.aeron.archive.client.PersistentSubscriptionException.
type Error struct {
	Reason  Reason
	Message string
}

func (e *Error) Error() string {
	return fmt.Sprintf("persistent subscription error (%s): %s", e.Reason, e.Message)
}

func newError(reason Reason, format string, args ...interface{}) *Error {
	return &Error{Reason: reason, Message: fmt.Sprintf(format, args...)}
}

// ReasonOf returns the Reason of a persistent subscription Error anywhere in err's chain.
func ReasonOf(err error) (Reason, bool) {
	var e *Error
	if errors.As(err, &e) {
		return e.Reason, true
	}
	return ReasonGeneric, false
}

// Archive error codes carried in the relevantId of an error ControlResponse (io.aeron.archive.client.ArchiveException).
const (
	ArchiveErrorGeneric          int32 = 0
	ArchiveErrorUnknownRecording int32 = 5
	ArchiveErrorInvalidPosition  int32 = 16
)

// ArchiveError is an error reported by the archive in a ControlResponse (Java's ArchiveException).
type ArchiveError struct {
	ErrorCode     int32
	CorrelationID int64
	Message       string
}

func (e *ArchiveError) Error() string {
	return fmt.Sprintf("archive error code=%d correlationId=%d: %s", e.ErrorCode, e.CorrelationID, e.Message)
}

// Event is a non-terminal informational error passed to Listener.OnError (Java's AeronEvent), e.g. when no live
// image becomes available within the message timeout.
type Event struct {
	Message string
}

func (e *Event) Error() string {
	return e.Message
}

// RegistrationError is a media driver error for an asynchronous subscription/publication registration
// (Java's RegistrationException).
type RegistrationError struct {
	RegistrationID int64
	ErrorCode      int32
	Cause          error
}

func (e *RegistrationError) Error() string {
	return fmt.Sprintf("registration %d failed (errorCode=%d): %v", e.RegistrationID, e.ErrorCode, e.Cause)
}

func (e *RegistrationError) Unwrap() error {
	return e.Cause
}

// IsTransient reports whether the registration can be retried (RESOURCE_TEMPORARILY_UNAVAILABLE).
func (e *RegistrationError) IsTransient() bool {
	return e.ErrorCode == command.ErrorCodeResourceTemporarilyUnavailable
}

// The aeron client reports driver registration errors only as formatted strings: "error on %d: %d: %s".
var registrationErrorPattern = regexp.MustCompile(`^error on (-?\d+): (-?\d+): `)

func toRegistrationError(registrationID int64, err error) *RegistrationError {
	code := int32(command.ErrorCodeGenericError)
	if m := registrationErrorPattern.FindStringSubmatch(err.Error()); m != nil {
		if c, perr := strconv.ParseInt(m[2], 10, 32); perr == nil {
			code = int32(c)
		}
	}
	return &RegistrationError{RegistrationID: registrationID, ErrorCode: code, Cause: err}
}

var errUnknownRegistration = errors.New("unknown registration")

func isTransient(err error) bool {
	var re *RegistrationError
	return errors.As(err, &re) && re.IsTransient()
}

func itoa(v int64) string {
	return strconv.FormatInt(v, 10)
}

func codeString(code codecs.ControlResponseCodeEnum) string {
	switch code {
	case codecs.ControlResponseCode.OK:
		return "OK"
	case codecs.ControlResponseCode.ERROR:
		return "ERROR"
	case codecs.ControlResponseCode.RECORDING_UNKNOWN:
		return "RECORDING_UNKNOWN"
	case codecs.ControlResponseCode.SUBSCRIPTION_UNKNOWN:
		return "SUBSCRIPTION_UNKNOWN"
	}
	return strconv.Itoa(int(code))
}
