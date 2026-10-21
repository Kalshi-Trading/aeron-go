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
	"strings"
)

// ErrorCode is the error code returned by the media driver in an ON_ERROR response, mirroring io.aeron.ErrorCode.
type ErrorCode int32

const (
	// ErrorCodeUnknownCodeValue is used when the driver returns a code this client does not recognise.
	ErrorCodeUnknownCodeValue ErrorCode = -1
	// ErrorCodeUnused is reserved and not used by the driver.
	ErrorCodeUnused ErrorCode = 0
	// ErrorCodeInvalidChannel indicates the channel URI could not be parsed or is not supported.
	ErrorCodeInvalidChannel ErrorCode = 1
	// ErrorCodeUnknownSubscription indicates an operation on a subscription the driver does not know.
	ErrorCodeUnknownSubscription ErrorCode = 2
	// ErrorCodeUnknownPublication indicates an operation on a publication the driver does not know.
	ErrorCodeUnknownPublication ErrorCode = 3
	// ErrorCodeChannelEndpointError indicates the channel endpoint could not be created/bound.
	ErrorCodeChannelEndpointError ErrorCode = 4
	// ErrorCodeUnknownCounter indicates an operation on a counter the driver does not know.
	ErrorCodeUnknownCounter ErrorCode = 5
	// ErrorCodeUnknownCommandTypeID indicates the command type was not recognised by the driver.
	ErrorCodeUnknownCommandTypeID ErrorCode = 6
	// ErrorCodeMalformedCommand indicates the command was malformed.
	ErrorCodeMalformedCommand ErrorCode = 7
	// ErrorCodeNotSupported indicates the requested operation is not supported.
	ErrorCodeNotSupported ErrorCode = 8
	// ErrorCodeUnknownHost indicates a host name in a channel could not be resolved.
	ErrorCodeUnknownHost ErrorCode = 9
	// ErrorCodeResourceTemporarilyUnavailable indicates a resource is temporarily unavailable; retry may succeed.
	ErrorCodeResourceTemporarilyUnavailable ErrorCode = 10
	// ErrorCodeGenericError is a catch-all for errors without a more specific code.
	ErrorCodeGenericError ErrorCode = 11
	// ErrorCodeStorageSpace indicates the driver ran out of storage space.
	ErrorCodeStorageSpace ErrorCode = 12
	// ErrorCodeImageRejected indicates an image was rejected.
	ErrorCodeImageRejected ErrorCode = 13
	// ErrorCodePublicationRevoked indicates the publication was revoked.
	ErrorCodePublicationRevoked ErrorCode = 14
)

var errorCodeNames = [...]string{
	"UNUSED",
	"INVALID_CHANNEL",
	"UNKNOWN_SUBSCRIPTION",
	"UNKNOWN_PUBLICATION",
	"CHANNEL_ENDPOINT_ERROR",
	"UNKNOWN_COUNTER",
	"UNKNOWN_COMMAND_TYPE_ID",
	"MALFORMED_COMMAND",
	"NOT_SUPPORTED",
	"UNKNOWN_HOST",
	"RESOURCE_TEMPORARILY_UNAVAILABLE",
	"GENERIC_ERROR",
	"STORAGE_SPACE",
	"IMAGE_REJECTED",
	"PUBLICATION_REVOKED",
}

// ErrorCodeFromValue maps a wire value to an ErrorCode, returning ErrorCodeUnknownCodeValue for unknown values
// (equivalent to Java ErrorCode.get(int)).
func ErrorCodeFromValue(value int32) ErrorCode {
	if value >= 0 && int(value) < len(errorCodeNames) {
		return ErrorCode(value)
	}
	return ErrorCodeUnknownCodeValue
}

// Value returns the wire value of the code.
func (c ErrorCode) Value() int32 {
	return int32(c)
}

// String returns the Java enum name of the code.
func (c ErrorCode) String() string {
	if c >= 0 && int(c) < len(errorCodeNames) {
		return errorCodeNames[c]
	}
	return "UNKNOWN_CODE_VALUE"
}

// ErrorCategory is the severity of an AeronError, mirroring io.aeron.exceptions.AeronException.Category.
type ErrorCategory int8

const (
	// CategoryFatal means the client (or component) cannot continue and must be closed.
	CategoryFatal ErrorCategory = iota
	// CategoryError is a normal error that the application should handle.
	CategoryError
	// CategoryWarn is a warning; the operation may be retried.
	CategoryWarn
)

// String returns FATAL, ERROR or WARN.
func (c ErrorCategory) String() string {
	switch c {
	case CategoryFatal:
		return "FATAL"
	case CategoryError:
		return "ERROR"
	case CategoryWarn:
		return "WARN"
	}
	return fmt.Sprintf("Category(%d)", int8(c))
}

// CategorizedError is implemented by all Aeron error types.
type CategorizedError interface {
	error
	ErrorCategory() ErrorCategory
}

// AeronError is the base error type for Aeron, mirroring io.aeron.exceptions.AeronException. All other Aeron error
// types in this package embed it, so errors.As(err, &aeronErr) with aeronErr of type *AeronError matches any of them.
type AeronError struct {
	Category ErrorCategory
	Message  string
	Cause    error
}

// NewAeronError creates an AeronError of the given category.
func NewAeronError(category ErrorCategory, message string) *AeronError {
	return &AeronError{Category: category, Message: message}
}

// WrapAeronError creates an AeronError with an underlying cause.
func WrapAeronError(category ErrorCategory, message string, cause error) *AeronError {
	return &AeronError{Category: category, Message: message, Cause: cause}
}

// Error formats the error as Java does: "<CATEGORY> - <message>".
func (e *AeronError) Error() string {
	var sb strings.Builder
	sb.WriteString(e.Category.String())
	sb.WriteString(" - ")
	sb.WriteString(e.Message)
	if e.Cause != nil {
		if e.Message != "" {
			sb.WriteString(": ")
		}
		sb.WriteString(e.Cause.Error())
	}
	return sb.String()
}

// Unwrap returns the cause, if any.
func (e *AeronError) Unwrap() error {
	return e.Cause
}

// ErrorCategory returns the category of the error.
func (e *AeronError) ErrorCategory() ErrorCategory {
	return e.Category
}

// As allows errors.As to extract the embedded *AeronError from any error type that embeds it.
func (e *AeronError) As(target any) bool {
	if t, ok := target.(**AeronError); ok {
		*t = e
		return true
	}
	return false
}

// CategoryOf returns the category of the first CategorizedError in err's chain. Errors that are not Aeron errors
// are reported as CategoryError with ok=false.
func CategoryOf(err error) (category ErrorCategory, ok bool) {
	var ce CategorizedError
	if errors.As(err, &ce) {
		return ce.ErrorCategory(), true
	}
	return CategoryError, false
}

// IsFatal reports whether err is an Aeron error of category FATAL (equivalent to AeronException.isFatal).
func IsFatal(err error) bool {
	c, ok := CategoryOf(err)
	return ok && c == CategoryFatal
}

// RegistrationError is returned when the media driver rejects a command (publication, subscription, counter,
// destination ...), mirroring io.aeron.exceptions.RegistrationException.
type RegistrationError struct {
	AeronError
	CorrelationID  int64
	ErrorCodeValue int32
	Code           ErrorCode
}

// NewRegistrationError creates a RegistrationError from the fields of a driver ON_ERROR response.
func NewRegistrationError(correlationID int64, errorCodeValue int32, message string) *RegistrationError {
	code := ErrorCodeFromValue(errorCodeValue)
	return &RegistrationError{
		AeronError: AeronError{
			Category: controlProtocolErrorCategory(code),
			Message:  fmt.Sprintf("%s, errorCodeValue=%d", stripCategoryName(message), errorCodeValue),
		},
		CorrelationID:  correlationID,
		ErrorCodeValue: errorCodeValue,
		Code:           code,
	}
}

// ErrorCode returns the decoded error code.
func (e *RegistrationError) ErrorCode() ErrorCode {
	return e.Code
}

// ControlProtocolError is an error in the control protocol between client and driver, mirroring
// io.aeron.exceptions.ControlProtocolException.
type ControlProtocolError struct {
	AeronError
	Code ErrorCode
}

// NewControlProtocolError creates a ControlProtocolError; RESOURCE_TEMPORARILY_UNAVAILABLE is WARN, others ERROR.
func NewControlProtocolError(code ErrorCode, message string) *ControlProtocolError {
	return &ControlProtocolError{
		AeronError: AeronError{Category: controlProtocolErrorCategory(code), Message: message},
		Code:       code,
	}
}

// ChannelEndpointError is reported via the error handler when a channel endpoint (send or receive socket) fails,
// mirroring io.aeron.exceptions.ChannelEndpointException.
type ChannelEndpointError struct {
	AeronError
	StatusIndicatorID int32
}

// NewChannelEndpointError creates a ChannelEndpointError for the given channel status indicator counter id.
func NewChannelEndpointError(statusIndicatorID int32, message string) *ChannelEndpointError {
	return &ChannelEndpointError{
		AeronError:        AeronError{Category: CategoryError, Message: message},
		StatusIndicatorID: statusIndicatorID,
	}
}

// TimeoutError is the base for all Aeron timeouts, mirroring io.aeron.exceptions.TimeoutException.
// errors.As(err, &timeoutErr) with timeoutErr of type *TimeoutError matches any of the specific timeout types.
type TimeoutError struct {
	AeronError
}

// NewTimeoutError creates a TimeoutError.
func NewTimeoutError(category ErrorCategory, message string) *TimeoutError {
	return &TimeoutError{AeronError{Category: category, Message: message}}
}

// As allows errors.As to extract the embedded *TimeoutError (or *AeronError).
func (e *TimeoutError) As(target any) bool {
	if t, ok := target.(**TimeoutError); ok {
		*t = e
		return true
	}
	return e.AeronError.As(target)
}

// DriverTimeoutError indicates the media driver has not responded in time (FATAL), mirroring
// io.aeron.exceptions.DriverTimeoutException.
type DriverTimeoutError struct {
	TimeoutError
}

// NewDriverTimeoutError creates a FATAL DriverTimeoutError.
func NewDriverTimeoutError(message string) *DriverTimeoutError {
	return &DriverTimeoutError{TimeoutError{AeronError{Category: CategoryFatal, Message: message}}}
}

// ClientTimeoutError indicates the driver has timed out this client (FATAL), mirroring
// io.aeron.exceptions.ClientTimeoutException.
type ClientTimeoutError struct {
	TimeoutError
}

// NewClientTimeoutError creates a FATAL ClientTimeoutError.
func NewClientTimeoutError(message string) *ClientTimeoutError {
	return &ClientTimeoutError{TimeoutError{AeronError{Category: CategoryFatal, Message: message}}}
}

// ConductorServiceTimeoutError indicates the client conductor was not serviced within the inter-service timeout
// (FATAL), mirroring io.aeron.exceptions.ConductorServiceTimeoutException.
type ConductorServiceTimeoutError struct {
	TimeoutError
}

// NewConductorServiceTimeoutError creates a FATAL ConductorServiceTimeoutError.
func NewConductorServiceTimeoutError(message string) *ConductorServiceTimeoutError {
	return &ConductorServiceTimeoutError{TimeoutError{AeronError{Category: CategoryFatal, Message: message}}}
}

// ConfigurationError indicates invalid configuration, mirroring io.aeron.exceptions.ConfigurationException.
type ConfigurationError struct {
	AeronError
}

// NewConfigurationError creates a ConfigurationError.
func NewConfigurationError(message string) *ConfigurationError {
	return &ConfigurationError{AeronError{Category: CategoryError, Message: message}}
}

// StorageSpaceError indicates insufficient storage space, mirroring io.aeron.exceptions.StorageSpaceException.
type StorageSpaceError struct {
	AeronError
}

// NewStorageSpaceError creates a StorageSpaceError.
func NewStorageSpaceError(message string) *StorageSpaceError {
	return &StorageSpaceError{AeronError{Category: CategoryError, Message: message}}
}

// IsStorageSpaceError reports whether err (or its chain) is a StorageSpaceError or an OS "no space left" error,
// mirroring StorageSpaceException.isStorageSpaceError.
func IsStorageSpaceError(err error) bool {
	for cause := err; cause != nil; cause = errors.Unwrap(cause) {
		if _, ok := cause.(*StorageSpaceError); ok {
			return true
		}
		msg := cause.Error()
		if strings.HasSuffix(msg, "no space left on device") || strings.HasSuffix(msg, "No space left on device") ||
			strings.HasSuffix(msg, "There is not enough space on the disk") {
			return true
		}
	}
	return false
}

// AeronEvent is a lightweight, non-exceptional notification carried through an error handler (default WARN),
// mirroring io.aeron.exceptions.AeronEvent.
type AeronEvent struct {
	AeronError
}

// NewAeronEvent creates an AeronEvent with category WARN.
func NewAeronEvent(message string) *AeronEvent {
	return &AeronEvent{AeronError{Category: CategoryWarn, Message: message}}
}

func controlProtocolErrorCategory(code ErrorCode) ErrorCategory {
	if code == ErrorCodeResourceTemporarilyUnavailable {
		return CategoryWarn
	}
	return CategoryError
}

func stripCategoryName(msg string) string {
	if len(msg) > 7 {
		if strings.HasPrefix(msg, "ERROR - ") || strings.HasPrefix(msg, "FATAL - ") {
			return msg[8:]
		}
		if strings.HasPrefix(msg, "WARN - ") {
			return msg[7:]
		}
	}
	return msg
}
