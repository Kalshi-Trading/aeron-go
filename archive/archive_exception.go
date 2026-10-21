// Copyright (C) 2026 Kalshi Trading
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

package archive

import (
	"errors"
	"fmt"
	"strconv"
)

// ArchiveErrorCode mirrors the Java ArchiveException error code constants. When the archive returns a
// ControlResponse with code ERROR, the relevantId field carries one of these values.
type ArchiveErrorCode int32

// Error codes as defined by io.aeron.archive.client.ArchiveException.
const (
	ErrorCodeGeneric                      ArchiveErrorCode = 0
	ErrorCodeActiveListing                ArchiveErrorCode = 1
	ErrorCodeActiveRecording              ArchiveErrorCode = 2
	ErrorCodeActiveSubscription           ArchiveErrorCode = 3
	ErrorCodeUnknownSubscription          ArchiveErrorCode = 4
	ErrorCodeUnknownRecording             ArchiveErrorCode = 5
	ErrorCodeUnknownReplay                ArchiveErrorCode = 6
	ErrorCodeMaxReplays                   ArchiveErrorCode = 7
	ErrorCodeMaxRecordings                ArchiveErrorCode = 8
	ErrorCodeInvalidExtension             ArchiveErrorCode = 9
	ErrorCodeAuthenticationRejected       ArchiveErrorCode = 10
	ErrorCodeStorageSpace                 ArchiveErrorCode = 11
	ErrorCodeUnknownReplication           ArchiveErrorCode = 12
	ErrorCodeUnauthorisedAction           ArchiveErrorCode = 13
	ErrorCodeReplicationConnectionFailure ArchiveErrorCode = 14
	ErrorCodeEmptyRecording               ArchiveErrorCode = 15
	ErrorCodeInvalidPosition              ArchiveErrorCode = 16
)

var archiveErrorCodeNames = [...]string{
	"GENERIC",
	"ACTIVE_LISTING",
	"ACTIVE_RECORDING",
	"ACTIVE_SUBSCRIPTION",
	"UNKNOWN_SUBSCRIPTION",
	"UNKNOWN_RECORDING",
	"UNKNOWN_REPLAY",
	"MAX_REPLAYS",
	"MAX_RECORDINGS",
	"INVALID_EXTENSION",
	"AUTHENTICATION_REJECTED",
	"STORAGE_SPACE",
	"UNKNOWN_REPLICATION",
	"UNAUTHORISED_ACTION",
	"REPLICATION_CONNECTION_FAILURE",
	"EMPTY_RECORDING",
	"INVALID_POSITION",
}

// String returns the Java constant name for the error code, matching ArchiveException.errorCodeAsString.
func (c ArchiveErrorCode) String() string {
	if c >= 0 && int(c) < len(archiveErrorCodeNames) {
		return archiveErrorCodeNames[c]
	}
	return "unknown error code: " + strconv.Itoa(int(c))
}

// ArchiveException is the error returned for archive operations that failed on the archive side, the Go
// counterpart of io.aeron.archive.client.ArchiveException.
type ArchiveException struct {
	Message       string
	ErrorCode     ArchiveErrorCode
	CorrelationId int64
}

// NewArchiveException creates an ArchiveException. Use aeron.NullValue for an unknown correlationId.
func NewArchiveException(message string, errorCode ArchiveErrorCode, correlationId int64) *ArchiveException {
	return &ArchiveException{Message: message, ErrorCode: errorCode, CorrelationId: correlationId}
}

// newArchiveResponseException builds the exception for a control response with code ERROR, matching the
// message format used by the Java AeronArchive.
func newArchiveResponseException(errorMessage string, relevantId int64, correlationId int64) *ArchiveException {
	code := ArchiveErrorCode(relevantId)
	return NewArchiveException(
		fmt.Sprintf("response for correlationId=%d, errorCode=%s, error: %s", correlationId, code, errorMessage),
		code,
		correlationId,
	)
}

func (e *ArchiveException) Error() string {
	return "ArchiveException: " + e.Message
}

// Is reports whether target is an *ArchiveException with the same ErrorCode, so that
// errors.Is(err, &ArchiveException{ErrorCode: ErrorCodeUnknownRecording}) works.
func (e *ArchiveException) Is(target error) bool {
	var t *ArchiveException
	if !errors.As(target, &t) || t == nil {
		return false
	}
	return t.ErrorCode == e.ErrorCode
}

// ArchiveErrorCodeOf returns the ArchiveErrorCode of err if it is, or wraps, an *ArchiveException.
func ArchiveErrorCodeOf(err error) (ArchiveErrorCode, bool) {
	var e *ArchiveException
	if errors.As(err, &e) {
		return e.ErrorCode, true
	}
	return ErrorCodeGeneric, false
}
