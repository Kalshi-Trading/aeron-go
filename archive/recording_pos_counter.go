// Deprecated wrappers around package archive/status, kept for source compatibility.
// See ~aeron/aeron-archive/src/main/java/io/aeron/archive/status/RecordingPos.java

package archive

import (
	"github.com/lirm/aeron-go/aeron/counters"
	"github.com/lirm/aeron-go/archive/status"
)

// Deprecated: use status.RecordingPositionTypeId.
const RECORDING_POSITION_COUNTER_TYPE_ID = status.RecordingPositionTypeId

// Deprecated: use status.NullRecordingId.
const NULL_RECORDING_ID = status.NullRecordingId

// Deprecated: use the status.*Offset constants.
const (
	RECORDING_ID_OFFSET           = status.RecordingIdOffset
	SESSION_ID_OFFSET             = status.SessionIdOffset
	SOURCE_IDENTITY_LENGTH_OFFSET = status.SourceIdentityLengthOffset
	SOURCE_IDENTITY_OFFSET        = status.SourceIdentityOffset
)

// FindCounterIdByRecording finds the active counter id for a recording in any archive.
//
// Deprecated: use status.FindCounterIdByRecording, which can also filter by archive id.
func FindCounterIdByRecording(countersReader *counters.Reader, recordingId int64) int32 {
	return status.FindCounterIdByRecording(countersReader, recordingId, status.NullValue)
}

// FindCounterIdBySession finds the active counter id for a recorded session in any archive.
//
// Deprecated: use status.FindCounterIdBySession, which can also filter by archive id.
func FindCounterIdBySession(countersReader *counters.Reader, sessionId int32) int32 {
	return status.FindCounterIdBySession(countersReader, sessionId, status.NullValue)
}

// Deprecated: use status.GetRecordingId.
func GetRecordingId(countersReader *counters.Reader, counterId int32) int64 {
	return status.GetRecordingId(countersReader, counterId)
}

// GetSourceIdentity returns the source identity for the recording, or "" if not found.
//
// Deprecated: use status.GetSourceIdentity.
func GetSourceIdentity(countersReader *counters.Reader, counterId int32) string {
	identity, _ := status.GetSourceIdentity(countersReader, counterId)
	return identity
}

// Deprecated: use status.IsActive.
func IsRecordingActive(countersReader *counters.Reader, counterId int32, recordingId int64) bool {
	return status.IsActive(countersReader, counterId, recordingId)
}
