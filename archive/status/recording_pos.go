// Copyright 2026 Kalshi Trading
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

package status

import (
	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/counters"
	"github.com/lirm/aeron-go/aeron/util"
)

// RecordingPos counter (io.aeron.archive.status.RecordingPos): the position a recording has reached when being
// archived. Its value can be passed as the limit counter of a bounded replay.
//
// Key layout:
//
//	 0                   1                   2                   3
//	 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
//	+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//	|                        Recording ID                           |
//	|                                                               |
//	+---------------------------------------------------------------+
//	|                         Session ID                            |
//	+---------------------------------------------------------------+
//	|                Source Identity Length                         |
//	+---------------------------------------------------------------+
//	|                Source Identity (ASCII)                       ...
//	...                                                             |
//	+---------------------------------------------------------------+
//	|                         Archive ID                            |
//	|                                                               |
//	+---------------------------------------------------------------+
const (
	RecordingPositionTypeId = ArchiveRecordingPositionTypeId
	// NullRecordingId is returned when a recording id cannot be found.
	NullRecordingId int64 = NullValue
	// RecordingPosName is the label prefix of the counter.
	RecordingPosName = "rec-pos"

	RecordingIdOffset          int32 = 0
	SessionIdOffset                  = RecordingIdOffset + util.SizeOfInt64
	SourceIdentityLengthOffset       = SessionIdOffset + util.SizeOfInt32
	SourceIdentityOffset             = SourceIdentityLengthOffset + util.SizeOfInt32
	// MaxSourceIdentityLength is the longest source identity the archive stores in the key; longer ones are truncated.
	MaxSourceIdentityLength = counters.MaxKeyLength - SourceIdentityOffset - util.SizeOfInt64
)

// recordingKeyArchiveId returns the archive id stored after the source identity, or false if the key is malformed.
func recordingKeyArchiveId(key *atomic.Buffer) (int64, bool) {
	length := key.GetInt32(SourceIdentityLengthOffset)
	if length < 0 || length > MaxSourceIdentityLength {
		return 0, false
	}
	return key.GetInt64(SourceIdentityOffset + length), true
}

func matchesArchive(key *atomic.Buffer, archiveId int64) bool {
	if archiveId == NullValue {
		return true
	}
	id, ok := recordingKeyArchiveId(key)
	return ok && id == archiveId
}

// FindCounterIdByRecording finds the active RecordingPos counter id for a recording. Pass NullValue as archiveId to
// match a recording in any archive sharing the driver.
// Returns counters.NullCounterId if not found.
func FindCounterIdByRecording(reader *counters.Reader, recordingId int64, archiveId int64) int32 {
	return findFirst(reader, RecordingPositionTypeId, func(key *atomic.Buffer) bool {
		return key.GetInt64(RecordingIdOffset) == recordingId && matchesArchive(key, archiveId)
	})
}

// FindCounterIdBySession finds the active RecordingPos counter id for the recorded image session id. Pass NullValue
// as archiveId to match a recording in any archive sharing the driver.
// Returns counters.NullCounterId if not found.
func FindCounterIdBySession(reader *counters.Reader, sessionId int32, archiveId int64) int32 {
	return findFirst(reader, RecordingPositionTypeId, func(key *atomic.Buffer) bool {
		return key.GetInt32(SessionIdOffset) == sessionId && matchesArchive(key, archiveId)
	})
}

// GetRecordingId returns the recording id of an active RecordingPos counter, or NullRecordingId.
func GetRecordingId(reader *counters.Reader, counterId int32) int64 {
	var key atomic.Buffer
	if !allocatedKey(reader, counterId, RecordingPositionTypeId, &key) {
		return NullRecordingId
	}
	return key.GetInt64(RecordingIdOffset)
}

// GetSessionId returns the recorded image session id of an active RecordingPos counter. ok is false if counterId is
// not an active RecordingPos counter.
func GetSessionId(reader *counters.Reader, counterId int32) (sessionId int32, ok bool) {
	var key atomic.Buffer
	if !allocatedKey(reader, counterId, RecordingPositionTypeId, &key) {
		return 0, false
	}
	return key.GetInt32(SessionIdOffset), true
}

// GetSourceIdentity returns the Image.SourceIdentity() of the recorded image. ok is false if counterId is not an
// active RecordingPos counter (Java returns null).
func GetSourceIdentity(reader *counters.Reader, counterId int32) (sourceIdentity string, ok bool) {
	var key atomic.Buffer
	if !allocatedKey(reader, counterId, RecordingPositionTypeId, &key) {
		return "", false
	}
	length := key.GetInt32(SourceIdentityLengthOffset)
	if length < 0 || length > MaxSourceIdentityLength {
		return "", false
	}
	return string(key.GetBytesArray(SourceIdentityOffset, length)), true
}

// GetArchiveId returns the id of the archive that owns an active RecordingPos counter, or NullValue.
func GetArchiveId(reader *counters.Reader, counterId int32) int64 {
	var key atomic.Buffer
	if !allocatedKey(reader, counterId, RecordingPositionTypeId, &key) {
		return NullValue
	}
	if id, ok := recordingKeyArchiveId(&key); ok {
		return id
	}
	return NullValue
}

// IsActive reports whether counterId is still an allocated RecordingPos counter for recordingId. Use it to detect
// that a recording has stopped (and the counter has been freed or reused).
func IsActive(reader *counters.Reader, counterId int32, recordingId int64) bool {
	var key atomic.Buffer
	return allocatedKey(reader, counterId, RecordingPositionTypeId, &key) &&
		key.GetInt64(RecordingIdOffset) == recordingId
}
