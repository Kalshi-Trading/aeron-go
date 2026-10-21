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
	"strconv"

	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/counters"
	"github.com/lirm/aeron-go/aeron/util"
)

// ArchiveIdLabelSuffix is appended, followed by the archive id, to the label of every archive counter.
const ArchiveIdLabelSuffix = " - archiveId="

// ArchiveIdKeyOffset is the offset of the archive id in the key of the per-archive counters allocated by
// io.aeron.archive.ArchiveCounters (error count, control sessions, cycle time, recorder/replayer stats, ...).
const ArchiveIdKeyOffset int32 = 0

// ArchiveCounterKeyLength is the key length of the per-archive counters allocated by ArchiveCounters.allocate.
const ArchiveCounterKeyLength = ArchiveIdKeyOffset + util.SizeOfInt64

// FindArchiveCounter finds the first allocated counter of typeId whose key starts with archiveId
// (io.aeron.archive.ArchiveCounters.find). Returns counters.NullCounterId if not found.
//
// Use it with the Archive*TypeId constants other than RecordingPositionTypeId and ControlSessionTypeId, which have
// different key layouts (see FindCounterIdByRecording and FindControlSessionCounter).
func FindArchiveCounter(reader *counters.Reader, typeId int32, archiveId int64) int32 {
	return findFirst(reader, typeId, func(key *atomic.Buffer) bool {
		return key.GetInt64(ArchiveIdKeyOffset) == archiveId
	})
}

// GetArchiveCounterArchiveId returns the archive id from the key of a per-archive counter of typeId, or NullValue if
// counterId is not an allocated counter of that type.
func GetArchiveCounterArchiveId(reader *counters.Reader, counterId int32, typeId int32) int64 {
	var key atomic.Buffer
	if !allocatedKey(reader, counterId, typeId, &key) {
		return NullValue
	}
	return key.GetInt64(ArchiveIdKeyOffset)
}

// ArchiveIdLabel returns the label suffix the archive appends to its counter labels
// (ArchiveCounters.appendArchiveIdLabel).
func ArchiveIdLabel(archiveId int64) string {
	return ArchiveIdLabelSuffix + strconv.FormatInt(archiveId, 10)
}

// LengthOfArchiveIdLabel returns len(ArchiveIdLabel(archiveId)) without allocating.
func LengthOfArchiveIdLabel(archiveId int64) int {
	n := len(ArchiveIdLabelSuffix) + 1
	if archiveId < 0 {
		n++
		if archiveId == -1<<63 {
			return len(ArchiveIdLabelSuffix) + len("-9223372036854775808")
		}
		archiveId = -archiveId
	}
	for archiveId >= 10 {
		archiveId /= 10
		n++
	}
	return n
}

// Control session counter (io.aeron.archive.ControlSessionCounter, since 1.49.0): one per connected archive client.
// Key: archiveId (int64) followed by controlSessionId (int64).
const (
	ControlSessionTypeId                   = ArchiveControlSessionTypeId
	ControlSessionName                     = "control-session"
	ControlSessionArchiveIdKeyOffset int32 = 0
	ControlSessionIdKeyOffset              = ControlSessionArchiveIdKeyOffset + util.SizeOfInt64
)

// FindControlSessionCounter finds the counter the archive allocated for controlSessionId
// (ControlSessionCounter.findByControlSessionId). Returns counters.NullCounterId if not found.
func FindControlSessionCounter(reader *counters.Reader, archiveId int64, controlSessionId int64) int32 {
	return findFirst(reader, ControlSessionTypeId, func(key *atomic.Buffer) bool {
		return key.GetInt64(ControlSessionArchiveIdKeyOffset) == archiveId &&
			key.GetInt64(ControlSessionIdKeyOffset) == controlSessionId
	})
}

// GetControlSessionId returns the control session id of an allocated control session counter. ok is false if
// counterId is not such a counter.
func GetControlSessionId(reader *counters.Reader, counterId int32) (controlSessionId int64, ok bool) {
	var key atomic.Buffer
	if !allocatedKey(reader, counterId, ControlSessionTypeId, &key) {
		return 0, false
	}
	return key.GetInt64(ControlSessionIdKeyOffset), true
}
