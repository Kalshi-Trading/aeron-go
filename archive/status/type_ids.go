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

import "github.com/lirm/aeron-go/aeron/counters"

// Archive counter type ids, see io.aeron.AeronCounters.ARCHIVE_*_TYPE_ID.
const (
	ArchiveRecordingPositionTypeId          = counters.ArchiveRecordingPositionTypeId
	ArchiveErrorCountTypeId                 = counters.ArchiveErrorCountTypeId
	ArchiveControlSessionsTypeId            = counters.ArchiveControlSessionsTypeId
	ArchiveMaxCycleTimeTypeId               = counters.ArchiveMaxCycleTimeTypeId
	ArchiveCycleTimeThresholdExceededTypeId = counters.ArchiveCycleTimeThresholdExceededTypeId
	ArchiveRecorderMaxWriteTimeTypeId       = counters.ArchiveRecorderMaxWriteTimeTypeId
	ArchiveRecorderTotalWriteBytesTypeId    = counters.ArchiveRecorderTotalWriteBytesTypeId
	ArchiveRecorderTotalWriteTimeTypeId     = counters.ArchiveRecorderTotalWriteTimeTypeId
	ArchiveReplayerMaxReadTimeTypeId        = counters.ArchiveReplayerMaxReadTimeTypeId
	ArchiveReplayerTotalReadBytesTypeId     = counters.ArchiveReplayerTotalReadBytesTypeId
	ArchiveReplayerTotalReadTimeTypeId      = counters.ArchiveReplayerTotalReadTimeTypeId
	ArchiveRecordingSessionCountTypeId      = counters.ArchiveRecordingSessionCountTypeId
	ArchiveReplaySessionCountTypeId         = counters.ArchiveReplaySessionCountTypeId
	ArchiveControlSessionTypeId             = counters.ArchiveControlSessionTypeId
)

// NullValue is the Aeron null sentinel (Aeron.NULL_VALUE). Passed as an archiveId it matches any archive.
const NullValue = -1
