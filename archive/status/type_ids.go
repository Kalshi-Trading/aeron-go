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

// Archive counter type ids, see io.aeron.AeronCounters.ARCHIVE_*_TYPE_ID.
const (
	ArchiveRecordingPositionTypeId          int32 = 100
	ArchiveErrorCountTypeId                 int32 = 101
	ArchiveControlSessionsTypeId            int32 = 102
	ArchiveMaxCycleTimeTypeId               int32 = 103
	ArchiveCycleTimeThresholdExceededTypeId int32 = 104
	ArchiveRecorderMaxWriteTimeTypeId       int32 = 105
	ArchiveRecorderTotalWriteBytesTypeId    int32 = 106
	ArchiveRecorderTotalWriteTimeTypeId     int32 = 107
	ArchiveReplayerMaxReadTimeTypeId        int32 = 108
	ArchiveReplayerTotalReadBytesTypeId     int32 = 109
	ArchiveReplayerTotalReadTimeTypeId      int32 = 110
	ArchiveRecordingSessionCountTypeId      int32 = 111
	ArchiveReplaySessionCountTypeId         int32 = 112
	ArchiveControlSessionTypeId             int32 = 113
)

// NullValue is the Aeron null sentinel (Aeron.NULL_VALUE). Passed as an archiveId it matches any archive.
const NullValue = -1
