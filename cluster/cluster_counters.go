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

package cluster

import (
	"strconv"

	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/counters"
)

// Cluster counter type ids, see io.aeron.AeronCounters.CLUSTER_*_TYPE_ID.
const (
	ClusterConsensusModuleStateCounterTypeId   int32 = 200
	ClusterNodeRoleCounterTypeId               int32 = 201
	ClusterControlToggleCounterTypeId          int32 = 202
	ClusterCommitPositionCounterTypeId         int32 = 203
	ClusterRecoveryStateCounterTypeId          int32 = 204
	ClusterSnapshotCounterTypeId               int32 = 205
	ClusterElectionStateCounterTypeId          int32 = 207
	ClusterBackupStateCounterTypeId            int32 = 208
	ClusterBackupLiveLogPositionCounterTypeId  int32 = 209
	ClusterBackupQueryDeadlineCounterTypeId    int32 = 210
	ClusterBackupErrorCountCounterTypeId       int32 = 211
	ClusterConsensusModuleErrorCountTypeId     int32 = 212
	ClusterClientTimeoutCountCounterTypeId     int32 = 213
	ClusterInvalidRequestCountCounterTypeId    int32 = 214
	ClusterClusteredServiceErrorCountTypeId    int32 = 215
	ClusterMaxCycleTimeCounterTypeId           int32 = 216
	ClusterCycleTimeThresholdExceededTypeId    int32 = 217
	ClusterServiceMaxCycleTimeCounterTypeId    int32 = 218
	ClusterServiceCycleTimeThresholdExceededId int32 = 219
	ClusterStandbyStateCounterTypeId           int32 = 220
	ClusterElectionCountCounterTypeId          int32 = 238
	ClusterLeadershipTermIdCounterTypeId       int32 = 239
)

// ClusterIdLabelSuffix is appended, followed by the cluster id, to cluster counter labels.
const ClusterIdLabelSuffix = " - clusterId="

// ClusterIdLabel returns the label suffix for clusterId (ClusterCounters label convention).
func ClusterIdLabel(clusterId int32) string {
	return ClusterIdLabelSuffix + strconv.FormatInt(int64(clusterId), 10)
}

// FindClusterCounter finds the first allocated counter of typeId whose key starts with clusterId
// (io.aeron.cluster.service.ClusterCounters.find). Returns counters.NullCounterId if not found.
func FindClusterCounter(reader *counters.Reader, typeId int32, clusterId int32) int32 {
	var key atomic.Buffer
	for id := int32(0); ; id++ {
		state, tid, ok := reader.WrapKey(id, &key)
		if !ok || state == counters.RecordUnused {
			return counters.NullCounterId
		}
		if state == counters.RecordAllocated && tid == typeId && key.GetInt32(0) == clusterId {
			return id
		}
	}
}
