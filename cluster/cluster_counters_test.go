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
	"testing"

	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/counters"
	"github.com/stretchr/testify/assert"
)

func TestFindClusterCounter(t *testing.T) {
	const n = 4
	metaData := atomic.MakeBuffer(make([]byte, n*counters.MetadataLength))
	values := atomic.MakeBuffer(make([]byte, n*counters.CounterLength))
	alloc := func(id int32, typeId int32, clusterId int32, state int32) {
		off := id * counters.MetadataLength
		metaData.PutInt32(off+counters.TypeIdOffset, typeId)
		metaData.PutInt32(off+counters.KeyOffset, clusterId)
		metaData.PutInt32Ordered(off, state)
	}
	alloc(0, ClusterCommitPositionCounterTypeId, 0, counters.RecordAllocated)
	alloc(1, ClusterNodeRoleCounterTypeId, 3, counters.RecordReclaimed)
	alloc(2, ClusterNodeRoleCounterTypeId, 3, counters.RecordAllocated)
	alloc(3, ClusterNodeRoleCounterTypeId, 4, counters.RecordAllocated)
	r := counters.NewReader(values, metaData)

	assert.Equal(t, int32(0), FindClusterCounter(r, ClusterCommitPositionCounterTypeId, 0))
	assert.Equal(t, int32(2), FindClusterCounter(r, ClusterNodeRoleCounterTypeId, 3))
	assert.Equal(t, int32(3), FindClusterCounter(r, ClusterNodeRoleCounterTypeId, 4))
	assert.Equal(t, counters.NullCounterId, FindClusterCounter(r, ClusterNodeRoleCounterTypeId, 5))
	assert.Equal(t, counters.NullCounterId, FindClusterCounter(r, ClusterRecoveryStateCounterTypeId, 0))
	assert.Equal(t, " - clusterId=7", ClusterIdLabel(7))
}
