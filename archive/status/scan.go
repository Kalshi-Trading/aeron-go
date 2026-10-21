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
)

// findFirst returns the first allocated counter of typeId whose key satisfies match, scanning ids from 0 until the
// first unused record, as the Java lookups do.
func findFirst(reader *counters.Reader, typeId int32, match func(key *atomic.Buffer) bool) int32 {
	var key atomic.Buffer
	for id := int32(0); ; id++ {
		state, tid, ok := reader.WrapKey(id, &key)
		if !ok || state == counters.RecordUnused {
			return counters.NullCounterId
		}
		if state == counters.RecordAllocated && tid == typeId && match(&key) {
			return id
		}
	}
}

// allocatedKey wraps key over counterId's key if the counter is allocated and of typeId.
func allocatedKey(reader *counters.Reader, counterId int32, typeId int32, key *atomic.Buffer) bool {
	state, tid, ok := reader.WrapKey(counterId, key)
	return ok && state == counters.RecordAllocated && tid == typeId
}
