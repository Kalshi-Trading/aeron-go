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

package counters

import (
	"testing"

	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/stretchr/testify/assert"
)

func TestWrapKey(t *testing.T) {
	const n = 2
	metaData := atomic.MakeBuffer(make([]byte, n*MetadataLength))
	values := atomic.MakeBuffer(make([]byte, n*CounterLength))
	off := int32(1) * MetadataLength
	metaData.PutInt32(off+TypeIdOffset, 77)
	metaData.PutInt64(off+KeyOffset+8, 1234)
	metaData.PutInt32Ordered(off, RecordAllocated)
	r := NewReader(values, metaData)

	var key atomic.Buffer
	state, typeId, ok := r.WrapKey(1, &key)
	assert.True(t, ok)
	assert.Equal(t, RecordAllocated, state)
	assert.Equal(t, int32(77), typeId)
	assert.Equal(t, int32(MaxKeyLength), key.Capacity())
	assert.Equal(t, int64(1234), key.GetInt64(8))

	state, _, ok = r.WrapKey(0, &key)
	assert.True(t, ok)
	assert.Equal(t, RecordUnused, state)

	for _, id := range []int32{-1, 2, 1000} {
		_, _, ok = r.WrapKey(id, &key)
		assert.False(t, ok, "id %d", id)
	}
}
