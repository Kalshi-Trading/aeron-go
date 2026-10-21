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
	"math"
	"strconv"
	"strings"
	"testing"

	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/counters"
	"github.com/lirm/aeron-go/archive/codecs"
	"github.com/stretchr/testify/assert"
)

func TestFindRecordingPosByRecordingAndSession(t *testing.T) {
	f := newFakeCounters(8)
	f.allocate(ArchiveErrorCountTypeId, func(k *atomic.Buffer) { k.PutInt64(0, 7) })
	a1 := f.allocate(RecordingPositionTypeId, recordingPosKey(5, 33, "127.0.0.1:4000", 1))
	a2 := f.allocate(RecordingPositionTypeId, recordingPosKey(5, 44, "aeron:ipc", 2))
	r := f.reader()

	assert.Equal(t, a1, FindCounterIdByRecording(r, 5, NullValue))
	assert.Equal(t, a1, FindCounterIdByRecording(r, 5, 1))
	assert.Equal(t, a2, FindCounterIdByRecording(r, 5, 2))
	assert.Equal(t, counters.NullCounterId, FindCounterIdByRecording(r, 5, 3))
	assert.Equal(t, counters.NullCounterId, FindCounterIdByRecording(r, 6, NullValue))

	assert.Equal(t, a1, FindCounterIdBySession(r, 33, NullValue))
	assert.Equal(t, a2, FindCounterIdBySession(r, 44, 2))
	assert.Equal(t, counters.NullCounterId, FindCounterIdBySession(r, 44, 1))
	assert.Equal(t, counters.NullCounterId, FindCounterIdBySession(r, 55, NullValue))
}

func TestFindSkipsReclaimedAndStopsAtUnused(t *testing.T) {
	f := newFakeCounters(8)
	reclaimed := f.allocate(RecordingPositionTypeId, recordingPosKey(1, 10, "x", 1))
	f.setState(reclaimed, counters.RecordReclaimed)
	live := f.allocate(RecordingPositionTypeId, recordingPosKey(1, 10, "x", 1))
	r := f.reader()
	assert.Equal(t, live, FindCounterIdByRecording(r, 1, 1))

	// A record after an unused slot is not visited, matching Java.
	f.next++
	f.allocate(RecordingPositionTypeId, recordingPosKey(2, 20, "y", 1))
	assert.Equal(t, counters.NullCounterId, FindCounterIdByRecording(r, 2, NullValue))
}

func TestFindsCounterInLastSlot(t *testing.T) {
	f := newFakeCounters(3)
	f.allocate(ArchiveErrorCountTypeId, nil)
	f.allocate(ArchiveErrorCountTypeId, nil)
	last := f.allocate(RecordingPositionTypeId, recordingPosKey(9, 99, "z", 1))
	r := f.reader()
	assert.Equal(t, int32(2), last)
	assert.Equal(t, last, FindCounterIdByRecording(r, 9, 1))
	assert.Equal(t, int64(9), GetRecordingId(r, last))
	assert.True(t, IsActive(r, last, 9))
	assert.Equal(t, counters.NullCounterId, FindCounterIdByRecording(r, 10, NullValue))
}

func TestRecordingPosAccessors(t *testing.T) {
	f := newFakeCounters(4)
	other := f.allocate(ArchiveErrorCountTypeId, func(k *atomic.Buffer) { k.PutInt64(0, 5) })
	id := f.allocate(RecordingPositionTypeId, recordingPosKey(42, -7, "192.168.0.1:5555", 123456789))
	f.setValue(id, 4096)
	r := f.reader()

	assert.Equal(t, int64(42), GetRecordingId(r, id))
	sid, ok := GetSessionId(r, id)
	assert.True(t, ok)
	assert.Equal(t, int32(-7), sid)
	src, ok := GetSourceIdentity(r, id)
	assert.True(t, ok)
	assert.Equal(t, "192.168.0.1:5555", src)
	assert.Equal(t, int64(123456789), GetArchiveId(r, id))
	assert.True(t, IsActive(r, id, 42))
	assert.False(t, IsActive(r, id, 43))
	assert.Equal(t, int64(4096), r.GetCounterValue(id))

	// Wrong type, out of range, unused.
	for _, bad := range []int32{other, -1, 3, 100} {
		assert.Equal(t, NullRecordingId, GetRecordingId(r, bad), "id %d", bad)
		_, ok := GetSourceIdentity(r, bad)
		assert.False(t, ok, "id %d", bad)
		_, ok = GetSessionId(r, bad)
		assert.False(t, ok, "id %d", bad)
		assert.Equal(t, int64(NullValue), GetArchiveId(r, bad), "id %d", bad)
		assert.False(t, IsActive(r, bad, 42), "id %d", bad)
	}

	f.setState(id, counters.RecordReclaimed)
	assert.False(t, IsActive(r, id, 42))
	assert.Equal(t, NullRecordingId, GetRecordingId(r, id))
}

func TestMaxLengthSourceIdentity(t *testing.T) {
	f := newFakeCounters(2)
	long := strings.Repeat("s", 200)
	id := f.allocate(RecordingPositionTypeId, recordingPosKey(1, 2, long, math.MaxInt64))
	r := f.reader()
	src, ok := GetSourceIdentity(r, id)
	assert.True(t, ok)
	assert.Equal(t, long[:MaxSourceIdentityLength], src)
	assert.Equal(t, int64(math.MaxInt64), GetArchiveId(r, id))
	assert.Equal(t, id, FindCounterIdByRecording(r, 1, math.MaxInt64))

	empty := f.allocate(RecordingPositionTypeId, recordingPosKey(3, 4, "", 9))
	src, ok = GetSourceIdentity(r, empty)
	assert.True(t, ok)
	assert.Equal(t, "", src)
	assert.Equal(t, int64(9), GetArchiveId(r, empty))
}

func TestCorruptSourceIdentityLength(t *testing.T) {
	f := newFakeCounters(2)
	id := f.allocate(RecordingPositionTypeId, func(k *atomic.Buffer) {
		k.PutInt64(RecordingIdOffset, 1)
		k.PutInt32(SourceIdentityLengthOffset, 10000)
	})
	r := f.reader()
	_, ok := GetSourceIdentity(r, id)
	assert.False(t, ok)
	assert.Equal(t, int64(NullValue), GetArchiveId(r, id))
	assert.Equal(t, counters.NullCounterId, FindCounterIdByRecording(r, 1, 0))
	assert.Equal(t, id, FindCounterIdByRecording(r, 1, NullValue))
}

// Mirrors ArchiveCountersTest.findReturnsFirstMatchingCounter / findReturnsNullValueIfCounterNotFound.
func TestFindArchiveCounter(t *testing.T) {
	f := newFakeCounters(8)
	typeId := int32(7)
	archiveId := int64(math.MinInt64 / 13)
	f.allocate(typeId, nil)
	f.allocate(typeId, func(k *atomic.Buffer) { k.PutInt64(0, 42) })
	f.allocate(21, func(k *atomic.Buffer) { k.PutInt64(0, archiveId) })
	c4 := f.allocate(typeId, func(k *atomic.Buffer) { k.PutInt64(0, archiveId) })
	f.allocate(typeId, func(k *atomic.Buffer) { k.PutInt64(0, archiveId) })
	r := f.reader()
	assert.Equal(t, c4, FindArchiveCounter(r, typeId, archiveId))
	assert.Equal(t, archiveId, GetArchiveCounterArchiveId(r, c4, typeId))
	assert.Equal(t, int64(NullValue), GetArchiveCounterArchiveId(r, c4, typeId+1))

	f2 := newFakeCounters(2)
	f2.allocate(42, func(k *atomic.Buffer) { k.PutInt64(0, 61312936129398123) })
	r2 := f2.reader()
	for _, tc := range []struct {
		typeId    int32
		archiveId int64
	}{{5, 8}, {42, -10}, {-19, 61312936129398123}} {
		assert.Equal(t, counters.NullCounterId, FindArchiveCounter(r2, tc.typeId, tc.archiveId))
	}
	assert.Equal(t, int32(0), FindArchiveCounter(r2, 42, 61312936129398123))
}

func TestArchiveIdLabel(t *testing.T) {
	assert.Equal(t, " - archiveId=-23462384", ArchiveIdLabel(-23462384))
	for _, id := range []int64{math.MinInt64, math.MaxInt64, 0, -1, 9, 10, 56436747823, -1235127312317278312} {
		assert.Equal(t, len(ArchiveIdLabelSuffix)+len(strconv.FormatInt(id, 10)), LengthOfArchiveIdLabel(id), "id %d", id)
		assert.Equal(t, LengthOfArchiveIdLabel(id), len(ArchiveIdLabel(id)))
	}
}

func TestFindControlSessionCounter(t *testing.T) {
	f := newFakeCounters(4)
	key := func(archiveId, sessionId int64) func(*atomic.Buffer) {
		return func(k *atomic.Buffer) {
			k.PutInt64(ControlSessionArchiveIdKeyOffset, archiveId)
			k.PutInt64(ControlSessionIdKeyOffset, sessionId)
		}
	}
	f.allocate(ControlSessionTypeId, key(1, 100))
	c2 := f.allocate(ControlSessionTypeId, key(2, 100))
	f.allocate(ArchiveControlSessionsTypeId, key(2, 200))
	r := f.reader()
	assert.Equal(t, c2, FindControlSessionCounter(r, 2, 100))
	assert.Equal(t, int32(0), FindControlSessionCounter(r, 1, 100))
	assert.Equal(t, counters.NullCounterId, FindControlSessionCounter(r, 2, 200))
	sid, ok := GetControlSessionId(r, c2)
	assert.True(t, ok)
	assert.Equal(t, int64(100), sid)
	_, ok = GetControlSessionId(r, 2)
	assert.False(t, ok)
}

func TestRecordingStateString(t *testing.T) {
	for _, s := range []codecs.RecordingStateEnum{codecs.RecordingState.INVALID, codecs.RecordingState.VALID, codecs.RecordingState.DELETED} {
		parsed, err := ParseRecordingState(RecordingStateString(s))
		assert.NoError(t, err)
		assert.Equal(t, s, parsed)
	}
	assert.Equal(t, "NULL_VAL", RecordingStateString(codecs.RecordingState.NullValue))
	assert.Equal(t, "UNKNOWN(9)", RecordingStateString(9))
	_, err := ParseRecordingState("nope")
	assert.Error(t, err)
}
