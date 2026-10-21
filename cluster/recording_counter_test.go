package cluster

import (
	"testing"

	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/counters"
	"github.com/lirm/aeron-go/archive/status"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type recCounters struct {
	metaData, values *atomic.Buffer
	next             int32
}

func newRecCounters(n int32) *recCounters {
	return &recCounters{
		metaData: atomic.MakeBuffer(make([]byte, n*counters.MetadataLength)),
		values:   atomic.MakeBuffer(make([]byte, n*counters.CounterLength)),
	}
}

func (f *recCounters) put(id int32, typeId int32, recordingId int64, sessionId int32, archiveId int64) {
	offset := id * counters.MetadataLength
	f.metaData.PutInt32(offset+counters.TypeIdOffset, typeId)
	key := offset + counters.KeyOffset
	f.metaData.PutInt64(key+status.RecordingIdOffset, recordingId)
	f.metaData.PutInt32(key+status.SessionIdOffset, sessionId)
	f.metaData.PutInt32(key+status.SourceIdentityLengthOffset, 0)
	f.metaData.PutInt64(key+status.SourceIdentityOffset, archiveId)
	f.metaData.PutInt32Ordered(offset, counters.RecordAllocated)
}

func (f *recCounters) add(typeId int32, recordingId int64, sessionId int32, archiveId int64) int32 {
	id := f.next
	f.next++
	f.put(id, typeId, recordingId, sessionId, archiveId)
	return id
}

// Java ClusteredServiceAgent.awaitRecordingCounter uses RecordingPos.findCounterIdBySession(counters, sessionId,
// archive.archiveId()), so a recording of the same session id by another archive sharing the driver is ignored.
func TestAwaitRecordingCounterFiltersByArchiveId(t *testing.T) {
	f := newRecCounters(4)
	f.add(status.RecordingPositionTypeId, 10, 7, 2)
	mine := f.add(status.RecordingPositionTypeId, 11, 7, 1)
	agent := newTestAgent()
	agent.counters = counters.NewReader(f.values, f.metaData)

	counterId, recordingId, err := agent.awaitRecordingCounter(7, 1, nil)
	require.NoError(t, err)
	assert.Equal(t, mine, counterId)
	assert.Equal(t, int64(11), recordingId)

	counterId, recordingId, err = agent.awaitRecordingCounter(7, 2, nil)
	require.NoError(t, err)
	assert.Equal(t, int32(0), counterId)
	assert.Equal(t, int64(10), recordingId)
}

// Java awaitRecordingComplete fails once !RecordingPos.isActive(counters, counterId, recordingId): a freed counter
// that has been reused for another counter (or another recording) is not the snapshot recording any more.
func TestAwaitRecordingCompleteDetectsReusedCounter(t *testing.T) {
	for name, reuse := range map[string]func(f *recCounters, id int32){
		"other type":      func(f *recCounters, id int32) { f.put(id, 1, 11, 7, 1) },
		"other recording": func(f *recCounters, id int32) { f.put(id, status.RecordingPositionTypeId, 12, 9, 1) },
		"reclaimed": func(f *recCounters, id int32) {
			f.metaData.PutInt32Ordered(id*counters.MetadataLength, counters.RecordReclaimed)
		},
	} {
		t.Run(name, func(t *testing.T) {
			f := newRecCounters(2)
			id := f.add(status.RecordingPositionTypeId, 11, 7, 1)
			agent := newTestAgent()
			agent.counters = counters.NewReader(f.values, f.metaData)
			reuse(f, id)
			f.values.PutInt64Ordered(counters.CounterOffset(id), 64)
			err := agent.awaitRecordingComplete(11, 128, id, nil)
			require.Error(t, err)
			assert.Contains(t, err.Error(), "recording stopped unexpectedly")
		})
	}

	f := newRecCounters(2)
	id := f.add(status.RecordingPositionTypeId, 11, 7, 1)
	f.values.PutInt64Ordered(counters.CounterOffset(id), 128)
	agent := newTestAgent()
	agent.counters = counters.NewReader(f.values, f.metaData)
	assert.NoError(t, agent.awaitRecordingComplete(11, 128, id, nil))
}
