package counters

import (
	"os"
	"testing"

	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/util"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const testNumCounters = 4

func newTestReader() *Reader {
	values := atomic.MakeBuffer(make([]byte, testNumCounters*CounterLength))
	meta := atomic.MakeBuffer(make([]byte, testNumCounters*MetadataLength))
	return NewReader(values, meta)
}

// allocate mimics the Java CountersManager.allocate layout.
func allocate(r *Reader, id int32, typeId int32, key []byte, label string, regId, ownerId, refId int64) {
	m := MetaDataOffset(id)
	r.metaData.PutInt32(m+TypeIdOffset, typeId)
	r.metaData.PutInt64(m+FreeForReuseDeadlineOffset, NotFreeToReuse)
	if len(key) > 0 {
		r.metaData.PutBytesSlice(m+KeyOffset, key)
	}
	lb := []byte(label)
	r.metaData.PutInt32(m+LabelOffset, int32(len(lb)))
	if len(lb) > 0 {
		r.metaData.PutBytesSlice(m+LabelOffset+util.SizeOfInt32, lb)
	}
	v := CounterOffset(id)
	r.values.PutInt64(v+RegistrationIdOffset, regId)
	r.values.PutInt64(v+OwnerIdOffset, ownerId)
	r.values.PutInt64(v+ReferenceIdOffset, refId)
	r.metaData.PutInt32Ordered(m, RecordAllocated)
}

func TestMaxCounterIdAndLastSlot(t *testing.T) {
	r := newTestReader()
	assert.EqualValues(t, testNumCounters-1, r.MaxCounterID())
	last := r.MaxCounterID()
	for id := int32(0); id <= last; id++ {
		allocate(r, id, 7, nil, "c", int64(100+id), 1, 2)
	}
	r.values.PutInt64(CounterOffset(last), 42)
	assert.EqualValues(t, 42, r.GetCounterValue(last))
	assert.True(t, r.IsCounterAllocated(last))
	assert.False(t, r.IsCounterAllocated(last+1))
	assert.EqualValues(t, 0, r.GetCounterValue(last+1))
	assert.Equal(t, last, r.FindByRegistrationID(int64(100+last)))
	_, err := NewAtomicCounter(r, last)
	assert.NoError(t, err)
	_, err = NewAtomicCounter(r, last+1)
	assert.Error(t, err)
	_, err = NewReadableCounter(r, last)
	assert.NoError(t, err)
	n := 0
	r.ForEach(func(int64, int32, string) { n++ })
	assert.Equal(t, testNumCounters, n)
}

func TestReaderAccessors(t *testing.T) {
	r := newTestReader()
	key := make([]byte, 12)
	atomic.MakeBuffer(key).PutInt64(0, 99)
	allocate(r, 0, 11, key, "heartbeat", 5, 6, 7)
	allocate(r, 1, 1001, nil, "", 8, -1, 0)
	r.metaData.PutInt32Ordered(MetaDataOffset(2), RecordReclaimed)
	r.metaData.PutInt64(MetaDataOffset(2)+FreeForReuseDeadlineOffset, 1234)

	assert.EqualValues(t, 11, r.GetCounterTypeID(0))
	assert.Equal(t, "heartbeat", r.GetCounterLabel(0))
	assert.Equal(t, "", r.GetCounterLabel(1))
	assert.EqualValues(t, 5, r.GetCounterRegistrationID(0))
	assert.EqualValues(t, 6, r.GetCounterOwnerID(0))
	assert.EqualValues(t, 7, r.GetCounterReferenceID(0))
	assert.Equal(t, RecordAllocated, r.GetCounterState(0))
	assert.Equal(t, RecordReclaimed, r.GetCounterState(2))
	assert.Equal(t, RecordUnused, r.GetCounterState(3))
	assert.Equal(t, NotFreeToReuse, r.GetFreeForReuseDeadline(0))
	assert.EqualValues(t, 1234, r.GetFreeForReuseDeadline(2))
	assert.EqualValues(t, 99, r.KeyBuffer(0).GetInt64(0))
	assert.Len(t, r.GetCounterKey(0), int(MaxKeyLength))
	assert.EqualValues(t, 0, r.FindByTypeIDAndRegistrationID(11, 5))
	assert.EqualValues(t, 1, r.FindByTypeIDAndRegistrationID(1001, 8))
	assert.Equal(t, NullCounterId, r.FindByTypeIDAndRegistrationID(11, 8))
	assert.EqualValues(t, 0, FindHeartbeatCounterIDByRegistrationID(r, HeartbeatTypeId, 99))
	assert.True(t, IsHeartbeatActive(r, 0, HeartbeatTypeId, 99))
	assert.False(t, IsHeartbeatActive(r, 1, HeartbeatTypeId, 99))

	var ids []int32
	var labels []string
	r.ForEachMetaData(func(id int32, typeId int32, keyBuffer *atomic.Buffer, label string) {
		ids = append(ids, id)
		labels = append(labels, label)
		assert.EqualValues(t, MaxKeyLength, keyBuffer.Capacity())
	})
	assert.Equal(t, []int32{0, 1}, ids)
	assert.Equal(t, []string{"heartbeat", ""}, labels)

	// Scan must not panic on an empty label.
	n := 0
	r.ScanUnsafe(func(c Counter) { n++ })
	assert.Equal(t, 2, n)

	_, err := r.GetKeyPartInt64(0, MaxKeyLength-4)
	assert.Error(t, err)
}

func TestStreamCounterAndChannelStatus(t *testing.T) {
	r := newTestReader()
	channel := "aeron:udp?endpoint=localhost:20121"
	key := make([]byte, 16+4+len(channel))
	kb := atomic.MakeBuffer(key)
	kb.PutInt64(StreamCounterRegistrationIdOffset, 77)
	kb.PutInt32(StreamCounterSessionIdOffset, 3)
	kb.PutInt32(StreamCounterStreamIdOffset, 1001)
	kb.PutInt32(StreamCounterChannelOffset, int32(len(channel)))
	copy(key[StreamCounterChannelOffset+4:], channel)
	allocate(r, 0, DriverPublisherLimitTypeId, key, PublisherLimitName, 77, 0, 0)

	ck := make([]byte, 4+len(channel))
	atomic.MakeBuffer(ck).PutInt32(0, int32(len(channel)))
	copy(ck[4:], channel)
	allocate(r, 1, DriverSendChannelStatusTypeId, ck, SendChannelStatusName, 0, 0, 0)
	r.values.PutInt64(CounterOffset(1), ChannelEndpointStatusActive)

	sid := int32(3)
	assert.EqualValues(t, 0, FindStreamCounter(r, DriverPublisherLimitTypeId, 77, &sid))
	other := int32(4)
	assert.Equal(t, NullCounterId, FindStreamCounter(r, DriverPublisherLimitTypeId, 77, &other))
	k, err := GetStreamCounterKey(r, 0)
	require.NoError(t, err)
	assert.Equal(t, StreamCounterKey{77, 3, 1001, channel}, k)
	id := FindChannelEndpointStatus(r, DriverSendChannelStatusTypeId, channel)
	assert.EqualValues(t, 1, id)
	assert.Equal(t, "ACTIVE", ChannelEndpointStatusString(r.GetCounterValue(id)))
	assert.NoError(t, ValidateCounterTypeID(r, 1, DriverSendChannelStatusTypeId))
	assert.Error(t, ValidateCounterTypeID(r, 1, DriverPublisherLimitTypeId))

	n, err := AppendToLabel(r.metaData, 0, " extra")
	require.NoError(t, err)
	assert.EqualValues(t, 6, n)
	assert.Equal(t, PublisherLimitName+" extra", r.GetCounterLabel(0))
	require.NoError(t, SetReferenceID(r.metaData, r.values, 0, 555))
	assert.EqualValues(t, 555, r.GetCounterReferenceID(0))
	_, err = AppendToLabel(r.metaData, 3, "x")
	assert.Error(t, err)
}

func TestAtomicCounterOps(t *testing.T) {
	r := newTestReader()
	allocate(r, 2, 1, nil, "x", 0, 0, 0)
	c, err := NewAtomicCounter(r, 2)
	require.NoError(t, err)
	assert.EqualValues(t, 0, c.Increment())
	assert.EqualValues(t, 1, c.IncrementPlain())
	assert.EqualValues(t, 2, c.Decrement())
	assert.EqualValues(t, 1, c.GetAndAdd(10))
	assert.EqualValues(t, 11, c.GetAndSet(5))
	assert.True(t, c.CompareAndSet(5, 6))
	assert.False(t, c.CompareAndSet(5, 7))
	assert.True(t, c.ProposeMax(10))
	assert.False(t, c.ProposeMaxOrdered(3))
	assert.EqualValues(t, 10, c.Get())
	assert.EqualValues(t, 10, r.GetCounterValue(2))
	assert.EqualValues(t, 1, c.TypeID())
	assert.Equal(t, "x", c.Label())
}

// CncFileDescriptor.checkVersion only requires the major version to match.
func TestMapFileChecksMajorVersionOnly(t *testing.T) {
	write := func(version int32) string {
		b := make([]byte, 4096)
		buf := atomic.MakeBuffer(b)
		buf.PutInt32(0, version)
		path := t.TempDir() + "/" + CncFile
		require.NoError(t, os.WriteFile(path, b, 0o644))
		return path
	}
	meta, m, err := MapFile(write(int32(util.SemanticVersionCompose(0, 2, 1))))
	require.NoError(t, err)
	require.NotNil(t, meta)
	require.NoError(t, m.Close())

	_, _, err = MapFile(write(int32(util.SemanticVersionCompose(1, 0, 0))))
	require.Error(t, err)
	_, _, err = MapFile(write(0))
	require.Error(t, err, "uninitialised CnC file")
}
