package aeron

import (
	"strings"
	"testing"

	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/counters"
	"github.com/stretchr/testify/require"
)

// AddStaticCounter provides a mock function.
func (_m *MockDriverProxy) AddStaticCounter(typeId int32, keyBuffer *atomic.Buffer, keyOffset int32, keyLength int32,
	labelBuffer *atomic.Buffer, labelOffset int32, labelLength int32, registrationId int64) (int64, error) {
	ret := _m.Called(typeId, keyBuffer, keyOffset, keyLength, labelBuffer, labelOffset, labelLength, registrationId)
	return ret.Get(0).(int64), ret.Error(1)
}

// AddStaticCounterByLabel provides a mock function.
func (_m *MockDriverProxy) AddStaticCounterByLabel(typeId int32, label string, registrationId int64) (int64, error) {
	ret := _m.Called(typeId, label, registrationId)
	return ret.Get(0).(int64), ret.Error(1)
}

const staticCounterRegId = int64(98765)

func TestStaticCounterIsNotClientOwnedAndNotRemovedOnClose(t *testing.T) {
	tc := newTestConductor(t)
	corrID, err := tc.AddStaticCounterByLabel(CounterTypeId, CounterLabel, staticCounterRegId)
	require.NoError(t, err)
	counter, err := tc.GetCounter(corrID)
	require.NoError(t, err)
	require.Nil(t, counter, "pending until ON_STATIC_COUNTER")

	// The driver records the user registration id in the counter's values.
	tc.counterReader.ValuesBuffer().PutInt64(CounterId*128+8, staticCounterRegId)
	tc.OnStaticCounter(corrID, CounterId)
	counter, err = tc.GetCounter(corrID)
	require.NoError(t, err)
	require.NotNil(t, counter)
	require.False(t, counter.IsClientOwned())
	require.Equal(t, corrID, counter.CorrelationId())
	require.Equal(t, staticCounterRegId, counter.RegistrationId())
	require.Equal(t, CounterId, counter.Id())
	counter.Increment()
	require.EqualValues(t, 1, tc.counterReader.GetCounterValue(CounterId))

	require.NoError(t, counter.Close())
	require.True(t, counter.IsClosed())
	_, err = tc.GetCounter(corrID)
	require.Error(t, err)
	require.Equal(t, 0, tc.proxy.called("RemoveCounter"))
}

func TestStaticCounterErrorIsReturned(t *testing.T) {
	tc := newTestConductor(t)
	corrID, err := tc.AddStaticCounterByLabel(CounterTypeId, CounterLabel, staticCounterRegId)
	require.NoError(t, err)
	tc.OnErrorResponse(corrID, 0, "cannot add static counter, because a non-static counter exists")
	counter, err := tc.GetCounter(corrID)
	require.Nil(t, counter)
	require.ErrorContains(t, err, "non-static counter exists")
}

func TestClientOwnedCounterClosedOnUnavailable(t *testing.T) {
	tc := newTestConductor(t)
	id, err := tc.AddCounterByLabel(CounterTypeId, CounterLabel)
	require.NoError(t, err)
	tc.OnAvailableCounter(id, CounterId)
	counter, err := tc.GetCounter(id)
	require.NoError(t, err)
	require.True(t, counter.IsClientOwned())
	require.Equal(t, id, counter.RegistrationId())
	require.Equal(t, id, counter.CorrelationId())

	tc.OnUnavailableCounter(id, CounterId)
	require.True(t, counter.IsClosed())
	_, err = tc.GetCounter(id)
	require.Error(t, err)
}

func TestAsyncRemoveCounter(t *testing.T) {
	tc := newTestConductor(t)
	id, err := tc.AddCounterByLabel(CounterTypeId, CounterLabel)
	require.NoError(t, err)
	tc.OnAvailableCounter(id, CounterId)
	counter, err := tc.GetCounter(id)
	require.NoError(t, err)
	require.NoError(t, tc.AsyncRemoveCounter(id))
	require.True(t, counter.IsClosed())
	require.Equal(t, 1, tc.proxy.called("RemoveCounter"))
}

// Java ClientConductor.asyncRemoveCounter sends REMOVE_COUNTER for a counter whose add is still in flight.
func TestAsyncRemovePendingCounterSendsRemove(t *testing.T) {
	tc := newTestConductor(t)
	id, err := tc.AddCounterByLabel(CounterTypeId, CounterLabel)
	require.NoError(t, err)
	require.NoError(t, tc.AsyncRemoveCounter(id))
	require.Equal(t, 1, tc.proxy.called("RemoveCounter"))
	tc.OnAvailableCounter(id, CounterId)
	_, err = tc.GetCounter(id)
	require.Error(t, err)
	require.NoError(t, tc.AsyncRemoveCounter(id))
	require.Equal(t, 1, tc.proxy.called("RemoveCounter"))
}

func TestAsyncRemoveCounterRejectsNonCounterAndIgnoresNull(t *testing.T) {
	tc := newTestConductor(t)
	pubID, err := tc.AsyncAddPublication("aeron:ipc", 1)
	require.NoError(t, err)
	require.Error(t, tc.AsyncRemoveCounter(pubID))
	require.NoError(t, tc.AsyncRemoveCounter(NullValue))
	require.Equal(t, 0, tc.proxy.called("RemoveCounter"))
}

// Java ClientConductor.addCounter rejects out of range key/label lengths before sending a command.
func TestAddCounterValidatesKeyAndLabelLength(t *testing.T) {
	tc := newTestConductor(t)
	buf := atomic.MakeBuffer(make([]byte, 1024))
	_, err := tc.AddCounter(CounterTypeId, buf, 0, -1, buf, 0, 1)
	require.Error(t, err)
	_, err = tc.AddCounter(CounterTypeId, buf, 0, counters.MaxKeyLength+1, buf, 0, 1)
	require.Error(t, err)
	_, err = tc.AddCounter(CounterTypeId, buf, 0, 0, buf, 0, counters.MaxLabelLength+1)
	require.Error(t, err)
	_, err = tc.AddCounter(CounterTypeId, buf, 0, 0, buf, 0, -1)
	require.Error(t, err)
	_, err = tc.AddStaticCounter(CounterTypeId, buf, 0, counters.MaxKeyLength+1, buf, 0, 1, 5)
	require.Error(t, err)
	long := strings.Repeat("x", int(counters.MaxLabelLength)+1)
	_, err = tc.AddCounterByLabel(CounterTypeId, long)
	require.Error(t, err)
	_, err = tc.AddStaticCounterByLabel(CounterTypeId, long, 5)
	require.Error(t, err)
	require.Equal(t, 0, tc.proxy.called("AddCounter")+tc.proxy.called("AddCounterByLabel")+
		tc.proxy.called("AddStaticCounter"))
	_, err = tc.AddCounter(CounterTypeId, buf, 0, counters.MaxKeyLength, buf, 0, counters.MaxLabelLength)
	require.NoError(t, err)
}

func TestNewCounterFromReader(t *testing.T) {
	values := atomic.MakeBuffer(make([]byte, 4*counters.CounterLength))
	meta := atomic.MakeBuffer(make([]byte, 4*counters.MetadataLength))
	reader := counters.NewReader(values, meta)
	_, err := NewCounterFromReader(reader, 1)
	require.Error(t, err)
	_, err = NewCounterFromReader(reader, 9)
	require.Error(t, err)
	values.PutInt64(counters.CounterOffset(1)+counters.RegistrationIdOffset, 77)
	meta.PutInt32Ordered(counters.MetaDataOffset(1), counters.RecordAllocated)
	c, err := NewCounterFromReader(reader, 1)
	require.NoError(t, err)
	require.EqualValues(t, 77, c.RegistrationId())
	require.EqualValues(t, NullValue, c.CorrelationId())
	require.EqualValues(t, 1, c.Id())
	c.Set(5)
	require.EqualValues(t, 5, reader.GetCounterValue(1))
	require.NoError(t, c.Close())
	require.True(t, c.IsClosed())
}
