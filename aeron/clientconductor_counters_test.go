package aeron

import (
	"testing"

	"github.com/lirm/aeron-go/aeron/atomic"
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
	require.Equal(t, corrID, counter.CorrelationID())
	require.Equal(t, staticCounterRegId, counter.RegistrationID())
	require.Equal(t, CounterId, counter.ID())
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
	require.Equal(t, id, counter.RegistrationID())
	require.Equal(t, id, counter.CorrelationID())

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
