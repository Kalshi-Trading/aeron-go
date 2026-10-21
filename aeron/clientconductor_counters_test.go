package aeron

import (
	"github.com/lirm/aeron-go/aeron/atomic"
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

func (c *ClientConductorTestSuite) TestStaticCounterIsNotClientOwnedAndNotRemovedOnClose() {
	c.driverProxy.On("AddStaticCounterByLabel", CounterTypeId, CounterLabel, staticCounterRegId).
		Return(int64(555), nil)

	corrId, err := c.cc.AddStaticCounterByLabel(CounterTypeId, CounterLabel, staticCounterRegId)
	c.Require().NoError(err)

	counter, err := c.cc.GetCounter(corrId)
	c.Require().NoError(err)
	c.Require().Nil(counter, "pending until ON_STATIC_COUNTER")

	// The driver records the user registration id in the counter's values.
	c.cc.counterReader.ValuesBuffer().PutInt64(CounterId*128+8, staticCounterRegId)
	c.cc.OnStaticCounter(corrId, CounterId)

	counter, err = c.cc.GetCounter(corrId)
	c.Require().NoError(err)
	c.Require().NotNil(counter)
	c.Assert().False(counter.IsClientOwned())
	c.Assert().Equal(corrId, counter.CorrelationId())
	c.Assert().Equal(staticCounterRegId, counter.RegistrationId())
	c.Assert().Equal(CounterId, counter.Id())

	counter.Increment()
	c.Assert().EqualValues(1, c.cc.counterReader.GetCounterValue(CounterId))

	// Close must not send RemoveCounter (the mock would panic on an unexpected call).
	c.Require().NoError(counter.Close())
	c.Assert().True(counter.IsClosed())
	_, err = c.cc.GetCounter(corrId)
	c.Assert().Error(err)
	c.driverProxy.AssertNotCalled(c.T(), "RemoveCounter", staticCounterRegId)
}

func (c *ClientConductorTestSuite) TestStaticCounterErrorIsReturned() {
	c.driverProxy.On("AddStaticCounterByLabel", CounterTypeId, CounterLabel, staticCounterRegId).
		Return(int64(556), nil)
	corrId, err := c.cc.AddStaticCounterByLabel(CounterTypeId, CounterLabel, staticCounterRegId)
	c.Require().NoError(err)
	c.cc.OnErrorResponse(corrId, 0, "cannot add static counter, because a non-static counter exists")
	counter, err := c.cc.GetCounter(corrId)
	c.Assert().Nil(counter)
	c.Assert().ErrorContains(err, "non-static counter exists")
}

func (c *ClientConductorTestSuite) TestClientOwnedCounterClosedOnUnavailable() {
	c.driverProxy.On("AddCounterByLabel", CounterTypeId, CounterLabel).Return(CounterRegId, nil)
	id, err := c.cc.AddCounterByLabel(CounterTypeId, CounterLabel)
	c.Require().NoError(err)
	c.cc.OnAvailableCounter(CounterRegId, CounterId)
	counter, err := c.cc.GetCounter(id)
	c.Require().NoError(err)
	c.Require().True(counter.IsClientOwned())
	c.Assert().Equal(id, counter.RegistrationId())
	c.Assert().Equal(id, counter.CorrelationId())

	c.cc.OnUnavailableCounter(CounterRegId, CounterId)
	c.Assert().True(counter.IsClosed())
	_, err = c.cc.GetCounter(id)
	c.Assert().Error(err)
}

func (c *ClientConductorTestSuite) TestAsyncRemoveCounter() {
	c.driverProxy.On("AddCounterByLabel", CounterTypeId, CounterLabel).Return(CounterRegId, nil)
	c.driverProxy.On("RemoveCounter", CounterRegId).Return(int64(124), nil)
	id, err := c.cc.AddCounterByLabel(CounterTypeId, CounterLabel)
	c.Require().NoError(err)
	c.cc.OnAvailableCounter(CounterRegId, CounterId)
	counter, err := c.cc.GetCounter(id)
	c.Require().NoError(err)

	c.Require().NoError(c.cc.AsyncRemoveCounter(id))
	c.Assert().True(counter.IsClosed())
	c.driverProxy.AssertCalled(c.T(), "RemoveCounter", CounterRegId)
}
