// Copyright 2022 Steven Stern
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

package aeron

import (
	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/stretchr/testify/mock"
)

type MockDriverProxy struct {
	mock.Mock
}

// AddCounter provides a mock function with given fields: typeId, keyBuffer, keyOffset, keyLength, labelBuffer, labelOffset, labelLength
func (_m *MockDriverProxy) AddCounter(typeId int32, keyBuffer *atomic.Buffer, keyOffset int32, keyLength int32, labelBuffer *atomic.Buffer, labelOffset int32, labelLength int32) (int64, error) {
	ret := _m.Called(typeId, keyBuffer, keyOffset, keyLength, labelBuffer, labelOffset, labelLength)

	var r0 int64
	if rf, ok := ret.Get(0).(func(int32, *atomic.Buffer, int32, int32, *atomic.Buffer, int32, int32) int64); ok {
		r0 = rf(typeId, keyBuffer, keyOffset, keyLength, labelBuffer, labelOffset, labelLength)
	} else {
		r0 = ret.Get(0).(int64)
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(int32, *atomic.Buffer, int32, int32, *atomic.Buffer, int32, int32) error); ok {
		r1 = rf(typeId, keyBuffer, keyOffset, keyLength, labelBuffer, labelOffset, labelLength)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// AddCounterByLabel provides a mock function with given fields: typeId, label
func (_m *MockDriverProxy) AddCounterByLabel(typeId int32, label string) (int64, error) {
	ret := _m.Called(typeId, label)

	var r0 int64
	if rf, ok := ret.Get(0).(func(int32, string) int64); ok {
		r0 = rf(typeId, label)
	} else {
		r0 = ret.Get(0).(int64)
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(int32, string) error); ok {
		r1 = rf(typeId, label)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// AddDestination provides a mock function with given fields: registrationID, channel
func (_m *MockDriverProxy) AddDestination(registrationID int64, channel string) (int64, error) {
	ret := _m.Called(registrationID, channel)

	var r0 int64
	if rf, ok := ret.Get(0).(func(int64, string) int64); ok {
		r0 = rf(registrationID, channel)
	} else {
		r0 = ret.Get(0).(int64)
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(int64, string) error); ok {
		r1 = rf(registrationID, channel)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// AddExclusivePublication provides a mock function with given fields: channel, streamID
func (_m *MockDriverProxy) AddExclusivePublication(channel string, streamID int32) (int64, error) {
	ret := _m.Called(channel, streamID)

	var r0 int64
	if rf, ok := ret.Get(0).(func(string, int32) int64); ok {
		r0 = rf(channel, streamID)
	} else {
		r0 = ret.Get(0).(int64)
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(string, int32) error); ok {
		r1 = rf(channel, streamID)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// AddPublication provides a mock function with given fields: channel, streamID
func (_m *MockDriverProxy) AddPublication(channel string, streamID int32) (int64, error) {
	ret := _m.Called(channel, streamID)

	var r0 int64
	if rf, ok := ret.Get(0).(func(string, int32) int64); ok {
		r0 = rf(channel, streamID)
	} else {
		r0 = ret.Get(0).(int64)
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(string, int32) error); ok {
		r1 = rf(channel, streamID)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// AddRcvDestination provides a mock function with given fields: registrationID, channel
func (_m *MockDriverProxy) AddRcvDestination(registrationID int64, channel string) (int64, error) {
	ret := _m.Called(registrationID, channel)

	var r0 int64
	if rf, ok := ret.Get(0).(func(int64, string) int64); ok {
		r0 = rf(registrationID, channel)
	} else {
		r0 = ret.Get(0).(int64)
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(int64, string) error); ok {
		r1 = rf(registrationID, channel)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// AddSubscription provides a mock function with given fields: channel, streamID
func (_m *MockDriverProxy) AddSubscription(channel string, streamID int32) (int64, error) {
	ret := _m.Called(channel, streamID)

	var r0 int64
	if rf, ok := ret.Get(0).(func(string, int32) int64); ok {
		r0 = rf(channel, streamID)
	} else {
		r0 = ret.Get(0).(int64)
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(string, int32) error); ok {
		r1 = rf(channel, streamID)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// ClientClose provides a mock function with given fields:
func (_m *MockDriverProxy) ClientClose() error {
	ret := _m.Called()

	var r0 error
	if rf, ok := ret.Get(0).(func() error); ok {
		r0 = rf()
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// ClientID provides a mock function with given fields:
func (_m *MockDriverProxy) ClientID() int64 {
	ret := _m.Called()

	var r0 int64
	if rf, ok := ret.Get(0).(func() int64); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(int64)
	}

	return r0
}

// NextCorrelationID provides a mock function with given fields:
func (_m *MockDriverProxy) NextCorrelationID() int64 {
	ret := _m.Called()

	var r0 int64
	if rf, ok := ret.Get(0).(func() int64); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(int64)
	}

	return r0
}

// RemoveCounter provides a mock function with given fields: registrationId
func (_m *MockDriverProxy) RemoveCounter(registrationId int64) (int64, error) {
	ret := _m.Called(registrationId)

	var r0 int64
	if rf, ok := ret.Get(0).(func(int64) int64); ok {
		r0 = rf(registrationId)
	} else {
		r0 = ret.Get(0).(int64)
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(int64) error); ok {
		r1 = rf(registrationId)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// RemoveDestination provides a mock function with given fields: registrationID, channel
func (_m *MockDriverProxy) RemoveDestination(registrationID int64, channel string) (int64, error) {
	ret := _m.Called(registrationID, channel)

	var r0 int64
	if rf, ok := ret.Get(0).(func(int64, string) int64); ok {
		r0 = rf(registrationID, channel)
	} else {
		r0 = ret.Get(0).(int64)
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(int64, string) error); ok {
		r1 = rf(registrationID, channel)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// RemovePublication provides a mock function with given fields: registrationID
func (_m *MockDriverProxy) RemovePublication(registrationID int64) error {
	ret := _m.Called(registrationID)

	var r0 error
	if rf, ok := ret.Get(0).(func(int64) error); ok {
		r0 = rf(registrationID)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// RemoveRcvDestination provides a mock function with given fields: registrationID, channel
func (_m *MockDriverProxy) RemoveRcvDestination(registrationID int64, channel string) (int64, error) {
	ret := _m.Called(registrationID, channel)

	var r0 int64
	if rf, ok := ret.Get(0).(func(int64, string) int64); ok {
		r0 = rf(registrationID, channel)
	} else {
		r0 = ret.Get(0).(int64)
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(int64, string) error); ok {
		r1 = rf(registrationID, channel)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// RemoveSubscription provides a mock function with given fields: registrationID
func (_m *MockDriverProxy) RemoveSubscription(registrationID int64) error {
	ret := _m.Called(registrationID)

	var r0 error
	if rf, ok := ret.Get(0).(func(int64) error); ok {
		r0 = rf(registrationID)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// TimeOfLastDriverKeepalive provides a mock function with given fields:
func (_m *MockDriverProxy) TimeOfLastDriverKeepalive() int64 {
	ret := _m.Called()

	var r0 int64
	if rf, ok := ret.Get(0).(func() int64); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(int64)
	}

	return r0
}

type mockConstructorTestingTNewMockDriverProxy interface {
	mock.TestingT
	Cleanup(func())
}

// NewMockDriverProxy creates a new instance of MockDriverProxy. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
func NewMockDriverProxy(t mockConstructorTestingTNewMockDriverProxy) *MockDriverProxy {
	mock := &MockDriverProxy{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}

func (_m *MockDriverProxy) RemoveDestinationByID(publicationRegistrationID int64, destinationRegistrationID int64) (int64, error) {
	ret := _m.Called(publicationRegistrationID, destinationRegistrationID)
	return ret.Get(0).(int64), ret.Error(1)
}

func (_m *MockDriverProxy) AddStaticCounter(typeID int32, keyBuffer *atomic.Buffer, keyOffset int32, keyLength int32,
	label string, registrationID int64) (int64, error) {
	ret := _m.Called(typeID, keyBuffer, keyOffset, keyLength, label, registrationID)
	return ret.Get(0).(int64), ret.Error(1)
}

func (_m *MockDriverProxy) GetNextAvailableSessionID(streamID int32) (int64, error) {
	ret := _m.Called(streamID)
	return ret.Get(0).(int64), ret.Error(1)
}
