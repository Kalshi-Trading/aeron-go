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

package aeron

import (
	"github.com/lirm/aeron-go/aeron/atomic"
)

// AddCounter allocates a client owned counter on the media driver and blocks until it is available
// (Aeron.addCounter). The Counter should be freed by calling Counter.Close().
func (aeron *Aeron) AddCounter(typeId int32, keyBuffer *atomic.Buffer, keyOffset int32, keyLength int32,
	labelBuffer *atomic.Buffer, labelOffset int32, labelLength int32) (*Counter, error) {
	if err := validateCounterKeyAndLabelLength(keyLength, labelLength); err != nil {
		return nil, err
	}
	cc := &aeron.conductor
	return cc.addCounterSync(kindPendingCounter, func() (int64, error) {
		return cc.driverProxy.AddCounter(typeId, keyBuffer, keyOffset, keyLength, labelBuffer, labelOffset, labelLength)
	})
}

// AddCounterByLabel allocates a client owned counter with an empty key and blocks until it is available
// (Aeron.addCounter(typeId, label)).
func (aeron *Aeron) AddCounterByLabel(typeId int32, label string) (*Counter, error) {
	if err := validateCounterLabel(label); err != nil {
		return nil, err
	}
	cc := &aeron.conductor
	return cc.addCounterSync(kindPendingCounter, func() (int64, error) {
		return cc.driverProxy.AddCounterByLabel(typeId, label)
	})
}

// AsyncAddCounter sends the add counter command and returns its correlation id without waiting; poll GetCounter.
func (aeron *Aeron) AsyncAddCounter(typeId int32, keyBuffer *atomic.Buffer, keyOffset int32, keyLength int32,
	labelBuffer *atomic.Buffer, labelOffset int32, labelLength int32) (int64, error) {
	return aeron.conductor.AddCounter(typeId, keyBuffer, keyOffset, keyLength, labelBuffer, labelOffset, labelLength)
}

// AsyncAddCounterByLabel sends the add counter command with an empty key and returns its correlation id.
func (aeron *Aeron) AsyncAddCounterByLabel(typeId int32, label string) (int64, error) {
	return aeron.conductor.AddCounterByLabel(typeId, label)
}

// AddStaticCounter adds (or finds) a static counter identified by (typeId, registrationId) and blocks until it is
// available (Aeron.addStaticCounter, driver 1.45+). Static counters are not owned by the client: they are not freed
// when the client closes and calling AddStaticCounter again returns the same counter id. The driver returns an error
// if a non-static counter exists with the same typeId and registrationId.
func (aeron *Aeron) AddStaticCounter(typeId int32, keyBuffer *atomic.Buffer, keyOffset int32, keyLength int32,
	labelBuffer *atomic.Buffer, labelOffset int32, labelLength int32, registrationId int64) (*Counter, error) {
	if err := validateCounterKeyAndLabelLength(keyLength, labelLength); err != nil {
		return nil, err
	}
	cc := &aeron.conductor
	return cc.addCounterSync(kindPendingStaticCounter, func() (int64, error) {
		return cc.driverProxy.AddStaticCounter(typeId, keyBuffer, keyOffset, keyLength, labelBuffer, labelOffset,
			labelLength, registrationId)
	})
}

// AddStaticCounterByLabel is AddStaticCounter with an empty key.
func (aeron *Aeron) AddStaticCounterByLabel(typeId int32, label string, registrationId int64) (*Counter, error) {
	if err := validateCounterLabel(label); err != nil {
		return nil, err
	}
	cc := &aeron.conductor
	return cc.addCounterSync(kindPendingStaticCounter, func() (int64, error) {
		return cc.driverProxy.AddStaticCounterByLabel(typeId, label, registrationId)
	})
}

// AsyncAddStaticCounter sends the add static counter command and returns its correlation id; poll GetCounter.
func (aeron *Aeron) AsyncAddStaticCounter(typeId int32, keyBuffer *atomic.Buffer, keyOffset int32, keyLength int32,
	labelBuffer *atomic.Buffer, labelOffset int32, labelLength int32, registrationId int64) (int64, error) {
	return aeron.conductor.AddStaticCounter(
		typeId, keyBuffer, keyOffset, keyLength, labelBuffer, labelOffset, labelLength, registrationId)
}

// AsyncAddStaticCounterByLabel sends the add static counter command with an empty key.
func (aeron *Aeron) AsyncAddStaticCounterByLabel(typeId int32, label string, registrationId int64) (int64, error) {
	return aeron.conductor.AddStaticCounterByLabel(typeId, label, registrationId)
}

// GetCounter retrieves the Counter for a correlation id returned by an Async*Counter call (Aeron.getCounter). It is
// non-blocking:
//
// - If the correlation id is unknown, an error is returned.
// - If the media driver has not answered the command, (nil, nil) is returned.
// - If the media driver has added the Counter, the Counter is returned.
// - If the media driver has returned an error, or timed out, that error is returned.
func (aeron *Aeron) GetCounter(correlationId int64) (*Counter, error) {
	return aeron.conductor.GetCounter(correlationId)
}

// FindCounter is the previous name of GetCounter.
//
// Deprecated: use GetCounter.
func (aeron *Aeron) FindCounter(registrationID int64) (*Counter, error) {
	return aeron.conductor.FindCounter(registrationID)
}

// AsyncRemoveCounter asks the driver to free a client owned counter without waiting (Aeron.asyncRemoveCounter).
func (aeron *Aeron) AsyncRemoveCounter(registrationId int64) error {
	return aeron.conductor.AsyncRemoveCounter(registrationId)
}
