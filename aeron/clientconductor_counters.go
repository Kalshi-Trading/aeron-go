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
	"fmt"
	"time"

	"github.com/lirm/aeron-go/aeron/atomic"
	ctr "github.com/lirm/aeron-go/aeron/counters"
)

// AddStaticCounter sends ADD_STATIC_COUNTER to the driver and returns the correlation id. The driver returns the
// existing counter if one with the same typeId and registrationId exists.
func (cc *ClientConductor) AddStaticCounter(typeId int32, keyBuffer *atomic.Buffer, keyOffset int32, keyLength int32,
	labelBuffer *atomic.Buffer, labelOffset int32, labelLength int32, registrationId int64) (int64, error) {
	logger.Debugf("AddStaticCounter: typeId=%d registrationId=%d", typeId, registrationId)

	if err := cc.getDriverStatus(); err != nil {
		return 0, err
	}
	if keyLength < 0 || keyLength > ctr.MaxKeyLength {
		return 0, fmt.Errorf("key length out of bounds: %d", keyLength)
	}
	if labelLength < 0 || labelLength > ctr.MaxLabelLength {
		return 0, fmt.Errorf("label length out of bounds: %d", labelLength)
	}
	cc.adminLock.Lock()
	defer cc.adminLock.Unlock()

	correlationId, err := cc.driverProxy.AddStaticCounter(
		typeId, keyBuffer, keyOffset, keyLength, labelBuffer, labelOffset, labelLength, registrationId)
	if err != nil {
		return 0, err
	}
	cc.addPendingStaticCounter(correlationId)
	return correlationId, nil
}

// AddStaticCounterByLabel is AddStaticCounter with an empty key.
func (cc *ClientConductor) AddStaticCounterByLabel(typeId int32, label string, registrationId int64) (int64, error) {
	logger.Debugf("AddStaticCounterByLabel: typeId=%d label=%s registrationId=%d", typeId, label, registrationId)

	if err := cc.getDriverStatus(); err != nil {
		return 0, err
	}
	if int32(len(label)) > ctr.MaxLabelLength {
		return 0, fmt.Errorf("label length out of bounds: %d", len(label))
	}
	cc.adminLock.Lock()
	defer cc.adminLock.Unlock()

	correlationId, err := cc.driverProxy.AddStaticCounterByLabel(typeId, label, registrationId)
	if err != nil {
		return 0, err
	}
	cc.addPendingStaticCounter(correlationId)
	return correlationId, nil
}

func (cc *ClientConductor) addPendingStaticCounter(correlationId int64) {
	counterState := new(counterStateDefn)
	counterState.Init(time.Now().UnixNano())
	counterState.isStatic = true
	cc.counters[correlationId] = counterState
}

// OnStaticCounter handles ON_STATIC_COUNTER from the driver. Unlike OnAvailableCounter no handlers are notified.
func (cc *ClientConductor) OnStaticCounter(correlationId int64, counterId int32) {
	logger.Debugf("OnStaticCounter: correlationId=%d, counterId=%d", correlationId, counterId)

	cc.adminLock.Lock()
	defer cc.adminLock.Unlock()

	if counterDef, ok := cc.counters[correlationId]; ok && counterDef.status == RegistrationStatus.AwaitingMediaDriver {
		counterDef.counterId = counterId
		counterDef.isStatic = true
		counterDef.status = RegistrationStatus.RegisteredMediaDriver
	}
}

// GetCounter is the Java-named equivalent of FindCounter: (nil, nil) while the driver has not responded.
func (cc *ClientConductor) GetCounter(correlationId int64) (*Counter, error) {
	return cc.FindCounter(correlationId)
}

// AsyncRemoveCounter asks the driver to remove a client owned counter without waiting for a response.
func (cc *ClientConductor) AsyncRemoveCounter(registrationId int64) error {
	if err := cc.getDriverStatus(); err != nil {
		return err
	}
	cc.adminLock.Lock()
	defer cc.adminLock.Unlock()

	if counterDef, ok := cc.counters[registrationId]; ok {
		if counterDef.isStatic {
			return fmt.Errorf("cannot remove static counter: registrationId=%d", registrationId)
		}
		if counterDef.counter != nil {
			counterDef.counter.internalClose()
		}
		delete(cc.counters, registrationId)
	}
	_, err := cc.driverProxy.RemoveCounter(registrationId)
	return err
}

// newCounterFromState must be called with adminLock held.
func (cc *ClientConductor) newCounterFromState(correlationId int64, counterDef *counterStateDefn) (*Counter, error) {
	if counterDef.isStatic {
		return newCounter(correlationId, cc.counterReader.GetCounterRegistrationId(counterDef.counterId), false, cc,
			counterDef.counterId)
	}
	return newCounter(correlationId, correlationId, true, cc, counterDef.counterId)
}

func (cc *ClientConductor) releaseStaticCounter(correlationId int64) {
	cc.adminLock.Lock()
	defer cc.adminLock.Unlock()
	delete(cc.counters, correlationId)
}

// onCounterUnavailable closes the local Counter when the driver removes it. Must be called with adminLock held.
func (cc *ClientConductor) onCounterUnavailable(registrationId int64) {
	if counterDef, ok := cc.counters[registrationId]; ok && !counterDef.isStatic {
		if counterDef.counter != nil {
			counterDef.counter.internalClose()
		}
		delete(cc.counters, registrationId)
	}
}
