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

package driver

import (
	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/command"
)

// AddStaticCounter sends ADD_STATIC_COUNTER with key and label buffers. Returns the correlation id.
func (driver *Proxy) AddStaticCounter(typeId int32, keyBuffer *atomic.Buffer, keyOffset int32, keyLength int32,
	labelBuffer *atomic.Buffer, labelOffset int32, labelLength int32, registrationId int64) (int64, error) {
	correlationID := driver.toDriverCommandBuffer.NextCorrelationID()

	filler := func(buffer *atomic.Buffer, length *int) int32 {
		var message command.StaticCounterMessage
		message.Wrap(buffer, 0)
		message.ClientID.Set(driver.clientID)
		message.CorrelationID.Set(correlationID)
		message.RegistrationID.Set(registrationId)
		message.CounterTypeID.Set(typeId)
		if keyLength > 0 {
			message.CopyKeyBuffer(keyBuffer, keyOffset, keyLength)
		}
		message.CopyLabelBuffer(labelBuffer, labelOffset, labelLength)
		*length = message.Size()
		return command.AddStaticCounter
	}

	if err := driver.writeCommandToDriver(filler); err != nil {
		return 0, err
	}
	return correlationID, nil
}

// AddStaticCounterByLabel sends ADD_STATIC_COUNTER with an empty key and the given label. Returns the correlation id.
func (driver *Proxy) AddStaticCounterByLabel(typeId int32, label string, registrationId int64) (int64, error) {
	correlationID := driver.toDriverCommandBuffer.NextCorrelationID()

	filler := func(buffer *atomic.Buffer, length *int) int32 {
		var message command.StaticCounterMessage
		message.Wrap(buffer, 0)
		message.ClientID.Set(driver.clientID)
		message.CorrelationID.Set(correlationID)
		message.RegistrationID.Set(registrationId)
		message.CounterTypeID.Set(typeId)
		message.CopyLabelString(label)
		*length = message.Size()
		return command.AddStaticCounter
	}

	if err := driver.writeCommandToDriver(filler); err != nil {
		return 0, err
	}
	return correlationID, nil
}
