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
	"fmt"

	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/counters"
)

// Counter is a counter allocated by the media driver for this client (io.aeron.Counter). It embeds
// *counters.AtomicCounter, so all value read/write helpers (Get, Set, Increment, GetAndAdd, CompareAndSet, ...) are
// available directly. Client owned counters are freed in the driver on Close; static counters (AddStaticCounter) are
// not owned by the client and outlive it.
type Counter struct {
	*counters.AtomicCounter
	correlationId   int64
	registrationId  int64
	clientOwned     bool
	clientConductor *ClientConductor
	counterId       int32
	isClosed        atomic.Bool
}

// NewCounter creates a client owned Counter whose correlation id and registration id are the same.
func NewCounter(
	registrationId int64,
	clientConductor *ClientConductor,
	counterId int32) (*Counter, error) {
	return newCounter(registrationId, registrationId, true, clientConductor, counterId)
}

func newCounter(correlationId int64, registrationId int64, clientOwned bool, clientConductor *ClientConductor,
	counterId int32) (*Counter, error) {
	atomicCounter, err := counters.NewAtomicCounter(clientConductor.CounterReader(), counterId)
	if err != nil {
		return nil, err
	}
	return &Counter{
		AtomicCounter:   atomicCounter,
		correlationId:   correlationId,
		registrationId:  registrationId,
		clientOwned:     clientOwned,
		clientConductor: clientConductor,
		counterId:       counterId,
	}, nil
}

// RegistrationId returns the registration id of the counter: the correlation id of the add command for client owned
// counters, or the user supplied registration id for static counters.
func (c *Counter) RegistrationId() int64 {
	return c.registrationId
}

// CorrelationId returns the correlation id of the command which added the counter.
func (c *Counter) CorrelationId() int64 {
	return c.correlationId
}

// IsClientOwned reports whether the counter is owned by this client (false for static counters).
func (c *Counter) IsClientOwned() bool {
	return c.clientOwned
}

func (c *Counter) Id() int32 {
	return c.counterId
}

// Counter returns the underlying AtomicCounter.
func (c *Counter) Counter() *counters.AtomicCounter {
	return c.AtomicCounter
}

// IsClosed reports whether Close has been called or the driver removed the counter.
func (c *Counter) IsClosed() bool {
	return c.isClosed.Get()
}

// Close releases the counter. For client owned counters the driver is asked to free it; static counters are only
// released locally.
func (c *Counter) Close() error {
	if c.isClosed.CompareAndSet(false, true) {
		if c.clientConductor != nil {
			if c.clientOwned {
				return c.clientConductor.releaseCounter(*c)
			}
			c.clientConductor.releaseStaticCounter(c.correlationId)
		}
	}
	return nil
}

func (c *Counter) internalClose() {
	c.isClosed.Set(true)
}

func (c *Counter) String() string {
	return fmt.Sprintf("Counter{id=%d, registrationId=%d, correlationId=%d, clientOwned=%t, closed=%t}",
		c.counterId, c.registrationId, c.correlationId, c.clientOwned, c.IsClosed())
}
