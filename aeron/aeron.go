// Copyright 2016 Stanislav Liberman
// Copyright 2022 Talos, Inc.
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
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/lirm/aeron-go/aeron/broadcast"
	"github.com/lirm/aeron-go/aeron/counters"
	"github.com/lirm/aeron-go/aeron/driver"
	"github.com/lirm/aeron-go/aeron/logbuffer"
	"github.com/lirm/aeron-go/aeron/logging"
	rb "github.com/lirm/aeron-go/aeron/ringbuffer"
	"github.com/lirm/aeron-go/aeron/util/memmap"
)

// NullValue is used to represent a null value for when some value is not yet set.
const NullValue = -1

// NewPublicationHandler is the handler type for new publication notification from the media driver
type NewPublicationHandler func(string, int32, int32, int64)

// NewSubscriptionHandler is the handler type for new subscription notification from the media driver
type NewSubscriptionHandler func(string, int32, int64)

// AvailableImageHandler is the handler type for image available notification from the media driver
type AvailableImageHandler func(Image)

// UnavailableImageHandler is the handler type for image unavailable notification from the media driver
type UnavailableImageHandler func(Image)

// AvailableCounterHandler is the function called by Aeron to deliver notification of a Counter being available.
// Implementations should do the minimum work for passing off state to another thread for later processing and should
// not make a reentrant call back into the Aeron instance.  Note that this is an interface instead of a function in
// order to support RemoveAvailableCounterHandler.
type AvailableCounterHandler interface {
	Handle(countersReader *counters.Reader, registrationId int64, counterId int32)
}

// UnavailableCounterHandler is for notification of Counters being removed via an Aeron client.  Note that this is an
// interface instead of a function in order to support RemoveAvailableCounterHandler.
type UnavailableCounterHandler interface {
	Handle(countersReader *counters.Reader, registrationId int64, counterId int32)
}

// Aeron is the primary interface to the media driver for managing subscriptions, publications and counters
// (io.aeron.Aeron). All methods are safe for concurrent use.
type Aeron struct {
	context            *Context
	conductor          ClientConductor
	toDriverRingBuffer rb.ManyToOne
	driverProxy        driver.Proxy

	counters *counters.MetaDataFlyweight
	cncFile  *memmap.File

	toClientsBroadcastReceiver *broadcast.Receiver
	toClientsCopyReceiver      *broadcast.CopyReceiver

	invoker *ConductorAgentInvoker

	closeOnce sync.Once
	closeErr  error
}

var logger = logging.MustGetLogger("aeron")

// Connect is the factory method used to create a new instance of Aeron based on Context settings. Like Java's
// Aeron.connect it waits up to the media driver timeout for the CnC file to appear and the driver to heartbeat,
// returning a *DriverTimeoutError otherwise.
func Connect(ctx *Context) (*Aeron, error) {
	if !ctx.inUse.CompareAndSwap(false, true) {
		return nil, NewAeronError(CategoryError, "context is already in use by another Aeron client")
	}
	if !ctx.concluded.Load() {
		if err := ctx.Conclude(); err != nil {
			ctx.inUse.Store(false)
			return nil, err
		}
	}
	aeron, err := connect(ctx)
	if err != nil {
		ctx.inUse.Store(false)
		return nil, err
	}
	return aeron, nil
}

func connect(ctx *Context) (*Aeron, error) {
	aeron := new(Aeron)
	aeron.context = ctx
	logger.Debugf("Connecting with context: %v", ctx)

	if err := aeron.connectToDriver(); err != nil {
		return nil, err
	}

	interServiceTo := time.Duration(aeron.counters.ClientLivenessTo.Get())
	if interServiceTo <= ctx.keepAliveInterval {
		_ = aeron.cncFile.Close()
		return nil, NewConfigurationError(fmt.Sprintf("interServiceTimeoutNs=%d <= keepAliveIntervalNs=%d",
			interServiceTo.Nanoseconds(), ctx.keepAliveInterval.Nanoseconds()))
	}

	aeron.driverProxy.Init(&aeron.toDriverRingBuffer)

	var err error
	aeron.toClientsBroadcastReceiver, err = broadcast.NewReceiver(aeron.counters.ToClientsBuf.Get())
	if err != nil {
		_ = aeron.cncFile.Close()
		return nil, err
	}

	aeron.toClientsCopyReceiver = broadcast.NewCopyReceiver(aeron.toClientsBroadcastReceiver)

	ctx.interServiceTo = interServiceTo
	ctx.clientID = aeron.driverProxy.ClientID()

	cc := &aeron.conductor
	cc.Init(&aeron.driverProxy, aeron.toClientsCopyReceiver, ctx.interServiceTo, ctx.mediaDriverTo,
		ctx.publicationConnectionTo, ctx.resourceLingerTo, aeron.counters)

	cc.onAvailableImageHandler = ctx.availableImageHandler
	cc.onUnavailableImageHandler = ctx.unavailableImageHandler
	cc.onNewPublicationHandler = ctx.newPublicationHandler
	cc.onNewSubscriptionHandler = ctx.newSubscriptionHandler
	cc.publicationErrorFrameHandler = ctx.publicationErrorFrameHandler
	cc.keepAliveIntervalNs = ctx.keepAliveInterval.Nanoseconds()
	cc.closeLingerNs = ctx.closeLingerDuration.Nanoseconds()
	cc.clientName = ctx.clientName
	cc.preTouchMappedMemory = ctx.preTouchMappedMemory
	cc.subscriberErrorHandler = ctx.subscriberErrorHandler
	cc.awaitingIdleStrategy = ctx.awaitingIdleStrategy
	cc.driverAgentInvoker = ctx.driverAgentInvoker

	if ctx.availableCounterHandler != nil {
		cc.AddAvailableCounterHandler(ctx.availableCounterHandler)
	}
	if ctx.unavailableCounterHandler != nil {
		cc.AddUnavailableCounterHandler(ctx.unavailableCounterHandler)
	}
	if ctx.closeHandler != nil {
		cc.AddCloseHandler(ctx.closeHandler)
	}

	cc.errorHandler = ctx.errorHandler

	if ctx.useConductorAgentInvoker {
		cc.initDutyCycleTimes()
		aeron.invoker = &ConductorAgentInvoker{aeron: aeron}
	} else {
		cc.Start(ctx.idleStrategy)
	}

	return aeron, nil
}

func (aeron *Aeron) connectToDriver() error {
	ctx := aeron.context
	cnc, err := awaitCncFile(ctx.CncFileName(), ctx.mediaDriverTo)
	if err != nil {
		if errors.Is(err, ErrDriverTimeout) {
			return &DriverTimeoutError{TimeoutError{AeronError{Category: CategoryFatal, Cause: err}}}
		}
		var aeronErr CategorizedError
		if errors.As(err, &aeronErr) {
			return err
		}
		return WrapAeronError(CategoryError, "cannot connect to media driver", err)
	}
	aeron.counters = cnc.meta
	aeron.cncFile = cnc.file
	ctx.filePageSize = cnc.filePageSize()
	if ctx.filePageSize == 0 {
		ctx.filePageSize = logbuffer.PageMinSize
	}
	aeron.toDriverRingBuffer.Init(aeron.counters.ToDriverBuf.Get())
	return nil
}

// Close closes the client: all publications, subscriptions and counters are closed, close handlers are invoked, the
// driver is informed and the CnC file is unmapped. Safe to call more than once and after a driver timeout.
func (aeron *Aeron) Close() error {
	aeron.closeOnce.Do(func() {
		if err := aeron.conductor.Close(); err != nil {
			if !errors.Is(err, ErrCloseFromCallback) {
				aeron.conductor.reportError(err)
			}
			aeron.closeErr = err
		}
		// The conductor goroutine may still be running (close from one of its callbacks, or a stop timeout): only
		// unmap the CnC file once it has exited.
		aeron.conductor.runAfterExit(func() {
			if err := aeron.cncFile.Close(); err != nil && aeron.closeErr == nil {
				aeron.closeErr = err
			}
		})
		aeron.context.inUse.Store(false)
	})
	if errors.Is(aeron.closeErr, ErrCloseFromCallback) {
		// The first Close came from a conductor callback: wait for it to complete unless this one does too.
		return aeron.conductor.Close()
	}
	return aeron.closeErr
}

// IsClosed reports whether the client has been closed or has terminated (e.g. on driver timeout).
func (aeron *Aeron) IsClosed() bool {
	return aeron.conductor.IsClosed()
}

// TerminalError returns the timeout error that terminated the client, or nil if closed normally / still open.
func (aeron *Aeron) TerminalError() error {
	return aeron.conductor.TerminalError()
}

// AddCloseHandler registers a handler invoked once when the client closes. Returns an id for RemoveCloseHandler.
func (aeron *Aeron) AddCloseHandler(handler func()) int64 {
	return aeron.conductor.AddCloseHandler(handler)
}

// RemoveCloseHandler removes a close handler registered with AddCloseHandler.
func (aeron *Aeron) RemoveCloseHandler(id int64) bool {
	return aeron.conductor.RemoveCloseHandler(id)
}

// AddSubscription adds a subscription, blocking until the driver confirms it or returns an error.
func (aeron *Aeron) AddSubscription(channel string, streamID int32) (*Subscription, error) {
	return aeron.conductor.AddSubscriptionWithHandlers(channel, streamID, aeron.context.availableImageHandler,
		aeron.context.unavailableImageHandler)
}

// AddSubscriptionWithHandlers is AddSubscription with image handlers specific to this subscription.
func (aeron *Aeron) AddSubscriptionWithHandlers(channel string, streamID int32,
	onAvailableImage AvailableImageHandler, onUnavailableImage UnavailableImageHandler) (*Subscription, error) {
	return aeron.conductor.AddSubscriptionWithHandlers(channel, streamID, onAvailableImage, onUnavailableImage)
}

// AddSubscriptionDeprecated adds a subscription without blocking the caller. The returned channel receives the
// subscription (or nil if the add failed, after the error was passed to the context's error handler) and is then
// closed.
//
// Deprecated: use AddSubscription, or AsyncAddSubscription with GetSubscription.
func (aeron *Aeron) AddSubscriptionDeprecated(channel string, streamID int32) chan *Subscription {
	ch := make(chan *Subscription, 1)
	go func() {
		sub, err := aeron.AddSubscription(channel, streamID)
		if err != nil {
			aeron.conductor.reportError(err)
		}
		ch <- sub
		close(ch)
	}()
	return ch
}

// AsyncAddSubscription sends ADD_SUBSCRIPTION without blocking; poll GetSubscription with the returned id.
func (aeron *Aeron) AsyncAddSubscription(channel string, streamID int32) (int64, error) {
	return aeron.conductor.AsyncAddSubscriptionWithHandlers(channel, streamID, aeron.context.availableImageHandler,
		aeron.context.unavailableImageHandler)
}

// AsyncAddSubscriptionWithHandlers is AsyncAddSubscription with subscription-specific image handlers.
func (aeron *Aeron) AsyncAddSubscriptionWithHandlers(channel string, streamID int32,
	onAvailableImage AvailableImageHandler, onUnavailableImage UnavailableImageHandler) (int64, error) {
	return aeron.conductor.AsyncAddSubscriptionWithHandlers(channel, streamID, onAvailableImage, onUnavailableImage)
}

// GetSubscription polls an async subscription add: (nil, nil) while pending, the subscription once ready, or the
// registration/timeout error.
func (aeron *Aeron) GetSubscription(registrationID int64) (*Subscription, error) {
	return aeron.conductor.GetSubscription(registrationID)
}

// AsyncRemoveSubscription closes a subscription by registration id without waiting for the driver.
func (aeron *Aeron) AsyncRemoveSubscription(registrationID int64) error {
	return aeron.conductor.AsyncRemoveSubscription(registrationID)
}

// AddPublication adds a concurrent publication, blocking until the driver confirms it or returns an error.
func (aeron *Aeron) AddPublication(channel string, streamID int32) (*Publication, error) {
	return aeron.conductor.AddPublication(channel, streamID)
}

// AddPublicationDeprecated adds a concurrent publication without blocking the caller. The returned channel receives
// the publication (or nil if the add failed, after the error was passed to the context's error handler) and is then
// closed.
//
// Deprecated: use AddPublication, or AsyncAddPublication with GetPublication.
func (aeron *Aeron) AddPublicationDeprecated(channel string, streamID int32) chan *Publication {
	ch := make(chan *Publication, 1)
	go func() {
		pub, err := aeron.AddPublication(channel, streamID)
		if err != nil {
			aeron.conductor.reportError(err)
		}
		ch <- pub
		close(ch)
	}()
	return ch
}

// AsyncAddPublication sends ADD_PUBLICATION without blocking; poll GetPublication with the returned id.
func (aeron *Aeron) AsyncAddPublication(channel string, streamID int32) (int64, error) {
	return aeron.conductor.AsyncAddPublication(channel, streamID)
}

// GetPublication polls an async publication add: (nil, nil) while pending.
func (aeron *Aeron) GetPublication(registrationID int64) (*Publication, error) {
	return aeron.conductor.GetPublication(registrationID)
}

// AddExclusivePublication adds an exclusive publication, blocking until the driver confirms it.
func (aeron *Aeron) AddExclusivePublication(channel string, streamID int32) (*ExclusivePublication, error) {
	return aeron.conductor.AddExclusivePublication(channel, streamID)
}

// AddExclusivePublicationDeprecated adds an exclusive publication without blocking the caller. The returned channel
// receives the publication (or nil if the add failed, after the error was passed to the context's error handler) and
// is then closed.
//
// Deprecated: use AddExclusivePublication, or AsyncAddExclusivePublication with GetExclusivePublication.
func (aeron *Aeron) AddExclusivePublicationDeprecated(channel string, streamID int32) chan *ExclusivePublication {
	ch := make(chan *ExclusivePublication, 1)
	go func() {
		pub, err := aeron.AddExclusivePublication(channel, streamID)
		if err != nil {
			aeron.conductor.reportError(err)
		}
		ch <- pub
		close(ch)
	}()
	return ch
}

// AsyncAddExclusivePublication sends ADD_EXCLUSIVE_PUBLICATION without blocking; poll GetExclusivePublication.
func (aeron *Aeron) AsyncAddExclusivePublication(channel string, streamID int32) (int64, error) {
	return aeron.conductor.AsyncAddExclusivePublication(channel, streamID)
}

// GetExclusivePublication polls an async exclusive publication add: (nil, nil) while pending.
func (aeron *Aeron) GetExclusivePublication(registrationID int64) (*ExclusivePublication, error) {
	return aeron.conductor.GetExclusivePublication(registrationID)
}

// AsyncRemovePublication closes a publication by registration id without waiting for the driver.
func (aeron *Aeron) AsyncRemovePublication(registrationID int64) error {
	return aeron.conductor.AsyncRemovePublication(registrationID)
}

// AddDestination adds a destination to a multi-destination-cast publication, waiting for the driver. Returns the
// destination registration id usable with RemoveDestinationByID.
func (aeron *Aeron) AddDestination(publicationRegistrationID int64, endpointChannel string) (int64, error) {
	return aeron.conductor.AddDestinationWithID(publicationRegistrationID, endpointChannel)
}

// RemoveDestination removes a destination, by channel, from a multi-destination-cast publication.
func (aeron *Aeron) RemoveDestination(publicationRegistrationID int64, endpointChannel string) error {
	return aeron.conductor.RemoveDestination(publicationRegistrationID, endpointChannel)
}

// RemoveDestinationByID removes a destination, by its registration id, from a multi-destination-cast publication.
func (aeron *Aeron) RemoveDestinationByID(publicationRegistrationID int64, destinationRegistrationID int64) error {
	return aeron.conductor.RemoveDestinationByID(publicationRegistrationID, destinationRegistrationID)
}

// AsyncAddDestination adds a publication destination without waiting; see IsCommandActive.
func (aeron *Aeron) AsyncAddDestination(publicationRegistrationID int64, endpointChannel string) (int64, error) {
	return aeron.conductor.AsyncAddDestination(publicationRegistrationID, endpointChannel)
}

// AsyncRemoveDestination removes a publication destination by channel without waiting.
func (aeron *Aeron) AsyncRemoveDestination(publicationRegistrationID int64, endpointChannel string) (int64, error) {
	return aeron.conductor.AsyncRemoveDestination(publicationRegistrationID, endpointChannel)
}

// AsyncRemoveDestinationByID removes a publication destination by registration id without waiting.
func (aeron *Aeron) AsyncRemoveDestinationByID(publicationRegistrationID int64, destinationRegistrationID int64) (int64, error) {
	return aeron.conductor.AsyncRemoveDestinationByID(publicationRegistrationID, destinationRegistrationID)
}

// AddRcvDestination adds a destination to a multi-destination subscription, waiting for the driver.
func (aeron *Aeron) AddRcvDestination(subscriptionRegistrationID int64, endpointChannel string) error {
	return aeron.conductor.AddRcvDestination(subscriptionRegistrationID, endpointChannel)
}

// RemoveRcvDestination removes a destination from a multi-destination subscription, waiting for the driver.
func (aeron *Aeron) RemoveRcvDestination(subscriptionRegistrationID int64, endpointChannel string) error {
	return aeron.conductor.RemoveRcvDestination(subscriptionRegistrationID, endpointChannel)
}

// AsyncAddRcvDestination adds a subscription destination without waiting.
func (aeron *Aeron) AsyncAddRcvDestination(subscriptionRegistrationID int64, endpointChannel string) (int64, error) {
	return aeron.conductor.AsyncAddRcvDestination(subscriptionRegistrationID, endpointChannel)
}

// AsyncRemoveRcvDestination removes a subscription destination without waiting.
func (aeron *Aeron) AsyncRemoveRcvDestination(subscriptionRegistrationID int64, endpointChannel string) (int64, error) {
	return aeron.conductor.AsyncRemoveRcvDestination(subscriptionRegistrationID, endpointChannel)
}

// IsCommandActive reports whether an async command is still awaiting a driver response.
func (aeron *Aeron) IsCommandActive(correlationID int64) bool {
	return aeron.conductor.IsCommandActive(correlationID)
}

// HasActiveCommands reports whether any async commands are awaiting a driver response.
func (aeron *Aeron) HasActiveCommands() bool {
	return aeron.conductor.HasActiveCommands()
}

// NextCorrelationID generates the next correlation id that is unique for the connected Media Driver.
func (aeron *Aeron) NextCorrelationID() int64 {
	return aeron.driverProxy.NextCorrelationID()
}

// ClientID returns the client identity that has been allocated for communicating with the media driver.
func (aeron *Aeron) ClientID() int64 {
	return aeron.driverProxy.ClientID()
}

// NextSessionID asks the driver for the next available session id for a stream.
func (aeron *Aeron) NextSessionID(streamID int32) (int32, error) {
	return aeron.conductor.NextSessionID(streamID)
}

// CounterReader returns a reader for the media driver counters.
func (aeron *Aeron) CounterReader() *counters.Reader {
	return aeron.conductor.CounterReader()
}

// AddAvailableCounterHandler adds a handler for counter availability; returns an id for removal.
func (aeron *Aeron) AddAvailableCounterHandler(handler AvailableCounterHandler) int64 {
	return aeron.conductor.AddAvailableCounterHandler(handler)
}

// RemoveAvailableCounterHandlerByID removes an available counter handler by id.
func (aeron *Aeron) RemoveAvailableCounterHandlerByID(registrationId int64) bool {
	return aeron.conductor.RemoveAvailableCounterHandlerByID(registrationId)
}

// RemoveAvailableCounterHandler removes an available counter handler by value.
func (aeron *Aeron) RemoveAvailableCounterHandler(handler AvailableCounterHandler) bool {
	return aeron.conductor.RemoveAvailableCounterHandler(handler)
}

// AddUnavailableCounterHandler adds a handler for counter removal; returns an id for removal.
func (aeron *Aeron) AddUnavailableCounterHandler(handler UnavailableCounterHandler) int64 {
	return aeron.conductor.AddUnavailableCounterHandler(handler)
}

// RemoveUnavailableCounterHandlerByID removes an unavailable counter handler by id.
func (aeron *Aeron) RemoveUnavailableCounterHandlerByID(registrationId int64) bool {
	return aeron.conductor.RemoveUnavailableCounterHandlerByID(registrationId)
}

// RemoveUnavailableCounterHandler removes an unavailable counter handler by value.
func (aeron *Aeron) RemoveUnavailableCounterHandler(handler UnavailableCounterHandler) bool {
	return aeron.conductor.RemoveUnavailableCounterHandler(handler)
}
