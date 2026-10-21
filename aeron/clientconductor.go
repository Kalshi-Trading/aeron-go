/*
Copyright 2016-2018 Stanislav Liberman
Copyright (C) 2022 Talos, Inc.
Copyright 2026 Kalshi Trading

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package aeron

import (
	"errors"
	"fmt"
	"io"
	"runtime"
	"sync"
	"time"

	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/broadcast"
	ctr "github.com/lirm/aeron-go/aeron/counters"
	"github.com/lirm/aeron-go/aeron/driver"
	"github.com/lirm/aeron-go/aeron/idlestrategy"
	"github.com/lirm/aeron-go/aeron/logbuffer"
)

const (
	idleSleepNs           = int64(16 * time.Millisecond)
	awaitingIdleSleep     = time.Millisecond
	explicitCloseLingerNs = int64(time.Second)
	closeWaitTimeout      = 10 * time.Second

	heartbeatTypeId               = int32(11)
	heartheatRegistrationIdOffset = int32(0)

	nullValue = int64(-1)
)

// RegistrationStatus is kept for source compatibility; registration state is now tracked internally.
var RegistrationStatus = struct {
	AwaitingMediaDriver   int
	RegisteredMediaDriver int
	ErroredMediaDriver    int
}{
	0,
	1,
	2,
}

type resourceKind int

const (
	kindPendingPublication resourceKind = iota
	kindPublication
	kindPendingExclusivePublication
	kindExclusivePublication
	kindPendingSubscription
	kindSubscription
	kindPendingCounter
	kindCounter
	kindPendingStaticCounter
	kindStaticCounter
)

func (k resourceKind) isPending() bool {
	switch k {
	case kindPendingPublication, kindPendingExclusivePublication, kindPendingSubscription, kindPendingCounter,
		kindPendingStaticCounter:
		return true
	}
	return false
}

// conductorResource is an entry of ClientConductor.resources, keyed by the correlation id of the add command
// (equivalent of Java's resourceByRegIdMap + stashedChannelByRegistrationId).
type conductorResource struct {
	kind                 resourceKind
	channel              string
	streamID             int32
	timeOfRegistrationNs int64
	publication          *Publication
	logBuffers           *logbuffer.LogBuffers
	subscription         *Subscription
	counter              *Counter
}

type lingeringResource struct {
	deadlineNs int64
	resource   io.Closer
}

type IdAndAvailableCounterHandler struct {
	registrationId int64
	handler        AvailableCounterHandler
}

func NewIdAndAvailablePair(registrationId int64, handler AvailableCounterHandler) *IdAndAvailableCounterHandler {
	return &IdAndAvailableCounterHandler{registrationId: registrationId, handler: handler}
}

type IdAndUnavailableCounterHandler struct {
	registrationId int64
	handler        UnavailableCounterHandler
}

func NewIdAndUnavailablePair(registrationId int64, handler UnavailableCounterHandler) *IdAndUnavailableCounterHandler {
	return &IdAndUnavailableCounterHandler{registrationId: registrationId, handler: handler}
}

type idAndCloseHandler struct {
	id      int64
	handler func()
}

// DriverProxy is the set of media driver commands used by the ClientConductor.
type DriverProxy interface {
	ClientID() int64
	TimeOfLastDriverKeepalive() int64
	NextCorrelationID() int64

	AddSubscription(channel string, streamID int32) (int64, error)
	RemoveSubscription(registrationID int64) error

	AddPublication(channel string, streamID int32) (int64, error)
	AddExclusivePublication(channel string, streamID int32) (int64, error)
	RemovePublication(registrationID int64) error

	ClientClose() error

	AddDestination(registrationID int64, channel string) (int64, error)
	RemoveDestination(registrationID int64, channel string) (int64, error)
	RemoveDestinationByID(publicationRegistrationID int64, destinationRegistrationID int64) (int64, error)

	AddRcvDestination(registrationID int64, channel string) (int64, error)
	RemoveRcvDestination(registrationID int64, channel string) (int64, error)

	AddCounter(typeId int32, keyBuffer *atomic.Buffer, keyOffset int32, keyLength int32,
		labelBuffer *atomic.Buffer, labelOffset int32, labelLength int32) (int64, error)
	AddCounterByLabel(typeId int32, label string) (int64, error)
	RemoveCounter(registrationId int64) (int64, error)
	AddStaticCounter(typeId int32, keyBuffer *atomic.Buffer, keyOffset int32, keyLength int32,
		labelBuffer *atomic.Buffer, labelOffset int32, labelLength int32, registrationId int64) (int64, error)
	AddStaticCounterByLabel(typeId int32, label string, registrationId int64) (int64, error)

	GetNextAvailableSessionID(streamID int32) (int64, error)
}

// ImageFactory creates images on ON_AVAILABLE_IMAGE. It may panic if the log buffer cannot be mapped.
type ImageFactory func(sessionID int32, corrID int64, logFilename string, subRegId int64, sourceIdentity string,
	counterValuesBuffer *atomic.Buffer, subscriberPositionID int32) Image

type driverEventsReceiver interface {
	ReceiveMessages() int
}

// ClientConductor is the client side of the media driver protocol, mirroring io.aeron.ClientConductor.
//
// All state is guarded by adminLock (Java's clientLock). Driver events are processed while holding the lock, either
// by the conductor goroutine's duty cycle or by a caller blocked in a synchronous command (awaitResponse). User
// callbacks (image/counter/close/error handlers) are queued while the lock is held and invoked after it is released,
// so handlers may safely call back into the client.
type ClientConductor struct {
	adminLock sync.Mutex

	driverProxy           DriverProxy
	driverListenerAdapter driverEventsReceiver

	counterValuesBuffer *atomic.Buffer
	counterReader       *ctr.Reader

	resources       map[int64]*conductorResource
	asyncCommandIDs map[int64]int64
	asyncErrors     map[int64]error
	logBuffersByID  map[int64]*logbuffer.LogBuffers
	lingering       []lingeringResource

	activeCorrelationID   int64
	receivedCorrelationID int64
	driverErr             error
	nextSessionIDResult   int32

	onNewPublicationHandler      NewPublicationHandler
	onNewSubscriptionHandler     NewSubscriptionHandler
	onAvailableImageHandler      AvailableImageHandler
	onUnavailableImageHandler    UnavailableImageHandler
	publicationErrorFrameHandler PublicationErrorFrameHandler

	// Ordering is only important in that the 0-index element must be called first on [un]available counters.
	availableCounterHandlers   []*IdAndAvailableCounterHandler
	unavailableCounterHandlers []*IdAndUnavailableCounterHandler
	closeHandlers              []idAndCloseHandler

	errorHandler func(error)
	imageFactory ImageFactory
	nanoClock    func() int64

	pendingCallbacks []func()

	isClosed      atomic.Bool
	isTerminating atomic.Bool
	terminalErr   error

	started   bool
	stopOnce  sync.Once
	stopCh    chan struct{}
	doneCh    chan struct{}
	closeOnce sync.Once

	timeOfLastKeepaliveNs   int64
	timeOfLastServiceNs     int64
	driverTimeoutNs         int64
	interServiceTimeoutNs   int64
	keepAliveIntervalNs     int64
	resourceLingerTimeoutNs int64
	closeLingerNs           int64

	heartbeatTimestamp *ctr.AtomicCounter

	counterMetaDataBuffer  *atomic.Buffer
	clientName             string
	preTouchMappedMemory   bool
	subscriberErrorHandler func(error)
}

// Init is the primary initialization method for ClientConductor
func (cc *ClientConductor) Init(driverProxy DriverProxy, bcast *broadcast.CopyReceiver,
	interServiceTo, driverTo, pubConnectionTo, lingerTo time.Duration, counters *ctr.MetaDataFlyweight) *ClientConductor {

	logger.Debugf("Initializing ClientConductor with: %v %v %d %d %d", driverProxy, bcast, interServiceTo,
		driverTo, pubConnectionTo)

	cc.driverProxy = driverProxy
	if bcast != nil {
		cc.driverListenerAdapter = driver.NewAdapter(cc, bcast)
	}
	cc.interServiceTimeoutNs = interServiceTo.Nanoseconds()
	cc.driverTimeoutNs = driverTo.Nanoseconds()
	cc.resourceLingerTimeoutNs = lingerTo.Nanoseconds()
	cc.keepAliveIntervalNs = DefaultKeepAliveInterval.Nanoseconds()

	cc.counterValuesBuffer = counters.ValuesBuf.Get()
	cc.counterMetaDataBuffer = counters.MetaDataBuf.Get()
	cc.counterReader = ctr.NewReader(counters.ValuesBuf.Get(), counters.MetaDataBuf.Get())

	cc.imageFactory = DefaultImageFactory
	cc.nanoClock = func() int64 { return time.Now().UnixNano() }

	cc.resources = make(map[int64]*conductorResource)
	cc.asyncCommandIDs = make(map[int64]int64)
	cc.asyncErrors = make(map[int64]error)
	cc.logBuffersByID = make(map[int64]*logbuffer.LogBuffers)
	cc.availableCounterHandlers = make([]*IdAndAvailableCounterHandler, 0)
	cc.unavailableCounterHandlers = make([]*IdAndUnavailableCounterHandler, 0)
	cc.activeCorrelationID = nullValue
	cc.receivedCorrelationID = nullValue
	cc.stopCh = make(chan struct{})
	cc.doneCh = make(chan struct{})

	now := cc.nanoClock()
	cc.timeOfLastKeepaliveNs = now
	cc.timeOfLastServiceNs = now
	return cc
}

// Start runs the conductor duty cycle on a dedicated goroutine (locked to an OS thread).
func (cc *ClientConductor) Start(idleStrategy idlestrategy.Idler) {
	cc.adminLock.Lock()
	cc.started = true
	now := cc.nanoClock()
	cc.timeOfLastKeepaliveNs = now
	cc.timeOfLastServiceNs = now
	cc.adminLock.Unlock()
	go cc.run(idleStrategy)
}

func (cc *ClientConductor) run(idleStrategy idlestrategy.Idler) {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	defer close(cc.doneCh)
	defer cc.onClose()

	for {
		select {
		case <-cc.stopCh:
			return
		default:
		}
		workCount, terminated := cc.doWork()
		if terminated {
			return
		}
		idleStrategy.Idle(workCount)
	}
}

func (cc *ClientConductor) doWork() (workCount int, terminated bool) {
	if !cc.adminLock.TryLock() {
		return 0, false
	}
	defer cc.unlock()
	defer func() {
		if r := recover(); r != nil {
			cc.queueError(fmt.Errorf("client conductor panic: %v", r))
		}
	}()

	if cc.isTerminating.Get() || cc.isClosed.Get() {
		return 0, true
	}
	workCount, err := cc.service(nullValue)
	if err != nil {
		cc.queueError(err)
	}
	return workCount, cc.isTerminating.Get()
}

// Close stops the conductor, force closes all resources (with linger), invokes close handlers and informs the driver.
// It is idempotent and safe to call concurrently.
func (cc *ClientConductor) Close() error {
	cc.stopOnce.Do(func() { close(cc.stopCh) })
	if !cc.started {
		cc.onClose()
		return nil
	}
	select {
	case <-cc.doneCh:
		return nil
	case <-time.After(closeWaitTimeout + time.Duration(cc.closeLingerNs)):
		msg := fmt.Sprintf("failed to stop conductor after %v", closeWaitTimeout)
		logger.Warning(msg)
		return errors.New(msg)
	}
}

// onClose mirrors Java ClientConductor.onClose: force close resources, notify close handlers, linger, unmap lingering
// buffers and send CLIENT_CLOSE.
func (cc *ClientConductor) onClose() {
	cc.closeOnce.Do(func() {
		cc.adminLock.Lock()
		cc.isTerminating.Set(true)
		cc.forceCloseResources()
		for _, h := range cc.closeHandlers {
			handler := h.handler
			cc.pendingCallbacks = append(cc.pendingCallbacks, handler)
		}
		cc.unlock()

		if cc.closeLingerNs > 0 {
			time.Sleep(time.Duration(cc.closeLingerNs))
		}

		cc.adminLock.Lock()
		for _, r := range cc.lingering {
			if err := r.resource.Close(); err != nil {
				logger.Debugf("error closing lingering resource: %v", err)
			}
		}
		cc.lingering = nil
		for id, lb := range cc.logBuffersByID {
			_ = lb.Close()
			delete(cc.logBuffersByID, id)
		}
		if err := cc.driverProxy.ClientClose(); err != nil {
			logger.Debugf("failed to send client close: %v", err)
		}
		cc.isClosed.Set(true)
		cc.adminLock.Unlock()
		logger.Debugf("Closed ClientConductor")
	})
}

// IsClosed reports whether the client has been closed or has terminated due to a fatal timeout.
func (cc *ClientConductor) IsClosed() bool {
	return cc.isClosed.Get() || cc.isTerminating.Get()
}

// TerminalError returns the error that caused the client to terminate (driver/client/conductor-service timeout),
// or nil.
func (cc *ClientConductor) TerminalError() error {
	cc.adminLock.Lock()
	defer cc.adminLock.Unlock()
	return cc.terminalErr
}

func (cc *ClientConductor) unlock() {
	callbacks := cc.pendingCallbacks
	cc.pendingCallbacks = nil
	cc.adminLock.Unlock()
	for _, cb := range callbacks {
		cc.invokeCallback(cb)
	}
}

func (cc *ClientConductor) invokeCallback(cb func()) {
	defer func() {
		if r := recover(); r != nil {
			cc.reportError(fmt.Errorf("panic in aeron callback: %v", r))
		}
	}()
	cb()
}

func (cc *ClientConductor) queueCallback(cb func()) {
	cc.pendingCallbacks = append(cc.pendingCallbacks, cb)
}

func (cc *ClientConductor) queueError(err error) {
	cc.queueCallback(func() { cc.reportError(err) })
}

func (cc *ClientConductor) reportError(err error) {
	defer func() {
		if r := recover(); r != nil {
			logger.Errorf("panic in error handler: %v (reporting %v)", r, err)
		}
	}()
	if cc.errorHandler != nil {
		cc.errorHandler(err)
	} else {
		logger.Error(err)
	}
}

func (cc *ClientConductor) ensureOpen() error {
	if cc.isClosed.Get() || cc.isTerminating.Get() {
		if cc.terminalErr != nil {
			return fmt.Errorf("%w: %v", ErrClientClosed, cc.terminalErr)
		}
		return ErrClientClosed
	}
	return nil
}

func (cc *ClientConductor) terminate(err error) error {
	if cc.terminalErr == nil {
		cc.terminalErr = err
	}
	cc.isTerminating.Set(true)
	cc.forceCloseResources()
	return err
}

// service processes timeouts and driver events. Must be called with adminLock held.
func (cc *ClientConductor) service(correlationID int64) (int, error) {
	workCount, err := cc.checkTimeouts(cc.nanoClock())
	if err != nil {
		return workCount, err
	}
	if cc.driverListenerAdapter == nil || cc.isTerminating.Get() {
		return workCount, nil
	}
	n, err := cc.receiveDriverEvents()
	return workCount + n, err
}

func (cc *ClientConductor) receiveDriverEvents() (n int, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = cc.terminate(fmt.Errorf("failed to read driver events: %v", r))
		}
	}()
	return cc.driverListenerAdapter.ReceiveMessages(), nil
}

// awaitResponse services the conductor until a response for correlationID arrives (Java awaitResponse).
// Must be called with adminLock held.
func (cc *ClientConductor) awaitResponse(correlationID int64) error {
	deadline := cc.nanoClock() + cc.driverTimeoutNs
	cc.activeCorrelationID = correlationID
	cc.receivedCorrelationID = nullValue
	cc.driverErr = nil
	defer func() { cc.activeCorrelationID = nullValue }()

	for {
		if _, err := cc.service(correlationID); err != nil {
			return err
		}
		if cc.receivedCorrelationID == correlationID {
			err := cc.driverErr
			cc.driverErr = nil
			return err
		}
		if err := cc.ensureOpen(); err != nil {
			return err
		}
		if cc.nanoClock() > deadline {
			return NewDriverTimeoutError(fmt.Sprintf(
				"no response from MediaDriver within %v (correlationId=%d)",
				time.Duration(cc.driverTimeoutNs), correlationID))
		}
		time.Sleep(awaitingIdleSleep)
	}
}

func (cc *ClientConductor) checkTimeouts(nowNs int64) (int, error) {
	if nowNs-cc.timeOfLastServiceNs < idleSleepNs {
		return 0, nil
	}
	if nowNs-cc.timeOfLastServiceNs > cc.interServiceTimeoutNs {
		return 0, cc.terminate(NewConductorServiceTimeoutError(fmt.Sprintf(
			"service interval exceeded: timeout=%v, interval=%v",
			time.Duration(cc.interServiceTimeoutNs), time.Duration(nowNs-cc.timeOfLastServiceNs))))
	}
	cc.timeOfLastServiceNs = nowNs

	workCount, err := cc.checkLiveness(nowNs)
	if err != nil {
		return workCount, err
	}
	return workCount + cc.checkLingeringResources(nowNs), nil
}

func (cc *ClientConductor) checkLiveness(nowNs int64) (int, error) {
	if nowNs-cc.timeOfLastKeepaliveNs < cc.keepAliveIntervalNs {
		return 0, nil
	}
	nowMs := nowNs / int64(time.Millisecond)
	driverTimeoutMs := cc.driverTimeoutNs / int64(time.Millisecond)
	lastKeepaliveMs := cc.driverProxy.TimeOfLastDriverKeepalive()
	if nowMs > lastKeepaliveMs+driverTimeoutMs {
		if lastKeepaliveMs <= 0 {
			return 0, cc.terminate(NewDriverTimeoutError("MediaDriver has been shutdown"))
		}
		return 0, cc.terminate(NewDriverTimeoutError(fmt.Sprintf(
			"MediaDriver keepalive: age=%dms > timeout=%dms", nowMs-lastKeepaliveMs, driverTimeoutMs)))
	}

	clientID := cc.driverProxy.ClientID()
	if cc.heartbeatTimestamp != nil {
		id := cc.heartbeatTimestamp.CounterId
		registrationID, ctrErr := cc.counterReader.GetKeyPartInt64(id, heartheatRegistrationIdOffset)
		if ctrErr != nil || !cc.counterReader.IsCounterAllocated(id) ||
			cc.counterReader.GetCounterTypeId(id) != heartbeatTypeId || registrationID != clientID {
			return 0, cc.terminate(fmt.Errorf("unexpected close of heartbeat timestamp counter: %d", id))
		}
		cc.heartbeatTimestamp.Set(nowMs)
	} else {
		counterID := cc.counterReader.FindCounter(heartbeatTypeId, func(keyBuffer *atomic.Buffer) bool {
			return keyBuffer.GetInt64(heartheatRegistrationIdOffset) == clientID
		})
		if counterID != ctr.NullCounterId {
			hb, ctrErr := ctr.NewAtomicCounter(cc.counterReader, counterID)
			if ctrErr != nil {
				logger.Warningf("unable to map heartbeat counter %d: %v", counterID, ctrErr)
			} else {
				cc.heartbeatTimestamp = hb
				cc.onHeartbeatCounterFound(counterID)
				hb.Set(nowMs)
			}
		}
	}
	cc.timeOfLastKeepaliveNs = nowNs
	return 1, nil
}

func (cc *ClientConductor) checkLingeringResources(nowNs int64) int {
	workCount := 0
	remaining := cc.lingering[:0]
	for _, r := range cc.lingering {
		if nowNs-r.deadlineNs >= 0 {
			if err := r.resource.Close(); err != nil {
				logger.Debugf("error closing lingering resource: %v", err)
			}
			workCount++
		} else {
			remaining = append(remaining, r)
		}
	}
	for i := len(remaining); i < len(cc.lingering); i++ {
		cc.lingering[i] = lingeringResource{}
	}
	cc.lingering = remaining
	return workCount
}

func (cc *ClientConductor) linger(resource io.Closer, lingerNs int64) {
	if lingerNs == nullValue {
		lingerNs = cc.resourceLingerTimeoutNs
	}
	cc.lingering = append(cc.lingering, lingeringResource{cc.nanoClock() + lingerNs, resource})
}

func (cc *ClientConductor) forceCloseResources() {
	for id, res := range cc.resources {
		switch res.kind {
		case kindPublication, kindExclusivePublication:
			res.publication.isClosed.Set(true)
			cc.releaseLogBuffers(res.logBuffers, res.publication.originalRegID, nullValue)
		case kindSubscription:
			sub := res.subscription
			sub.isClosed.Set(true)
			cc.closeImages(sub.images.Empty(), sub.UnavailableImageHandler(), nullValue)
		case kindCounter:
			res.counter.isClosed.Set(true)
			cc.notifyUnavailableCounter(res.counter.RegistrationId(), res.counter.Id())
		}
		delete(cc.resources, id)
	}
}

func (cc *ClientConductor) closeImages(images []Image, handler UnavailableImageHandler, lingerNs int64) {
	for _, image := range images {
		if image == nil {
			continue
		}
		cc.linger(image, lingerNs)
		if handler != nil {
			img := image
			cc.queueCallback(func() { cc.callSubscriberHandler(handler, img) })
		}
	}
}

func (cc *ClientConductor) mapLogBuffers(registrationID int64, logFileName string) (lb *logbuffer.LogBuffers, err error) {
	if lb = cc.logBuffersByID[registrationID]; lb == nil {
		defer func() {
			if r := recover(); r != nil {
				lb, err = nil, fmt.Errorf("failed to map log buffer %s: %v", logFileName, r)
			}
		}()
		lb = cc.wrapLogBuffers(logFileName)
		cc.logBuffersByID[registrationID] = lb
	}
	lb.IncRef()
	return lb, nil
}

func (cc *ClientConductor) releaseLogBuffers(lb *logbuffer.LogBuffers, registrationID int64, lingerNs int64) {
	if lb == nil {
		return
	}
	if lb.DecRef() <= 0 {
		delete(cc.logBuffersByID, registrationID)
		cc.linger(lb, lingerNs)
	}
}

// ---------------------------------------------------------------------------------------------------------------
// Publications

// AddPublication adds a concurrent publication and blocks until the driver responds.
func (cc *ClientConductor) AddPublication(channel string, streamID int32) (*Publication, error) {
	return cc.addPublication(kindPendingPublication, channel, streamID)
}

// AddExclusivePublication adds an exclusive publication and blocks until the driver responds.
func (cc *ClientConductor) AddExclusivePublication(channel string, streamID int32) (*Publication, error) {
	return cc.addPublication(kindPendingExclusivePublication, channel, streamID)
}

func (cc *ClientConductor) addPublication(kind resourceKind, channel string, streamID int32) (*Publication, error) {
	cc.adminLock.Lock()
	defer cc.unlock()
	corrID, err := cc.sendAddPublication(kind, channel, streamID)
	if err != nil {
		return nil, err
	}
	if err := cc.awaitResponse(corrID); err != nil {
		delete(cc.resources, corrID)
		return nil, err
	}
	res := cc.resources[corrID]
	if res == nil || res.publication == nil {
		return nil, fmt.Errorf("publication %d not available after response", corrID)
	}
	return res.publication, nil
}

// AsyncAddPublication sends ADD_PUBLICATION without waiting; poll GetPublication with the returned registration id.
func (cc *ClientConductor) AsyncAddPublication(channel string, streamID int32) (int64, error) {
	return cc.asyncAdd(func() (int64, error) { return cc.sendAddPublication(kindPendingPublication, channel, streamID) })
}

// AsyncAddExclusivePublication sends ADD_EXCLUSIVE_PUBLICATION without waiting; poll GetExclusivePublication.
func (cc *ClientConductor) AsyncAddExclusivePublication(channel string, streamID int32) (int64, error) {
	return cc.asyncAdd(func() (int64, error) {
		return cc.sendAddPublication(kindPendingExclusivePublication, channel, streamID)
	})
}

func (cc *ClientConductor) asyncAdd(send func() (int64, error)) (int64, error) {
	cc.adminLock.Lock()
	defer cc.unlock()
	corrID, err := send()
	if err != nil {
		return 0, err
	}
	cc.asyncCommandIDs[corrID] = cc.nanoClock()
	return corrID, nil
}

func (cc *ClientConductor) sendAddPublication(kind resourceKind, channel string, streamID int32) (int64, error) {
	if err := cc.ensureOpen(); err != nil {
		return 0, err
	}
	var corrID int64
	var err error
	if kind == kindPendingExclusivePublication {
		corrID, err = cc.driverProxy.AddExclusivePublication(channel, streamID)
	} else {
		corrID, err = cc.driverProxy.AddPublication(channel, streamID)
	}
	if err != nil {
		return 0, err
	}
	cc.resources[corrID] = &conductorResource{kind: kind, channel: channel, streamID: streamID,
		timeOfRegistrationNs: cc.nanoClock()}
	return corrID, nil
}

// GetPublication polls for a publication added with AsyncAddPublication. It returns (nil, nil) while the command is
// in flight, the error if the driver rejected it, and a DriverTimeoutError if no response arrived in time.
func (cc *ClientConductor) GetPublication(registrationID int64) (*Publication, error) {
	res, err := cc.getResource(registrationID, kindPendingPublication, kindPublication, kindExclusivePublication)
	if res == nil || err != nil {
		return nil, err
	}
	return res.publication, nil
}

// GetExclusivePublication polls for a publication added with AsyncAddExclusivePublication.
func (cc *ClientConductor) GetExclusivePublication(registrationID int64) (*Publication, error) {
	res, err := cc.getResource(registrationID, kindPendingExclusivePublication, kindExclusivePublication)
	if res == nil || err != nil {
		return nil, err
	}
	return res.publication, nil
}

// FindPublication is the pre-parity name of GetPublication; it accepts both concurrent and exclusive publications.
//
// Deprecated: use GetPublication or GetExclusivePublication.
func (cc *ClientConductor) FindPublication(registrationID int64) (*Publication, error) {
	return cc.GetPublication(registrationID)
}

// getResource implements polling: pendingKind is the expected in-flight kind, readyKinds the acceptable ready kinds.
func (cc *ClientConductor) getResource(registrationID int64, pendingKind resourceKind,
	readyKinds ...resourceKind) (*conductorResource, error) {
	cc.adminLock.Lock()
	defer cc.unlock()
	if err := cc.ensureOpen(); err != nil {
		return nil, err
	}
	if err, ok := cc.asyncErrors[registrationID]; ok {
		delete(cc.asyncErrors, registrationID)
		return nil, err
	}
	res := cc.resources[registrationID]
	if res == nil {
		return nil, fmt.Errorf("%w: %d", ErrUnknownRegistration, registrationID)
	}
	if res.kind.isPending() {
		if res.kind != pendingKind && !(pendingKind == kindPendingPublication && res.kind == kindPendingExclusivePublication) {
			return nil, fmt.Errorf("registration id %d is not of the requested resource type", registrationID)
		}
		if cc.nanoClock()-res.timeOfRegistrationNs > cc.driverTimeoutNs {
			delete(cc.resources, registrationID)
			delete(cc.asyncCommandIDs, registrationID)
			return nil, NewDriverTimeoutError(fmt.Sprintf(
				"no response from MediaDriver within %v (correlationId=%d)",
				time.Duration(cc.driverTimeoutNs), registrationID))
		}
		return nil, nil
	}
	for _, k := range readyKinds {
		if res.kind == k {
			return res, nil
		}
	}
	return nil, fmt.Errorf("registration id %d is not of the requested resource type", registrationID)
}

// releasePublication is called by Publication.Close.
func (cc *ClientConductor) releasePublication(regID int64) error {
	cc.adminLock.Lock()
	defer cc.unlock()
	return cc.releasePublicationLocked(regID)
}

func (cc *ClientConductor) releasePublicationLocked(regID int64) error {
	if cc.isTerminating.Get() || cc.isClosed.Get() {
		return nil
	}
	res := cc.resources[regID]
	if res == nil || (res.kind != kindPublication && res.kind != kindExclusivePublication) {
		return nil
	}
	res.publication.isClosed.Set(true)
	delete(cc.resources, regID)
	cc.releaseLogBuffers(res.logBuffers, res.publication.originalRegID, explicitCloseLingerNs)
	return cc.driverProxy.RemovePublication(regID)
}

// AsyncRemovePublication closes the publication with the given registration id without blocking.
func (cc *ClientConductor) AsyncRemovePublication(registrationID int64) error {
	cc.adminLock.Lock()
	defer cc.unlock()
	if err := cc.ensureOpen(); err != nil {
		return err
	}
	return cc.releasePublicationLocked(registrationID)
}

// ---------------------------------------------------------------------------------------------------------------
// Subscriptions

// AddSubscription adds a subscription and blocks until the driver responds.
func (cc *ClientConductor) AddSubscription(channel string, streamID int32) (*Subscription, error) {
	return cc.AddSubscriptionWithHandlers(channel, streamID, cc.onAvailableImageHandler, cc.onUnavailableImageHandler)
}

// AddSubscriptionWithHandlers adds a subscription with specific image handlers and blocks until the driver responds.
func (cc *ClientConductor) AddSubscriptionWithHandlers(channel string, streamID int32,
	onAvailableImage AvailableImageHandler, onUnavailableImage UnavailableImageHandler) (*Subscription, error) {
	cc.adminLock.Lock()
	defer cc.unlock()
	corrID, err := cc.sendAddSubscription(channel, streamID, onAvailableImage, onUnavailableImage)
	if err != nil {
		return nil, err
	}
	if err := cc.awaitResponse(corrID); err != nil {
		delete(cc.resources, corrID)
		return nil, err
	}
	res := cc.resources[corrID]
	if res == nil || res.kind != kindSubscription {
		return nil, fmt.Errorf("subscription %d not available after response", corrID)
	}
	return res.subscription, nil
}

// AsyncAddSubscription sends ADD_SUBSCRIPTION without waiting; poll GetSubscription.
func (cc *ClientConductor) AsyncAddSubscription(channel string, streamID int32) (int64, error) {
	return cc.AsyncAddSubscriptionWithHandlers(channel, streamID, cc.onAvailableImageHandler, cc.onUnavailableImageHandler)
}

// AsyncAddSubscriptionWithHandlers is AsyncAddSubscription with specific image handlers.
func (cc *ClientConductor) AsyncAddSubscriptionWithHandlers(channel string, streamID int32,
	onAvailableImage AvailableImageHandler, onUnavailableImage UnavailableImageHandler) (int64, error) {
	return cc.asyncAdd(func() (int64, error) {
		return cc.sendAddSubscription(channel, streamID, onAvailableImage, onUnavailableImage)
	})
}

func (cc *ClientConductor) sendAddSubscription(channel string, streamID int32,
	onAvailableImage AvailableImageHandler, onUnavailableImage UnavailableImageHandler) (int64, error) {
	if err := cc.ensureOpen(); err != nil {
		return 0, err
	}
	corrID, err := cc.driverProxy.AddSubscription(channel, streamID)
	if err != nil {
		return 0, err
	}
	sub := NewSubscription(cc, channel, corrID, streamID, ChannelStatusNoIdAllocated, onAvailableImage,
		onUnavailableImage)
	cc.resources[corrID] = &conductorResource{kind: kindPendingSubscription, channel: channel, streamID: streamID,
		timeOfRegistrationNs: cc.nanoClock(), subscription: sub}
	return corrID, nil
}

// GetSubscription polls for a subscription added with AsyncAddSubscription.
func (cc *ClientConductor) GetSubscription(registrationID int64) (*Subscription, error) {
	res, err := cc.getResource(registrationID, kindPendingSubscription, kindSubscription)
	if res == nil || err != nil {
		return nil, err
	}
	return res.subscription, nil
}

// FindSubscription is the pre-parity name of GetSubscription.
//
// Deprecated: use GetSubscription.
func (cc *ClientConductor) FindSubscription(registrationID int64) (*Subscription, error) {
	return cc.GetSubscription(registrationID)
}

// releaseSubscription is called by Subscription.Close after it has emptied its image list.
func (cc *ClientConductor) releaseSubscription(regID int64, images []Image) error {
	cc.adminLock.Lock()
	defer cc.unlock()
	return cc.releaseSubscriptionLocked(regID, images)
}

func (cc *ClientConductor) releaseSubscriptionLocked(regID int64, images []Image) error {
	if cc.isTerminating.Get() || cc.isClosed.Get() {
		return nil
	}
	res := cc.resources[regID]
	if res == nil || res.kind != kindSubscription {
		return nil
	}
	delete(cc.resources, regID)
	cc.closeImages(images, res.subscription.UnavailableImageHandler(), explicitCloseLingerNs)
	return cc.driverProxy.RemoveSubscription(regID)
}

// AsyncRemoveSubscription closes the subscription with the given registration id without blocking.
func (cc *ClientConductor) AsyncRemoveSubscription(registrationID int64) error {
	cc.adminLock.Lock()
	defer cc.unlock()
	if err := cc.ensureOpen(); err != nil {
		return err
	}
	res := cc.resources[registrationID]
	if res == nil || res.kind != kindSubscription {
		return nil
	}
	if !res.subscription.isClosed.CompareAndSet(false, true) {
		return nil
	}
	return cc.releaseSubscriptionLocked(registrationID, res.subscription.images.Empty())
}

// ---------------------------------------------------------------------------------------------------------------
// Destinations

// AddDestination adds a destination to a multi-destination-cast publication and waits for the driver.
func (cc *ClientConductor) AddDestination(registrationID int64, endpointChannel string) error {
	_, err := cc.AddDestinationWithID(registrationID, endpointChannel)
	return err
}

// AddDestinationWithID is AddDestination returning the destination registration id, usable with
// RemoveDestinationByID.
func (cc *ClientConductor) AddDestinationWithID(registrationID int64, endpointChannel string) (int64, error) {
	return cc.syncCommand(func() (int64, error) { return cc.driverProxy.AddDestination(registrationID, endpointChannel) })
}

// RemoveDestination removes a destination, by channel, from a multi-destination-cast publication.
func (cc *ClientConductor) RemoveDestination(registrationID int64, endpointChannel string) error {
	_, err := cc.syncCommand(func() (int64, error) {
		return cc.driverProxy.RemoveDestination(registrationID, endpointChannel)
	})
	return err
}

// RemoveDestinationByID removes a destination, by its registration id, from a multi-destination-cast publication.
func (cc *ClientConductor) RemoveDestinationByID(registrationID int64, destinationRegistrationID int64) error {
	_, err := cc.syncCommand(func() (int64, error) {
		return cc.driverProxy.RemoveDestinationByID(registrationID, destinationRegistrationID)
	})
	return err
}

// AddRcvDestination adds a destination to a multi-destination subscription and waits for the driver.
func (cc *ClientConductor) AddRcvDestination(registrationID int64, endpointChannel string) error {
	_, err := cc.syncCommand(func() (int64, error) {
		return cc.driverProxy.AddRcvDestination(registrationID, endpointChannel)
	})
	return err
}

// RemoveRcvDestination removes a destination from a multi-destination subscription and waits for the driver.
func (cc *ClientConductor) RemoveRcvDestination(registrationID int64, endpointChannel string) error {
	_, err := cc.syncCommand(func() (int64, error) {
		return cc.driverProxy.RemoveRcvDestination(registrationID, endpointChannel)
	})
	return err
}

// AsyncAddDestination sends ADD_DESTINATION without waiting. The returned correlation id is also the destination
// registration id; track completion with IsCommandActive. Failures are reported to the error handler.
func (cc *ClientConductor) AsyncAddDestination(registrationID int64, endpointChannel string) (int64, error) {
	return cc.asyncCommand(func() (int64, error) { return cc.driverProxy.AddDestination(registrationID, endpointChannel) })
}

// AsyncRemoveDestination sends REMOVE_DESTINATION without waiting.
func (cc *ClientConductor) AsyncRemoveDestination(registrationID int64, endpointChannel string) (int64, error) {
	return cc.asyncCommand(func() (int64, error) {
		return cc.driverProxy.RemoveDestination(registrationID, endpointChannel)
	})
}

// AsyncRemoveDestinationByID sends REMOVE_DESTINATION_BY_ID without waiting.
func (cc *ClientConductor) AsyncRemoveDestinationByID(registrationID int64, destinationRegistrationID int64) (int64, error) {
	return cc.asyncCommand(func() (int64, error) {
		return cc.driverProxy.RemoveDestinationByID(registrationID, destinationRegistrationID)
	})
}

// AsyncAddRcvDestination sends ADD_RCV_DESTINATION without waiting.
func (cc *ClientConductor) AsyncAddRcvDestination(registrationID int64, endpointChannel string) (int64, error) {
	return cc.asyncCommand(func() (int64, error) {
		return cc.driverProxy.AddRcvDestination(registrationID, endpointChannel)
	})
}

// AsyncRemoveRcvDestination sends REMOVE_RCV_DESTINATION without waiting.
func (cc *ClientConductor) AsyncRemoveRcvDestination(registrationID int64, endpointChannel string) (int64, error) {
	return cc.asyncCommand(func() (int64, error) {
		return cc.driverProxy.RemoveRcvDestination(registrationID, endpointChannel)
	})
}

func (cc *ClientConductor) syncCommand(send func() (int64, error)) (int64, error) {
	cc.adminLock.Lock()
	defer cc.unlock()
	if err := cc.ensureOpen(); err != nil {
		return 0, err
	}
	corrID, err := send()
	if err != nil {
		return 0, err
	}
	return corrID, cc.awaitResponse(corrID)
}

func (cc *ClientConductor) asyncCommand(send func() (int64, error)) (int64, error) {
	return cc.asyncAdd(func() (int64, error) {
		if err := cc.ensureOpen(); err != nil {
			return 0, err
		}
		return send()
	})
}

// IsCommandActive reports whether an async command with the given correlation id is still awaiting a response.
func (cc *ClientConductor) IsCommandActive(correlationID int64) bool {
	cc.adminLock.Lock()
	defer cc.unlock()
	_, ok := cc.asyncCommandIDs[correlationID]
	return ok
}

// HasActiveCommands reports whether any async commands are awaiting a response.
func (cc *ClientConductor) HasActiveCommands() bool {
	cc.adminLock.Lock()
	defer cc.unlock()
	return len(cc.asyncCommandIDs) > 0
}

// ---------------------------------------------------------------------------------------------------------------
// Counters

// AddCounter sends ADD_COUNTER without waiting and returns the registration id; poll GetCounter.
func (cc *ClientConductor) AddCounter(typeId int32, keyBuffer *atomic.Buffer, keyOffset int32, keyLength int32,
	labelBuffer *atomic.Buffer, labelOffset int32, labelLength int32) (int64, error) {
	return cc.asyncAdd(func() (int64, error) {
		return cc.sendAddCounter(kindPendingCounter, func() (int64, error) {
			return cc.driverProxy.AddCounter(typeId, keyBuffer, keyOffset, keyLength, labelBuffer, labelOffset, labelLength)
		})
	})
}

// AddCounterByLabel sends ADD_COUNTER with an empty key without waiting; poll GetCounter.
func (cc *ClientConductor) AddCounterByLabel(typeId int32, label string) (int64, error) {
	return cc.asyncAdd(func() (int64, error) {
		return cc.sendAddCounter(kindPendingCounter, func() (int64, error) {
			return cc.driverProxy.AddCounterByLabel(typeId, label)
		})
	})
}

// AddStaticCounter sends ADD_STATIC_COUNTER without waiting and returns the correlation id; poll GetCounter. Static
// counters are not owned by this client and are never removed by it.
func (cc *ClientConductor) AddStaticCounter(typeId int32, keyBuffer *atomic.Buffer, keyOffset int32, keyLength int32,
	labelBuffer *atomic.Buffer, labelOffset int32, labelLength int32, registrationId int64) (int64, error) {
	return cc.asyncAdd(func() (int64, error) {
		return cc.sendAddCounter(kindPendingStaticCounter, func() (int64, error) {
			return cc.driverProxy.AddStaticCounter(typeId, keyBuffer, keyOffset, keyLength, labelBuffer, labelOffset,
				labelLength, registrationId)
		})
	})
}

// AddStaticCounterByLabel is AddStaticCounter with an empty key.
func (cc *ClientConductor) AddStaticCounterByLabel(typeId int32, label string, registrationId int64) (int64, error) {
	return cc.asyncAdd(func() (int64, error) {
		return cc.sendAddCounter(kindPendingStaticCounter, func() (int64, error) {
			return cc.driverProxy.AddStaticCounterByLabel(typeId, label, registrationId)
		})
	})
}

func (cc *ClientConductor) sendAddCounter(kind resourceKind, send func() (int64, error)) (int64, error) {
	if err := cc.ensureOpen(); err != nil {
		return 0, err
	}
	corrID, err := send()
	if err != nil {
		return 0, err
	}
	cc.resources[corrID] = &conductorResource{kind: kind, timeOfRegistrationNs: cc.nanoClock()}
	return corrID, nil
}

// GetCounter polls for a counter added with AddCounter*/AddStaticCounter*: (nil, nil) while the driver has not
// responded.
func (cc *ClientConductor) GetCounter(correlationID int64) (*Counter, error) {
	cc.adminLock.Lock()
	res := cc.resources[correlationID]
	static := res != nil && (res.kind == kindPendingStaticCounter || res.kind == kindStaticCounter)
	cc.adminLock.Unlock()
	kinds := []resourceKind{kindPendingCounter, kindCounter}
	if static {
		kinds = []resourceKind{kindPendingStaticCounter, kindStaticCounter}
	}
	res, err := cc.getResource(correlationID, kinds[0], kinds[1])
	if res == nil || err != nil {
		return nil, err
	}
	return res.counter, nil
}

// releaseStaticCounter is called by Counter.Close for a static counter: the local reference is dropped, the driver
// counter is left in place.
func (cc *ClientConductor) releaseStaticCounter(correlationID int64) {
	cc.adminLock.Lock()
	defer cc.unlock()
	if res := cc.resources[correlationID]; res != nil && res.kind == kindStaticCounter {
		delete(cc.resources, correlationID)
	}
}

// FindCounter is the pre-parity name of GetCounter.
//
// Deprecated: use GetCounter.
func (cc *ClientConductor) FindCounter(registrationID int64) (*Counter, error) {
	return cc.GetCounter(registrationID)
}

// releaseCounter is called by Counter.Close. Static counters are not removed from the driver.
func (cc *ClientConductor) releaseCounter(counter Counter) error {
	cc.adminLock.Lock()
	defer cc.unlock()
	return cc.releaseCounterLocked(counter.RegistrationId(), counter.Id())
}

func (cc *ClientConductor) releaseCounterLocked(registrationID int64, counterID int32) error {
	if cc.isTerminating.Get() || cc.isClosed.Get() {
		return nil
	}
	res := cc.resources[registrationID]
	if res == nil || res.kind != kindCounter || res.counter.Id() != counterID {
		return nil
	}
	res.counter.isClosed.Set(true)
	delete(cc.resources, registrationID)
	corrID, err := cc.driverProxy.RemoveCounter(registrationID)
	if err != nil {
		return err
	}
	cc.asyncCommandIDs[corrID] = cc.nanoClock()
	return nil
}

// AsyncRemoveCounter closes the client-owned counter with the given registration id without blocking.
func (cc *ClientConductor) AsyncRemoveCounter(registrationID int64) error {
	cc.adminLock.Lock()
	defer cc.unlock()
	if err := cc.ensureOpen(); err != nil {
		return err
	}
	res := cc.resources[registrationID]
	if res == nil || res.kind != kindCounter {
		return nil
	}
	return cc.releaseCounterLocked(registrationID, res.counter.Id())
}

// NextSessionID asks the driver for the next available session id for a stream (driver >= 1.47).
func (cc *ClientConductor) NextSessionID(streamID int32) (int32, error) {
	cc.adminLock.Lock()
	defer cc.unlock()
	if err := cc.ensureOpen(); err != nil {
		return 0, err
	}
	corrID, err := cc.driverProxy.GetNextAvailableSessionID(streamID)
	if err != nil {
		return 0, err
	}
	if err := cc.awaitResponse(corrID); err != nil {
		return 0, err
	}
	return cc.nextSessionIDResult, nil
}

// ---------------------------------------------------------------------------------------------------------------
// Handlers

func (cc *ClientConductor) AddAvailableCounterHandler(handler AvailableCounterHandler) int64 {
	cc.adminLock.Lock()
	defer cc.unlock()
	id := cc.driverProxy.NextCorrelationID()
	cc.availableCounterHandlers = append(cc.availableCounterHandlers, NewIdAndAvailablePair(id, handler))
	return id
}

func (cc *ClientConductor) RemoveAvailableCounterHandlerById(registrationId int64) bool {
	cc.adminLock.Lock()
	defer cc.unlock()
	for i, pair := range cc.availableCounterHandlers {
		if pair.registrationId == registrationId {
			cc.availableCounterHandlers = append(cc.availableCounterHandlers[:i:i], cc.availableCounterHandlers[i+1:]...)
			return true
		}
	}
	return false
}

func (cc *ClientConductor) RemoveAvailableCounterHandler(handler AvailableCounterHandler) bool {
	cc.adminLock.Lock()
	defer cc.unlock()
	for i, pair := range cc.availableCounterHandlers {
		if pair.handler == handler {
			cc.availableCounterHandlers = append(cc.availableCounterHandlers[:i:i], cc.availableCounterHandlers[i+1:]...)
			return true
		}
	}
	return false
}

func (cc *ClientConductor) AddUnavailableCounterHandler(handler UnavailableCounterHandler) int64 {
	cc.adminLock.Lock()
	defer cc.unlock()
	id := cc.driverProxy.NextCorrelationID()
	cc.unavailableCounterHandlers = append(cc.unavailableCounterHandlers, NewIdAndUnavailablePair(id, handler))
	return id
}

func (cc *ClientConductor) RemoveUnavailableCounterHandlerById(registrationId int64) bool {
	cc.adminLock.Lock()
	defer cc.unlock()
	for i, pair := range cc.unavailableCounterHandlers {
		if pair.registrationId == registrationId {
			cc.unavailableCounterHandlers = append(cc.unavailableCounterHandlers[:i:i], cc.unavailableCounterHandlers[i+1:]...)
			return true
		}
	}
	return false
}

func (cc *ClientConductor) RemoveUnavailableCounterHandler(handler UnavailableCounterHandler) bool {
	cc.adminLock.Lock()
	defer cc.unlock()
	for i, pair := range cc.unavailableCounterHandlers {
		if pair.handler == handler {
			cc.unavailableCounterHandlers = append(cc.unavailableCounterHandlers[:i:i], cc.unavailableCounterHandlers[i+1:]...)
			return true
		}
	}
	return false
}

// AddCloseHandler registers a handler called once when the client closes; returns an id for RemoveCloseHandler.
func (cc *ClientConductor) AddCloseHandler(handler func()) int64 {
	cc.adminLock.Lock()
	defer cc.unlock()
	id := cc.driverProxy.NextCorrelationID()
	cc.closeHandlers = append(cc.closeHandlers, idAndCloseHandler{id, handler})
	return id
}

// RemoveCloseHandler removes a close handler by the id returned from AddCloseHandler.
func (cc *ClientConductor) RemoveCloseHandler(id int64) bool {
	cc.adminLock.Lock()
	defer cc.unlock()
	for i, h := range cc.closeHandlers {
		if h.id == id {
			cc.closeHandlers = append(cc.closeHandlers[:i:i], cc.closeHandlers[i+1:]...)
			return true
		}
	}
	return false
}

func (cc *ClientConductor) notifyAvailableCounter(registrationID int64, counterID int32) {
	for _, pair := range cc.availableCounterHandlers {
		h := pair.handler
		cc.queueCallback(func() { h.Handle(cc.counterReader, registrationID, counterID) })
	}
}

func (cc *ClientConductor) notifyUnavailableCounter(registrationID int64, counterID int32) {
	for _, pair := range cc.unavailableCounterHandlers {
		h := pair.handler
		cc.queueCallback(func() { h.Handle(cc.counterReader, registrationID, counterID) })
	}
}

// ---------------------------------------------------------------------------------------------------------------
// Driver events (driver.Listener / driver.ExtendedListener). Invoked by the listener adapter with adminLock held.

func (cc *ClientConductor) completeCommand(correlationID int64) {
	delete(cc.asyncCommandIDs, correlationID)
	if correlationID == cc.activeCorrelationID {
		cc.receivedCorrelationID = correlationID
	}
}

func (cc *ClientConductor) failCommand(correlationID int64, err error) bool {
	res := cc.resources[correlationID]
	wasPending := res != nil && res.kind.isPending()
	if wasPending {
		delete(cc.resources, correlationID)
	}
	if correlationID == cc.activeCorrelationID {
		cc.receivedCorrelationID = correlationID
		cc.driverErr = err
		delete(cc.asyncCommandIDs, correlationID)
		return true
	}
	if _, ok := cc.asyncCommandIDs[correlationID]; ok {
		delete(cc.asyncCommandIDs, correlationID)
		if wasPending {
			cc.asyncErrors[correlationID] = err
		} else {
			cc.queueError(err)
		}
		return true
	}
	return false
}

func (cc *ClientConductor) OnNewPublication(streamID int32, sessionID int32, posLimitCounterID int32,
	channelStatusIndicatorID int32, logFileName string, corrID int64, regID int64) {
	cc.onNewPublication(kindPendingPublication, kindPublication, streamID, sessionID, posLimitCounterID,
		channelStatusIndicatorID, logFileName, corrID, regID)
}

func (cc *ClientConductor) OnNewExclusivePublication(streamID int32, sessionID int32, posLimitCounterID int32,
	channelStatusIndicatorID int32, logFileName string, corrID int64, regID int64) {
	cc.onNewPublication(kindPendingExclusivePublication, kindExclusivePublication, streamID, sessionID,
		posLimitCounterID, channelStatusIndicatorID, logFileName, corrID, regID)
}

func (cc *ClientConductor) onNewPublication(pendingKind, readyKind resourceKind, streamID int32, sessionID int32,
	posLimitCounterID int32, channelStatusIndicatorID int32, logFileName string, corrID int64, regID int64) {
	logger.Debugf("OnNewPublication: streamId=%d, sessionId=%d, posLimitCounterID=%d, channelStatusIndicatorID=%d, "+
		"logFileName=%s, correlationID=%d, regID=%d",
		streamID, sessionID, posLimitCounterID, channelStatusIndicatorID, logFileName, corrID, regID)

	res := cc.resources[corrID]
	if res == nil || res.kind != pendingKind {
		return
	}
	lb, err := cc.mapLogBuffers(regID, logFileName)
	if err != nil {
		cc.failCommand(corrID, err)
		return
	}
	pub := NewPublication(lb)
	pub.conductor = cc
	pub.channel = res.channel
	pub.regID = corrID
	pub.originalRegID = regID
	pub.streamID = streamID
	pub.sessionID = sessionID
	pub.pubLimit = NewPosition(cc.counterValuesBuffer, posLimitCounterID)
	pub.channelStatusIndicatorID = channelStatusIndicatorID

	res.kind = readyKind
	res.publication = pub
	res.logBuffers = lb
	cc.completeCommand(corrID)

	if h := cc.onNewPublicationHandler; h != nil {
		channel := res.channel
		cc.queueCallback(func() { h(channel, streamID, sessionID, corrID) })
	}
}

func (cc *ClientConductor) OnSubscriptionReady(corrID int64, channelStatusIndicatorID int32) {
	logger.Debugf("OnSubscriptionReady: correlationID=%d, channelStatusIndicatorID=%d", corrID, channelStatusIndicatorID)
	res := cc.resources[corrID]
	if res == nil || res.kind != kindPendingSubscription {
		return
	}
	res.subscription.channelStatusID = channelStatusIndicatorID
	res.kind = kindSubscription
	cc.completeCommand(corrID)
	if h := cc.onNewSubscriptionHandler; h != nil {
		channel, streamID := res.channel, res.streamID
		cc.queueCallback(func() { h(channel, streamID, corrID) })
	}
}

func (cc *ClientConductor) OnOperationSuccess(corrID int64) {
	logger.Debugf("OnOperationSuccess: correlationId=%d", corrID)
	cc.completeCommand(corrID)
}

func (cc *ClientConductor) OnErrorResponse(corrID int64, errorCode int32, errorMessage string) {
	logger.Debugf("OnErrorResponse: correlationID=%d, errorCode=%d, errorMessage=%s", corrID, errorCode, errorMessage)
	cc.failCommand(corrID, NewRegistrationError(corrID, errorCode, errorMessage))
}

func (cc *ClientConductor) OnChannelEndpointError(corrID int64, errorMessage string) {
	logger.Debugf("OnChannelEndpointError: correlationID=%d, errorMessage=%s", corrID, errorMessage)

	statusIndicatorID := int32(corrID)
	for _, res := range cc.resources {
		match := false
		switch res.kind {
		case kindPublication, kindExclusivePublication:
			match = res.publication.ChannelStatusID() == statusIndicatorID
		case kindSubscription:
			match = res.subscription.ChannelStatusId() == statusIndicatorID
		}
		if match {
			cc.queueError(NewChannelEndpointError(statusIndicatorID, errorMessage))
		}
	}
	cc.failCommand(corrID, NewRegistrationError(corrID, 4, errorMessage))
}

func (cc *ClientConductor) OnAvailableCounter(corrID int64, counterID int32) {
	logger.Debugf("OnAvailableCounter: registrationId=%d, counterId=%d", corrID, counterID)
	if res := cc.resources[corrID]; res != nil && res.kind == kindPendingCounter {
		counter, err := NewCounter(corrID, cc, counterID)
		if err != nil {
			cc.failCommand(corrID, err)
		} else {
			res.kind = kindCounter
			res.counter = counter
			cc.completeCommand(corrID)
		}
	}
	cc.notifyAvailableCounter(corrID, counterID)
}

func (cc *ClientConductor) OnUnavailableCounter(registrationID int64, counterID int32) {
	logger.Debugf("OnUnavailableCounter: registrationId=%d, counterId=%d", registrationID, counterID)
	if res := cc.resources[registrationID]; res != nil && res.kind == kindCounter && res.counter.Id() == counterID {
		res.counter.isClosed.Set(true)
		delete(cc.resources, registrationID)
	}
	cc.notifyUnavailableCounter(registrationID, counterID)
}

func (cc *ClientConductor) OnStaticCounter(corrID int64, counterID int32) {
	logger.Debugf("OnStaticCounter: correlationId=%d, counterId=%d", corrID, counterID)
	res := cc.resources[corrID]
	if res == nil || res.kind != kindPendingStaticCounter {
		return
	}
	counter, err := newCounter(corrID, cc.counterReader.GetCounterRegistrationId(counterID), false, cc, counterID)
	if err != nil {
		cc.failCommand(corrID, err)
		return
	}
	res.kind = kindStaticCounter
	res.counter = counter
	cc.completeCommand(corrID)
}

func (cc *ClientConductor) OnNextAvailableSessionID(corrID int64, sessionID int32) {
	if corrID == cc.activeCorrelationID {
		cc.nextSessionIDResult = sessionID
		cc.completeCommand(corrID)
	}
}

// OnPublicationError dispatches ON_PUBLICATION_ERROR to the publication error frame handler when the frame is for a
// publication owned by this client.
func (cc *ClientConductor) OnPublicationError(buffer *atomic.Buffer, offset int32, length int32) {
	h := cc.publicationErrorFrameHandler
	if h == nil {
		return
	}
	var frame PublicationErrorFrame
	if err := decodePublicationErrorFrame(buffer, offset, length, &frame); err != nil {
		cc.queueError(err)
		return
	}
	logger.Debugf("OnPublicationError: %v", &frame)
	for _, res := range cc.resources {
		if (res.kind == kindPublication || res.kind == kindExclusivePublication) &&
			res.publication.originalRegID == frame.RegistrationID {
			cc.queueCallback(func() { h(&frame) })
			return
		}
	}
}

func (cc *ClientConductor) OnClientTimeout(clientID int64) {
	logger.Debugf("OnClientTimeout: clientID=%d", clientID)
	if clientID == cc.driverProxy.ClientID() && !cc.isClosed.Get() && !cc.isTerminating.Get() {
		cc.queueError(cc.terminate(NewClientTimeoutError(fmt.Sprintf("client timeout from driver: clientId=%d",
			clientID))))
	}
}

// DefaultImageFactory maps the image log buffer and creates the image. It panics if the log cannot be mapped.
func DefaultImageFactory(sessionID int32, corrID int64, logFilename string, subRegId int64, sourceIdentity string,
	counterValuesBuffer *atomic.Buffer, subscriberPositionID int32) Image {
	image := NewImage(sessionID, corrID, logbuffer.Wrap(logFilename))
	image.subscriptionRegistrationID = subRegId
	image.sourceIdentity = sourceIdentity
	image.subscriberPosition = NewPosition(counterValuesBuffer, subscriberPositionID)
	return image
}

func (cc *ClientConductor) newImage(sessionID int32, corrID int64, logFilename string, subRegID int64,
	sourceIdentity string, subscriberPositionID int32) (image Image, err error) {
	defer func() {
		if r := recover(); r != nil {
			image, err = nil, fmt.Errorf("failed to create image %s: %v", logFilename, r)
		}
	}()
	return cc.imageFactory(sessionID, corrID, logFilename, subRegID, sourceIdentity, cc.counterValuesBuffer,
		subscriberPositionID), nil
}

func (cc *ClientConductor) OnAvailableImage(streamID int32, sessionID int32, logFilename string, sourceIdentity string,
	subscriberPositionID int32, subsRegID int64, corrID int64) {
	logger.Debugf("OnAvailableImage: streamId=%d, sessionId=%d, logFilename=%s, sourceIdentity=%s, subsRegID=%d, "+
		"corrID=%d", streamID, sessionID, logFilename, sourceIdentity, subsRegID, corrID)

	res := cc.resources[subsRegID]
	if res == nil || res.kind != kindSubscription {
		return
	}
	sub := res.subscription
	for _, img := range sub.images.Get() {
		if img.CorrelationID() == corrID {
			return
		}
	}
	image, err := cc.newImage(sessionID, corrID, logFilename, subsRegID, sourceIdentity, subscriberPositionID)
	if err != nil {
		cc.queueError(err)
		return
	}
	sub.addImage(image)
	if h := sub.AvailableImageHandler(); h != nil {
		cc.queueCallback(func() { cc.callSubscriberHandler(h, image) })
	}
}

func (cc *ClientConductor) OnUnavailableImage(corrID int64, subscriptionRegistrationID int64) {
	logger.Debugf("OnUnavailableImage: corrID=%d subscriptionRegistrationID=%d", corrID, subscriptionRegistrationID)
	res := cc.resources[subscriptionRegistrationID]
	if res == nil || res.kind != kindSubscription {
		return
	}
	image := res.subscription.removeImage(corrID)
	if image == nil {
		return
	}
	cc.closeImages([]Image{image}, res.subscription.UnavailableImageHandler(), nullValue)
}

// CounterReader returns the counters reader for the driver's CnC counters.
func (cc *ClientConductor) CounterReader() *ctr.Reader {
	return cc.counterReader
}
