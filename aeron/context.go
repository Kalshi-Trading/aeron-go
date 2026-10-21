/*
Copyright 2016 Stanislav Liberman
Copyright 2022 Steven Stern

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
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	syncatomic "sync/atomic"
	"time"

	"github.com/lirm/aeron-go/aeron/errorlog"
	"github.com/lirm/aeron-go/aeron/idlestrategy"
)

// AgentInvoker is a unit of work that can be invoked on the caller's goroutine (org.agrona.concurrent.AgentInvoker).
// Invoke returns the amount of work done.
type AgentInvoker interface {
	Invoke() int
}

// CloseHandler is called when the Aeron client is closed, either explicitly or due to a driver timeout.
type CloseHandler func()

// Context holds the configuration of an Aeron client, a port of io.aeron.Aeron.Context and io.aeron.CommonContext.
// See https://github.com/aeron-io/aeron/wiki/Configuration-Options#aeron-client-options.
//
// Defaults are taken from the environment in NewContext (AERON_DIR, AERON_DRIVER_TIMEOUT, AERON_CLIENT_NAME,
// AERON_CLIENT_IDLE_SLEEP_DURATION, AERON_CLIENT_RESOURCE_LINGER_DURATION, AERON_CLIENT_CLOSE_LINGER_DURATION,
// AERON_CLIENT_PRE_TOUCH_MAPPED_MEMORY and AERON_ENABLE_EXPERIMENTAL_FEATURES). Conclude validates the configuration
// and is called by Connect.
//
// A Context may be used by only one open Aeron client at a time, since Connect records connection state (ClientID,
// InterServiceTimeout, FilePageSize) in it. Unlike Java it may be reused once that client is closed.
type Context struct {
	aeronDir       string
	aeronDirectory string
	mediaDriverTo  time.Duration

	enableExperimentalFeatures bool

	clientName               string
	clientID                 int64
	useConductorAgentInvoker bool
	preTouchMappedMemory     bool
	driverAgentInvoker       AgentInvoker

	errorHandler           func(error)
	subscriberErrorHandler func(error)

	newPublicationHandler        NewPublicationHandler
	newSubscriptionHandler       NewSubscriptionHandler
	availableImageHandler        AvailableImageHandler
	unavailableImageHandler      UnavailableImageHandler
	availableCounterHandler      AvailableCounterHandler
	unavailableCounterHandler    UnavailableCounterHandler
	closeHandler                 CloseHandler
	publicationErrorFrameHandler PublicationErrorFrameHandler

	keepAliveInterval       time.Duration
	interServiceTo          time.Duration
	idleSleepDuration       time.Duration
	resourceLingerTo        time.Duration
	closeLingerDuration     time.Duration
	publicationConnectionTo time.Duration
	filePageSize            int32

	idleStrategy         idlestrategy.Idler
	awaitingIdleStrategy idlestrategy.Idler

	concluded syncatomic.Bool
	inUse     syncatomic.Bool
}

// NewContext creates and initializes new Context for Aeron
func NewContext() *Context {
	ctx := new(Context)

	ctx.aeronDir = DefaultAeronDirectoryName()
	ctx.clientName = os.Getenv(ClientNameEnv)
	ctx.clientID = NullValue
	ctx.preTouchMappedMemory = envBool(PreTouchMappedMemoryEnv, false)
	ctx.enableExperimentalFeatures = envBool(EnableExperimentalFeaturesEnv, false)

	ctx.errorHandler = func(err error) { logger.Error(err) }

	ctx.newPublicationHandler = func(string, int32, int32, int64) {}
	ctx.newSubscriptionHandler = func(string, int32, int64) {}
	ctx.availableImageHandler = func(Image) {}
	ctx.unavailableImageHandler = func(Image) {}
	ctx.publicationErrorFrameHandler = func(*PublicationErrorFrame) {}

	ctx.mediaDriverTo = envMillis(DriverTimeoutEnv, DefaultDriverTimeout)
	ctx.keepAliveInterval = DefaultKeepAliveInterval
	ctx.idleSleepDuration = envDuration(IdleSleepDurationEnv, DefaultIdleSleepDuration)
	ctx.resourceLingerTo = envDuration(ResourceLingerDurationEnv, DefaultResourceLingerDuration)
	ctx.closeLingerDuration = envDuration(CloseLingerDurationEnv, DefaultCloseLingerDuration)
	ctx.publicationConnectionTo = DefaultPublicationConnectionTimeout

	return ctx
}

// Conclude validates the configuration and fills in derived defaults (Aeron.Context.conclude). It is called by
// Connect and need not be called directly. Settings read from the CnC file (InterServiceTimeout, FilePageSize) and the
// ClientID are only available once connected.
func (ctx *Context) Conclude() error {
	if !ctx.concluded.CompareAndSwap(false, true) {
		return errors.New("context already concluded")
	}
	if err := ctx.conclude(); err != nil {
		ctx.concluded.Store(false)
		return err
	}
	return nil
}

func (ctx *Context) conclude() error {
	if ctx.aeronDir == "" {
		return errors.New("aeron directory name must not be empty")
	}
	if len(ctx.clientName) > MaxClientNameLength {
		return fmt.Errorf("clientName length must <= %d", MaxClientNameLength)
	}
	if ctx.driverAgentInvoker != nil && !ctx.useConductorAgentInvoker {
		return errors.New("must use Context.UseConductorAgentInvoker(true) when Context.DriverAgentInvoker() is set")
	}
	if ctx.idleSleepDuration < 0 || ctx.idleSleepDuration > maxIdleSleepDuration {
		return fmt.Errorf("invalid idle sleep duration: %dns", ctx.idleSleepDuration.Nanoseconds())
	}
	if ctx.mediaDriverTo <= 0 {
		return fmt.Errorf("invalid driver timeout: %v", ctx.mediaDriverTo)
	}
	if ctx.keepAliveInterval <= 0 {
		return fmt.Errorf("invalid keepalive interval: %v", ctx.keepAliveInterval)
	}
	if ctx.resourceLingerTo < 0 || ctx.closeLingerDuration < 0 {
		return errors.New("linger durations must not be negative")
	}
	ctx.aeronDirectory = canonicalPath(ctx.aeronDir)
	if ctx.idleStrategy == nil {
		ctx.idleStrategy = idlestrategy.Sleeping{SleepFor: ctx.idleSleepDuration}
	}
	if ctx.awaitingIdleStrategy == nil {
		ctx.awaitingIdleStrategy = idlestrategy.Sleeping{SleepFor: DefaultAwaitingIdleSleepDuration}
	}
	if ctx.errorHandler == nil {
		ctx.errorHandler = func(err error) { logger.Error(err) }
	}
	if ctx.subscriberErrorHandler == nil {
		ctx.subscriberErrorHandler = ctx.errorHandler
	}
	if ctx.publicationErrorFrameHandler == nil {
		ctx.publicationErrorFrameHandler = func(*PublicationErrorFrame) {}
	}
	return nil
}

// canonicalPath is the absolute path with symlinks resolved where it exists (File.getCanonicalFile).
func canonicalPath(dir string) string {
	abs, err := filepath.Abs(dir)
	if err != nil {
		return filepath.Clean(dir)
	}
	if resolved, err := filepath.EvalSymlinks(abs); err == nil {
		return resolved
	}
	return abs
}

// IsConcluded returns true once Conclude has succeeded.
func (ctx *Context) IsConcluded() bool {
	return ctx.concluded.Load()
}

// AeronDirectory returns the canonical (absolute, symlink-resolved) Aeron directory. Empty until concluded
// (CommonContext.aeronDirectory).
func (ctx *Context) AeronDirectory() string {
	return ctx.aeronDirectory
}

// EnableExperimentalFeatures sets whether experimental features are enabled (CommonContext.enableExperimentalFeatures).
func (ctx *Context) EnableExperimentalFeatures(enable bool) *Context {
	ctx.enableExperimentalFeatures = enable
	return ctx
}

// GetEnableExperimentalFeatures returns whether experimental features are enabled.
func (ctx *Context) GetEnableExperimentalFeatures() bool {
	return ctx.enableExperimentalFeatures
}

// DeleteAeronDirectory deletes the Aeron directory and its contents (CommonContext.deleteAeronDirectory).
func (ctx *Context) DeleteAeronDirectory() error {
	dir := ctx.aeronDirectory
	if dir == "" {
		dir = ctx.aeronDir
	}
	if dir == "" {
		return errors.New("aeron directory name must not be empty")
	}
	return os.RemoveAll(dir)
}

// SaveErrorLog writes the driver's distinct error log from this context's CnC file to w, returning the number of
// distinct errors observed (CommonContext.saveErrorLog). It returns 0 if no CnC file exists.
func (ctx *Context) SaveErrorLog(w io.Writer) (int, error) {
	return SaveErrorLog(ctx.aeronDir, w)
}

// SaveErrorLog writes the distinct error log from the CnC file in aeronDir to w, returning the number of distinct
// errors observed. It returns 0 if no CnC file exists.
func SaveErrorLog(aeronDir string, w io.Writer) (int, error) {
	cncFile := CncFileName(aeronDir)
	if !cncFileUsable(cncFile) {
		return 0, nil
	}
	m, err := mapCncFile(cncFile, false)
	if err != nil || m == nil {
		return 0, err
	}
	defer m.Close()
	return errorlog.PrintErrorLog(m.meta.ErrorBuf.Get(), w), nil
}

// GenerateRandomDirName returns the default Aeron directory name (ignoring AERON_DIR) with a random UUID suffix, for
// use with embedded drivers (CommonContext.generateRandomDirName).
func GenerateRandomDirName() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%s-%s-%x-%x-%x-%x-%x", defaultAeronDirBase(), userName(), b[0:4], b[4:6], b[6:8], b[8:10],
		b[10:])
}

// ErrorHandler sets the error handler callback
func (ctx *Context) ErrorHandler(handler func(error)) *Context {
	ctx.errorHandler = handler
	return ctx
}

// GetErrorHandler returns the error handler.
func (ctx *Context) GetErrorHandler() func(error) {
	return ctx.errorHandler
}

// SubscriberErrorHandler sets the handler for errors raised by subscription related callbacks (available and
// unavailable image handlers). Defaults to the ErrorHandler.
func (ctx *Context) SubscriberErrorHandler(handler func(error)) *Context {
	ctx.subscriberErrorHandler = handler
	return ctx
}

// GetSubscriberErrorHandler returns the subscriber error handler.
func (ctx *Context) GetSubscriberErrorHandler() func(error) {
	return ctx.subscriberErrorHandler
}

// AeronDir sets the root directory for media driver files
func (ctx *Context) AeronDir(dir string) *Context {
	ctx.aeronDir = dir
	return ctx
}

// AeronDirectoryName sets the root directory for media driver files. Alias of AeronDir matching the Java name.
func (ctx *Context) AeronDirectoryName(dir string) *Context {
	return ctx.AeronDir(dir)
}

// GetAeronDirectoryName returns the Aeron directory.
func (ctx *Context) GetAeronDirectoryName() string {
	return ctx.aeronDir
}

// MediaDriverTimeout sets the driver timeout. Deprecated: use DriverTimeout.
func (ctx *Context) MediaDriverTimeout(to time.Duration) *Context {
	return ctx.DriverTimeout(to)
}

// DriverTimeout sets the timeout after which the media driver is considered inactive if it has not updated its
// keepalive (CommonContext.driverTimeoutMs). It is also the timeout for awaiting the CnC file on Connect.
func (ctx *Context) DriverTimeout(to time.Duration) *Context {
	ctx.mediaDriverTo = to
	return ctx
}

// GetDriverTimeout returns the driver timeout.
func (ctx *Context) GetDriverTimeout() time.Duration {
	return ctx.mediaDriverTo
}

// KeepAliveInterval sets the interval between client keepalives / heartbeat counter updates.
func (ctx *Context) KeepAliveInterval(interval time.Duration) *Context {
	ctx.keepAliveInterval = interval
	return ctx
}

// GetKeepAliveInterval returns the keepalive interval.
func (ctx *Context) GetKeepAliveInterval() time.Duration {
	return ctx.keepAliveInterval
}

// ResourceLingerTimeout sets the duration for which released resources linger before being unmapped.
// Deprecated: use ResourceLingerDuration.
func (ctx *Context) ResourceLingerTimeout(to time.Duration) *Context {
	return ctx.ResourceLingerDuration(to)
}

// ResourceLingerDuration sets the duration for which released resources linger before being unmapped.
func (ctx *Context) ResourceLingerDuration(to time.Duration) *Context {
	ctx.resourceLingerTo = to
	return ctx
}

// GetResourceLingerDuration returns the resource linger duration.
func (ctx *Context) GetResourceLingerDuration() time.Duration {
	return ctx.resourceLingerTo
}

// CloseLingerDuration sets how long to linger on close before unmapping lingering resources, giving in-flight
// operations on other goroutines time to complete.
func (ctx *Context) CloseLingerDuration(d time.Duration) *Context {
	ctx.closeLingerDuration = d
	return ctx
}

// GetCloseLingerDuration returns the close linger duration.
func (ctx *Context) GetCloseLingerDuration() time.Duration {
	return ctx.closeLingerDuration
}

// InterServiceTimeout is ignored: as in Java the inter-service timeout is the driver's client liveness timeout read
// from the CnC file on Connect. Deprecated: configure aeron.client.liveness.timeout on the driver instead.
func (ctx *Context) InterServiceTimeout(time.Duration) *Context {
	return ctx
}

// GetInterServiceTimeout returns the inter-service (client liveness) timeout read from the CnC file. Zero until
// connected.
func (ctx *Context) GetInterServiceTimeout() time.Duration {
	return ctx.interServiceTo
}

// IdleSleepDuration sets the sleep duration of the default conductor idle strategy (0..1s). Ignored when an
// IdleStrategy is set explicitly.
func (ctx *Context) IdleSleepDuration(d time.Duration) *Context {
	ctx.idleSleepDuration = d
	return ctx
}

// GetIdleSleepDuration returns the idle sleep duration.
func (ctx *Context) GetIdleSleepDuration() time.Duration {
	return ctx.idleSleepDuration
}

// PublicationConnectionTimeout sets the Go-specific timeout used to determine Publication connectivity.
func (ctx *Context) PublicationConnectionTimeout(to time.Duration) *Context {
	ctx.publicationConnectionTo = to
	return ctx
}

// PreTouchMappedMemory sets whether log buffers are pre-touched when mapped so pages are faulted in up front.
func (ctx *Context) PreTouchMappedMemory(preTouch bool) *Context {
	ctx.preTouchMappedMemory = preTouch
	return ctx
}

// GetPreTouchMappedMemory returns whether mapped log buffers are pre-touched.
func (ctx *Context) GetPreTouchMappedMemory() bool {
	return ctx.preTouchMappedMemory
}

// UseConductorAgentInvoker sets whether the client conductor is driven by the caller via
// Aeron.ConductorAgentInvoker() instead of a dedicated goroutine.
func (ctx *Context) UseConductorAgentInvoker(use bool) *Context {
	ctx.useConductorAgentInvoker = use
	return ctx
}

// GetUseConductorAgentInvoker returns whether the conductor agent invoker is used.
func (ctx *Context) GetUseConductorAgentInvoker() bool {
	return ctx.useConductorAgentInvoker
}

// DriverAgentInvoker sets an invoker for an embedded media driver which is invoked on each conductor duty cycle.
// Requires UseConductorAgentInvoker(true).
func (ctx *Context) DriverAgentInvoker(invoker AgentInvoker) *Context {
	ctx.driverAgentInvoker = invoker
	return ctx
}

// GetDriverAgentInvoker returns the driver agent invoker, if any.
func (ctx *Context) GetDriverAgentInvoker() AgentInvoker {
	return ctx.driverAgentInvoker
}

// ClientName sets the client name (max 100 characters) which is appended to the client heartbeat counter label so
// the client can be identified in driver counters (e.g. AeronStat).
func (ctx *Context) ClientName(name string) *Context {
	ctx.clientName = name
	return ctx
}

// GetClientName returns the client name.
func (ctx *Context) GetClientName() string {
	return ctx.clientName
}

// ClientID returns the client id allocated when connecting to the driver, or NullValue before Connect.
func (ctx *Context) ClientID() int64 {
	return ctx.clientID
}

// FilePageSize returns the driver's file page size read from the CnC file. Zero until connected.
func (ctx *Context) FilePageSize() int32 {
	return ctx.filePageSize
}

// NewSubscriptionHandler sets an optional callback for new subscriptions
func (ctx *Context) NewSubscriptionHandler(handler func(string, int32, int64)) *Context {
	ctx.newSubscriptionHandler = handler
	return ctx
}

// NewPublicationHandler sets an optional callback for new publications
func (ctx *Context) NewPublicationHandler(handler func(string, int32, int32, int64)) *Context {
	ctx.newPublicationHandler = handler
	return ctx
}

// AvailableImageHandler sets an optional default callback for available image notifications
func (ctx *Context) AvailableImageHandler(handler func(Image)) *Context {
	ctx.availableImageHandler = handler
	return ctx
}

// GetAvailableImageHandler returns the default available image handler.
func (ctx *Context) GetAvailableImageHandler() AvailableImageHandler {
	return ctx.availableImageHandler
}

// UnavailableImageHandler sets an optional default callback for unavailable image notification
func (ctx *Context) UnavailableImageHandler(handler func(Image)) *Context {
	ctx.unavailableImageHandler = handler
	return ctx
}

// GetUnavailableImageHandler returns the default unavailable image handler.
func (ctx *Context) GetUnavailableImageHandler() UnavailableImageHandler {
	return ctx.unavailableImageHandler
}

// CloseHandler sets a handler called once when the client is closed, explicitly or due to a driver timeout.
func (ctx *Context) CloseHandler(handler CloseHandler) *Context {
	ctx.closeHandler = handler
	return ctx
}

// GetCloseHandler returns the close handler.
func (ctx *Context) GetCloseHandler() CloseHandler {
	return ctx.closeHandler
}

// PublicationErrorFrameHandler sets the handler called when a publication owned by this client receives an error
// frame from a receiver (e.g. a subscriber rejecting the image).
func (ctx *Context) PublicationErrorFrameHandler(handler PublicationErrorFrameHandler) *Context {
	ctx.publicationErrorFrameHandler = handler
	return ctx
}

// GetPublicationErrorFrameHandler returns the publication error frame handler.
func (ctx *Context) GetPublicationErrorFrameHandler() PublicationErrorFrameHandler {
	return ctx.publicationErrorFrameHandler
}

// CncFileName returns the name of the Counters file
func (ctx *Context) CncFileName() string {
	return CncFileName(ctx.aeronDir)
}

// IsDriverActive reports whether a driver is active in this context's Aeron directory within its driver timeout.
func (ctx *Context) IsDriverActive(logger func(string)) (bool, error) {
	return IsDriverActive(ctx.aeronDir, ctx.mediaDriverTo, logger)
}

// RequestDriverTermination requests the driver in this context's Aeron directory to terminate.
func (ctx *Context) RequestDriverTermination(token []byte) (bool, error) {
	return RequestDriverTermination(ctx.aeronDir, token)
}

// IdleStrategy provides an IdleStrategy for the thread responsible for communicating
// with the Aeron Media Driver.
func (ctx *Context) IdleStrategy(idleStrategy idlestrategy.Idler) *Context {
	ctx.idleStrategy = idleStrategy
	return ctx
}

// GetIdleStrategy returns the conductor idle strategy.
func (ctx *Context) GetIdleStrategy() idlestrategy.Idler {
	return ctx.idleStrategy
}

// AwaitingIdleStrategy sets the idle strategy used while awaiting driver responses in blocking calls.
func (ctx *Context) AwaitingIdleStrategy(idleStrategy idlestrategy.Idler) *Context {
	ctx.awaitingIdleStrategy = idleStrategy
	return ctx
}

// GetAwaitingIdleStrategy returns the awaiting idle strategy.
func (ctx *Context) GetAwaitingIdleStrategy() idlestrategy.Idler {
	return ctx.awaitingIdleStrategy
}

// AvailableCounterHandler sets up a callback for when a Counter is available.  This will be added to the list before
// additional handlers are added with Aeron.AddAvailableCounterHandler.
func (ctx *Context) AvailableCounterHandler(handler AvailableCounterHandler) *Context {
	ctx.availableCounterHandler = handler
	return ctx
}

// GetAvailableCounterHandler gets the callback handler for when a counter is available.
func (ctx *Context) GetAvailableCounterHandler() AvailableCounterHandler {
	return ctx.availableCounterHandler
}

// UnavailableCounterHandler sets up a callback for when a Counter is unavailable.  This will be added to the list first
// before additional handlers are added with Aeron.AddUnavailableCounterHandler.
func (ctx *Context) UnavailableCounterHandler(handler UnavailableCounterHandler) *Context {
	ctx.unavailableCounterHandler = handler
	return ctx
}

// GetUnavailableCounterHandler gets the callback handler for when a counter is unavailable.
func (ctx *Context) GetUnavailableCounterHandler() UnavailableCounterHandler {
	return ctx.unavailableCounterHandler
}

// String returns a description of the configuration.
func (ctx *Context) String() string {
	return fmt.Sprintf("Context{aeronDir=%s, enableExperimentalFeatures=%t, clientName=%q, clientId=%d, driverTimeout=%v, keepAliveInterval=%v, "+
		"interServiceTimeout=%v, idleSleepDuration=%v, resourceLingerDuration=%v, closeLingerDuration=%v, "+
		"publicationConnectionTimeout=%v, preTouchMappedMemory=%t, useConductorAgentInvoker=%t, filePageSize=%d}",
		ctx.aeronDir, ctx.enableExperimentalFeatures, ctx.clientName, ctx.clientID, ctx.mediaDriverTo, ctx.keepAliveInterval, ctx.interServiceTo,
		ctx.idleSleepDuration, ctx.resourceLingerTo, ctx.closeLingerDuration, ctx.publicationConnectionTo,
		ctx.preTouchMappedMemory, ctx.useConductorAgentInvoker, ctx.filePageSize)
}
