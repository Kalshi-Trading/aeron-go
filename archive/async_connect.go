// Copyright (C) 2026 Kalshi Trading.
// Copyright (C) 2014-2025 Real Logic Limited.
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

package archive

import (
	"bytes"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/lirm/aeron-go/aeron"
	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/logbuffer"
	"github.com/lirm/aeron-go/aeron/logbuffer/term"
	"github.com/lirm/aeron-go/aeron/logging"
	"github.com/lirm/aeron-go/archive/codecs"
)

// AsyncConnectState is the state of an AsyncConnect; values match Java AeronArchive.AsyncConnect.State.step.
type AsyncConnectState int

const (
	AsyncConnectAwaitSubscription AsyncConnectState = iota
	AsyncConnectAddPublication
	AsyncConnectAwaitPublicationConnected
	AsyncConnectSendConnectRequest
	AsyncConnectAwaitSubscriptionConnected
	AsyncConnectAwaitConnectResponse
	AsyncConnectSendArchiveIDRequest
	AsyncConnectAwaitArchiveIDResponse
	AsyncConnectSendChallengeResponse
	AsyncConnectAwaitChallengeResponse
	AsyncConnectDone
)

var asyncConnectStateNames = [...]string{
	"AWAIT_SUBSCRIPTION", "ADD_PUBLICATION", "AWAIT_PUBLICATION_CONNECTED", "SEND_CONNECT_REQUEST",
	"AWAIT_SUBSCRIPTION_CONNECTED", "AWAIT_CONNECT_RESPONSE", "SEND_ARCHIVE_ID_REQUEST", "AWAIT_ARCHIVE_ID_RESPONSE",
	"SEND_CHALLENGE_RESPONSE", "AWAIT_CHALLENGE_RESPONSE", "DONE",
}

func (s AsyncConnectState) String() string {
	if s >= 0 && int(s) < len(asyncConnectStateNames) {
		return asyncConnectStateNames[s]
	}
	return "UNKNOWN(" + strconv.Itoa(int(s)) + ")"
}

// Protocol versions, see Java AeronArchive.Configuration.PROTOCOL_*_VERSION.
var (
	ProtocolSemanticVersion       = codecs.SemanticVersion()
	ProtocolMajorVersion          = SemanticVersionMajor(ProtocolSemanticVersion)
	protocolVersionWithArchiveID  = SemanticVersionCompose(1, 11, 0)
	ErrConnectTimeout             = errors.New("archive connect timeout")
	ErrAsyncConnectClosed         = errors.New("archive async connect closed")
	ErrIncompatibleArchiveVersion = errors.New("incompatible archive protocol version")
)

// ClientVersion is reported to the archive in the connect request client info.
const ClientVersion = "1.53.3"

// SemanticVersionCompose packs a major.minor.patch semantic version as used by the archive protocol.
func SemanticVersionCompose(major, minor, patch int32) int32 { return major<<16 | minor<<8 | patch }

// SemanticVersionMajor extracts the major component of a semantic version.
func SemanticVersionMajor(v int32) int32 { return (v >> 16) & 0xFF }

// SemanticVersionMinor extracts the minor component of a semantic version.
func SemanticVersionMinor(v int32) int32 { return (v >> 8) & 0xFF }

// SemanticVersionPatch extracts the patch component of a semantic version.
func SemanticVersionPatch(v int32) int32 { return v & 0xFF }

// SemanticVersionString formats a semantic version as major.minor.patch.
func SemanticVersionString(v int32) string {
	return fmt.Sprintf("%d.%d.%d", SemanticVersionMajor(v), SemanticVersionMinor(v), SemanticVersionPatch(v))
}

// ConnectError is returned when the archive rejects or fails a connect, mirroring Java ArchiveException.
type ConnectError struct {
	Message       string
	ErrorCode     int32 // the archive's error code (ControlResponse.relevantId) for ERROR responses
	CorrelationID int64
	Code          codecs.ControlResponseCodeEnum
	cause         error
}

func (e *ConnectError) Error() string {
	return fmt.Sprintf("archive connect failed: %s (code=%d errorCode=%d correlationId=%d)",
		e.Message, e.Code, e.ErrorCode, e.CorrelationID)
}

func (e *ConnectError) Unwrap() error { return e.cause }

// AsyncConnect is a non-blocking connect to an archive, equivalent to Java AeronArchive.AsyncConnect.
// Call Poll() until it returns a non-nil Archive or an error. On error call Close() to release resources.
// An AsyncConnect is not safe for concurrent use.
type AsyncConnect struct {
	options  *Options
	aeron    *aeron.Aeron
	deadline time.Time
	state    AsyncConnectState
	err      error
	closed   bool

	subscriptionRegID int64
	publicationRegID  int64
	subscription      *aeron.Subscription
	publication       *aeron.ExclusivePublication

	correlationID                   int64
	controlSessionID                int64
	encodedCredentialsFromChallenge []byte
	poller                          *connectResponsePoller
	marshaller                      *codecs.SbeGoMarshaller
	archive                         *Archive
	pendingArchive                  *Archive // populated once the subscription exists so image callbacks can reach it
}

// NewAsyncConnect begins an asynchronous connect, equivalent to Java AeronArchive.asyncConnect(ctx).
// options may be nil for defaults; the options are copied and concluded. aeronCtx is used to create an Aeron client
// when options.Aeron is nil (aeronCtx may be nil for the default context).
func NewAsyncConnect(options *Options, aeronCtx *aeron.Context) (*AsyncConnect, error) {
	if options == nil {
		options = DefaultOptions()
	}
	opts := options.Clone()
	if err := opts.conclude(); err != nil {
		return nil, err
	}
	setLogLevels(opts)

	ac := &AsyncConnect{
		options:           opts,
		state:             AsyncConnectAwaitSubscription,
		subscriptionRegID: aeron.NullValue,
		publicationRegID:  aeron.NullValue,
		correlationID:     aeron.NullValue,
		controlSessionID:  aeron.NullValue,
		marshaller:        codecs.NewSbeGoMarshaller(),
	}
	ac.pendingArchive = newArchive(opts, aeronCtx)

	if opts.Aeron == nil {
		if aeronCtx == nil {
			aeronCtx = aeron.NewContext()
			ac.pendingArchive.aeronContext = aeronCtx
		}
		if logging.GetLevel("archive") >= logging.DEBUG {
			aeronCtx.NewSubscriptionHandler(LoggingNewSubscriptionListener)
			aeronCtx.NewPublicationHandler(LoggingNewPublicationListener)
		}
		a, err := aeron.Connect(aeronCtx)
		if err != nil {
			return nil, err
		}
		opts.Aeron = a
		opts.OwnsAeronClient = true
	}
	ac.aeron = opts.Aeron
	ac.pendingArchive.aeron = opts.Aeron
	if err := opts.applySessionID(ac.aeron.NextSessionID); err != nil {
		if opts.OwnsAeronClient {
			err = errors.Join(err, ac.aeron.Close())
		}
		return nil, err
	}
	ac.deadline = time.Now().Add(opts.Timeout)
	return ac, nil
}

func setLogLevels(opts *Options) {
	logging.SetLevel(opts.ArchiveLoglevel, "archive")
	for _, l := range []string{"aeron", "memmap", "driver", "counters", "logbuffers", "buffer", "rb"} {
		logging.SetLevel(opts.AeronLoglevel, l)
	}
}

// Options returns the concluded options in use by this connect.
func (ac *AsyncConnect) Options() *Options { return ac.options }

// State returns the current state.
func (ac *AsyncConnect) State() AsyncConnectState { return ac.state }

// Step returns the current state as an int, equivalent to Java AsyncConnect.step().
func (ac *AsyncConnect) Step() int { return int(ac.state) }

// Poll advances the connect. It returns the connected Archive once done, (nil, nil) while in progress, or an error
// if the connect failed or timed out. Errors are sticky.
func (ac *AsyncConnect) Poll() (*Archive, error) {
	if ac.err != nil {
		return nil, ac.err
	}
	if ac.closed {
		return nil, ErrAsyncConnectClosed
	}
	if ac.state == AsyncConnectDone {
		return ac.archive, nil
	}
	if err := ac.checkDeadline(); err != nil {
		ac.err = err
		return nil, err
	}
	if ac.options.AgentInvoker != nil {
		ac.options.AgentInvoker()
	}

	var err error
	switch ac.state {
	case AsyncConnectAwaitSubscription:
		err = ac.awaitSubscription()
	case AsyncConnectAddPublication:
		err = ac.addPublication()
	case AsyncConnectAwaitPublicationConnected:
		if ac.publication.IsConnected() {
			ac.correlationID = nextCorrelationID()
			ac.state = AsyncConnectSendConnectRequest
		}
	case AsyncConnectSendConnectRequest:
		err = ac.sendConnectRequest()
	case AsyncConnectAwaitSubscriptionConnected:
		if ac.subscription.IsConnected() {
			ac.state = AsyncConnectAwaitConnectResponse
		}
	case AsyncConnectSendArchiveIDRequest:
		err = ac.sendArchiveIDRequest()
	case AsyncConnectSendChallengeResponse:
		err = ac.trySend(AsyncConnectAwaitChallengeResponse, func() ([]byte, error) {
			return codecs.ChallengeResponsePacket(ac.marshaller, ac.options.RangeChecking, ac.controlSessionID,
				ac.correlationID, ac.encodedCredentialsFromChallenge)
		})
	case AsyncConnectAwaitConnectResponse, AsyncConnectAwaitArchiveIDResponse, AsyncConnectAwaitChallengeResponse:
		err = ac.pollForResponse()
	}

	if err != nil {
		ac.err = err
		return nil, err
	}
	if ac.state == AsyncConnectDone {
		return ac.archive, nil
	}
	return nil, nil
}

func (ac *AsyncConnect) checkDeadline() error {
	if time.Now().After(ac.deadline) {
		pub, sub := ac.options.RequestChannel, ac.options.ResponseChannel
		if ac.publication != nil {
			pub = ac.publication.Channel()
		}
		if ac.subscription != nil {
			sub = ac.subscription.Channel()
		}
		return fmt.Errorf("%w: step=%s publication=%s subscription=%s", ErrConnectTimeout, ac.state, pub, sub)
	}
	return nil
}

func (ac *AsyncConnect) awaitSubscription() error {
	if ac.subscriptionRegID == aeron.NullValue {
		pending := ac.pendingArchive
		onAvailable := func(image aeron.Image) {
			if l := pending.Listeners.AvailableImageListener; l != nil {
				l(image)
			}
		}
		onUnavailable := func(image aeron.Image) {
			pending.setState(ArchiveStateDisconnected)
			if l := pending.Listeners.UnavailableImageListener; l != nil {
				l(image)
			}
		}
		id, err := ac.aeron.AsyncAddSubscriptionWithHandlers(
			ac.options.ResponseChannel, ac.options.ResponseStream, onAvailable, onUnavailable)
		if err != nil {
			return err
		}
		ac.subscriptionRegID = id
	}
	sub, err := ac.aeron.GetSubscription(ac.subscriptionRegID)
	if err != nil {
		ac.subscriptionRegID = aeron.NullValue
		return ignoreTemporarilyUnavailable(err)
	}
	if sub == nil {
		return nil
	}
	ac.subscription = sub
	ac.subscriptionRegID = aeron.NullValue
	ac.poller = newConnectResponsePoller(sub)

	response, err := aeron.ParseChannelUri(ac.options.ResponseChannel)
	if err != nil {
		return err
	}
	if isControlModeResponse(response) {
		request, err := aeron.ParseChannelUri(ac.options.RequestChannel)
		if err != nil {
			return err
		}
		request.Set(responseCorrelationIDParamName, strconv.FormatInt(sub.RegistrationID(), 10))
		ac.options.RequestChannel = request.String()
	}
	ac.state = AsyncConnectAddPublication
	return nil
}

func (ac *AsyncConnect) addPublication() error {
	if ac.publicationRegID == aeron.NullValue {
		id, err := ac.aeron.AsyncAddExclusivePublication(ac.options.RequestChannel, ac.options.RequestStream)
		if err != nil {
			return err
		}
		ac.publicationRegID = id
	}
	pub, err := ac.aeron.GetExclusivePublication(ac.publicationRegID)
	if err != nil {
		ac.publicationRegID = aeron.NullValue
		return ignoreTemporarilyUnavailable(err)
	}
	if pub == nil {
		return nil
	}
	ac.publication = pub
	ac.publicationRegID = aeron.NullValue
	ac.state = AsyncConnectAwaitPublicationConnected
	return nil
}

// ignoreTemporarilyUnavailable lets the add be retried on the next poll when the driver rejects it with
// RESOURCE_TEMPORARILY_UNAVAILABLE, as Java AsyncConnect does.
func ignoreTemporarilyUnavailable(err error) error {
	var regErr *aeron.RegistrationError
	if errors.As(err, &regErr) && regErr.Code == aeron.ErrorCodeResourceTemporarilyUnavailable {
		return nil
	}
	return err
}

func (ac *AsyncConnect) clientInfo() string {
	return fmt.Sprintf("name=%s version=%s commit=aeron-go", ac.options.ClientName, ClientVersion)
}

func (ac *AsyncConnect) sendConnectRequest() error {
	responseChannel := ac.subscription.TryResolveChannelEndpointPort()
	if responseChannel == "" {
		return nil
	}
	return ac.trySend(AsyncConnectAwaitSubscriptionConnected, func() ([]byte, error) {
		return codecs.AuthConnectRequestPacket(ac.marshaller, ac.options.RangeChecking, ac.correlationID,
			ac.options.ResponseStream, responseChannel, ac.options.CredentialsSupplier.EncodedCredentials(),
			ac.clientInfo())
	})
}

// trySend makes a single offer attempt (Java try* proxy methods) and transitions to next on success.
func (ac *AsyncConnect) trySend(next AsyncConnectState, encode func() ([]byte, error)) error {
	b, err := encode()
	if err != nil {
		return err
	}
	if ac.publication.Offer(atomic.MakeBuffer(b, len(b)), 0, int32(len(b)), nil) > 0 {
		ac.state = next
	}
	return nil
}

func (ac *AsyncConnect) sendArchiveIDRequest() error {
	b, err := codecs.ArchiveIdRequestPacket(ac.marshaller, ac.options.RangeChecking, ac.controlSessionID, ac.correlationID)
	if err != nil {
		return err
	}
	sent, err := ac.offer(b)
	if sent {
		ac.state = AsyncConnectAwaitArchiveIDResponse
	}
	return err
}

// offer is Java ArchiveProxy.offer: up to MessageRetryAttempts attempts, failing fast when the publication is
// closed, not connected or at its max position.
func (ac *AsyncConnect) offer(b []byte) (bool, error) {
	buf := atomic.MakeBuffer(b, len(b))
	if r, ok := ac.options.IdleStrategy.(interface{ Reset() }); ok {
		r.Reset()
	}
	for attempts := ac.options.MessageRetryAttempts; ; {
		switch position := ac.publication.Offer(buf, 0, int32(len(b)), nil); {
		case position > 0:
			return true, nil
		case position == aeron.PublicationClosed:
			return false, NewArchiveException("connection to the archive has been closed", ErrorCodeGeneric, aeron.NullValue)
		case position == aeron.NotConnected:
			return false, NewArchiveException("connection to the archive is no longer available", ErrorCodeGeneric, aeron.NullValue)
		case position == aeron.MaxPositionExceeded:
			return false, NewArchiveException(fmt.Sprintf("offer failed due to max position being reached: term-length=%d",
				ac.publication.TermBufferLength()), ErrorCodeGeneric, aeron.NullValue)
		}
		if attempts--; attempts <= 0 {
			return false, nil
		}
		ac.options.IdleStrategy.Idle(0)
	}
}

func (ac *AsyncConnect) closeSession() {
	if b, err := codecs.CloseSessionRequestPacket(ac.marshaller, ac.options.RangeChecking, ac.controlSessionID); err == nil {
		_, _ = ac.offer(b)
	}
}

func (ac *AsyncConnect) pollForResponse() error {
	p := ac.poller
	p.poll()
	if !p.isPollComplete || p.correlationID != ac.correlationID {
		return nil
	}
	ac.controlSessionID = p.controlSessionID
	if p.wasChallenged() {
		ac.encodedCredentialsFromChallenge = ac.options.CredentialsSupplier.OnChallenge(p.encodedChallenge)
		ac.correlationID = nextCorrelationID()
		ac.state = AsyncConnectSendChallengeResponse
		return nil
	}
	if p.code != codecs.ControlResponseCode.OK {
		ac.closeSession()
		e := &ConnectError{CorrelationID: ac.correlationID, Code: p.code, Message: p.errorMessage}
		if p.code == codecs.ControlResponseCode.ERROR {
			e.ErrorCode = int32(p.relevantID)
			e.cause = newArchiveResponseException(p.errorMessage, p.relevantID, ac.correlationID)
		} else {
			e.Message = fmt.Sprintf("unexpected response: code=%d %s", p.code, p.errorMessage)
		}
		return e
	}
	if ac.state == AsyncConnectAwaitArchiveIDResponse {
		return ac.transitionToDone(p.relevantID)
	}
	if SemanticVersionMajor(p.version) != ProtocolMajorVersion {
		ac.closeSession()
		return &ConnectError{
			CorrelationID: ac.correlationID,
			Code:          p.code,
			Message: fmt.Sprintf("archive protocol version %s incompatible with client %s",
				SemanticVersionString(p.version), SemanticVersionString(ProtocolSemanticVersion)),
			cause: ErrIncompatibleArchiveVersion,
		}
	}
	if p.version < protocolVersionWithArchiveID {
		return ac.transitionToDone(aeron.NullValue)
	}
	ac.correlationID = nextCorrelationID()
	ac.state = AsyncConnectSendArchiveIDRequest
	return nil
}

func (ac *AsyncConnect) transitionToDone(archiveID int64) error {
	b, err := codecs.KeepAliveRequestPacket(ac.marshaller, ac.options.RangeChecking, ac.controlSessionID, aeron.NullValue)
	if err != nil {
		return err
	}
	if sent, err := ac.offer(b); !sent {
		ac.closeSession()
		return &ConnectError{Message: "failed to send keep alive after archive connect", CorrelationID: ac.correlationID,
			cause: err}
	}
	a := ac.pendingArchive
	a.SessionID = ac.controlSessionID
	a.archiveID = archiveID
	a.Proxy.Publication = ac.publication
	a.Control.Subscription = ac.subscription
	a.setState(ArchiveStateConnected)
	ac.archive = a
	ac.state = AsyncConnectDone
	logger.Infof("Archive connection established for sessionId:%d archiveId:%d", a.SessionID, archiveID)
	return nil
}

// Close releases resources if the connect has not completed. It is a no-op once Poll() has returned an Archive.
func (ac *AsyncConnect) Close() error {
	if ac.closed || ac.state == AsyncConnectDone {
		return nil
	}
	ac.closed = true
	if ac.options.OwnsAeronClient {
		return ac.aeron.Close()
	}
	// As Java AsyncConnect.close: close what was resolved, otherwise remove the in-flight add by registration id so
	// it is not leaked when the driver answers.
	var errs []error
	if ac.subscription != nil {
		errs = append(errs, ac.subscription.Close())
	} else if ac.subscriptionRegID != aeron.NullValue {
		errs = append(errs, ac.aeron.AsyncRemoveSubscription(ac.subscriptionRegID))
	}
	if ac.publication != nil {
		errs = append(errs, ac.publication.Close())
	} else if ac.publicationRegID != aeron.NullValue {
		errs = append(errs, ac.aeron.AsyncRemovePublication(ac.publicationRegID))
	}
	return errors.Join(errs...)
}

// Connect connects to an archive, blocking until connected or Options.Timeout elapses. It is the equivalent of Java
// AeronArchive.connect(ctx). options may be nil for defaults. aeronCtx is used to create an Aeron client when
// options.Aeron is nil.
func Connect(options *Options, aeronCtx *aeron.Context) (*Archive, error) {
	ac, err := NewAsyncConnect(options, aeronCtx)
	if err != nil {
		return nil, err
	}
	idle := ac.options.IdleStrategy
	previous := ac.State()
	for {
		a, err := ac.Poll()
		if err != nil {
			if cerr := ac.Close(); cerr != nil {
				err = errors.Join(err, cerr)
			}
			return nil, err
		}
		if a != nil {
			return a, nil
		}
		if ac.State() == previous {
			idle.Idle(0)
		} else {
			if r, ok := idle.(interface{ Reset() }); ok {
				r.Reset()
			}
			previous = ac.State()
		}
	}
}

// connectResponsePoller is the connect subset of Java ControlResponsePoller: it captures the next ControlResponse
// or Challenge on the control response subscription.
type connectResponsePoller struct {
	subscription *aeron.Subscription
	assembler    *aeron.ControlledFragmentAssembler
	marshaller   *codecs.SbeGoMarshaller

	isPollComplete   bool
	controlSessionID int64
	correlationID    int64
	relevantID       int64
	code             codecs.ControlResponseCodeEnum
	version          int32
	errorMessage     string
	encodedChallenge []byte
}

func newConnectResponsePoller(sub *aeron.Subscription) *connectResponsePoller {
	p := &connectResponsePoller{subscription: sub, marshaller: codecs.NewSbeGoMarshaller()}
	p.assembler = aeron.NewControlledFragmentAssembler(p.onFragment, aeron.DefaultFragmentAssemblyBufferLength)
	return p
}

func (p *connectResponsePoller) reset() {
	p.isPollComplete = false
	p.controlSessionID = aeron.NullValue
	p.correlationID = aeron.NullValue
	p.relevantID = aeron.NullValue
	p.code = codecs.ControlResponseCode.NullValue
	p.version = 0
	p.errorMessage = ""
	p.encodedChallenge = nil
}

func (p *connectResponsePoller) wasChallenged() bool { return p.encodedChallenge != nil }

func (p *connectResponsePoller) poll() int {
	if p.isPollComplete {
		p.reset()
	}
	return p.subscription.ControlledPoll(p.assembler.OnFragment, controlFragmentLimit)
}

func (p *connectResponsePoller) onFragment(buffer *atomic.Buffer, offset int32, length int32, _ *logbuffer.Header) term.ControlledPollAction {
	if p.isPollComplete {
		return term.ControlledPollActionAbort
	}
	buf := new(bytes.Buffer)
	buffer.WriteBytes(buf, offset, length)

	var hdr codecs.SbeGoMessageHeader
	if err := hdr.Decode(p.marshaller, buf); err != nil {
		logger.Warningf("connect: failed to decode message header: %s", err)
		return term.ControlledPollActionContinue
	}
	if hdr.SchemaId != (*codecs.ControlResponse)(nil).SbeSchemaId() {
		logger.Warningf("connect: unexpected schemaId=%d", hdr.SchemaId)
		return term.ControlledPollActionContinue
	}

	switch hdr.TemplateId {
	case codecIds.controlResponse:
		var r codecs.ControlResponse
		if err := r.Decode(p.marshaller, buf, hdr.Version, hdr.BlockLength, false); err != nil {
			logger.Warningf("connect: failed to decode control response: %s", err)
			return term.ControlledPollActionContinue
		}
		p.controlSessionID = r.ControlSessionId
		p.correlationID = r.CorrelationId
		p.relevantID = r.RelevantId
		p.code = r.Code
		p.version = r.Version
		p.errorMessage = string(r.ErrorMessage)
		p.encodedChallenge = nil
		p.isPollComplete = true
		return term.ControlledPollActionBreak

	case codecIds.challenge:
		var c codecs.Challenge
		if err := c.Decode(p.marshaller, buf, hdr.Version, hdr.BlockLength, false); err != nil {
			logger.Warningf("connect: failed to decode challenge: %s", err)
			return term.ControlledPollActionContinue
		}
		p.controlSessionID = c.ControlSessionId
		p.correlationID = c.CorrelationId
		p.relevantID = aeron.NullValue
		p.code = codecs.ControlResponseCode.NullValue
		p.version = c.Version
		p.errorMessage = ""
		p.encodedChallenge = append([]byte{}, c.EncodedChallenge...)
		p.isPollComplete = true
		return term.ControlledPollActionBreak
	}
	return term.ControlledPollActionContinue
}
