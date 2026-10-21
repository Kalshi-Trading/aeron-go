// Copyright (C) 2021-2022 Talos, Inc.
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
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/lirm/aeron-go/aeron"
	"github.com/lirm/aeron-go/aeron/idlestrategy"
	"github.com/lirm/aeron-go/archive/codecs"
	"go.uber.org/zap/zapcore"
)

// Options is the Go equivalent of Java's AeronArchive.Context.
//
// Options passed to NewArchive / NewAsyncConnect are copied and the copy is "concluded" (defaults applied,
// channels augmented); the caller's struct is never mutated so a single Options may be reused for many
// connections. The concluded copy is available as Archive.Options.
//
// Those attributes marked [init] must be changed before connecting
// Those attributes marked [runtime] may be changed at any time
// Those attributes marked [enable] may be changed when the feature is not enabled
type Options struct {
	RequestChannel         string             // [init] Control request publication channel (Java controlRequestChannel)
	RequestStream          int32              // [init] and stream (Java controlRequestStreamId)
	ResponseChannel        string             // [init] Control response subscription channel, may use control-mode=response
	ResponseStream         int32              // [init] and stream (Java controlResponseStreamId)
	RecordingEventsChannel string             // [enable] Recording progress events
	RecordingEventsStream  int32              // [enable] and stream
	ArchiveLoglevel        zapcore.Level      // [runtime] via logging.SetLevel()
	AeronLoglevel          zapcore.Level      // [runtime] via logging.SetLevel()
	Timeout                time.Duration      // [runtime] Message timeout for sending/receiving control messages and connect (Java messageTimeoutNs), default 10s
	IdleStrategy           idlestrategy.Idler // [runtime] Idlestrategy for sending/receiving control messages, defaults to a backoff strategy
	MessageRetryAttempts   int                // [init] Offer attempts for messages sent with retry (Java messageRetryAttempts), default 3
	RangeChecking          bool               // [runtime] archive protocol marshalling checks

	// ControlTermBufferLength, ControlTermBufferSparse and ControlMtuLength are applied to the control request and
	// response channels as term-length, sparse and mtu unless those params are already present on the channel.
	ControlTermBufferLength int  // [init] default 64k
	ControlTermBufferSparse bool // [init] default true
	ControlMtuLength        int  // [init] default 1408

	// ClientName is sent to the archive as part of the client info in the connect request.
	ClientName string // [init]

	// CredentialsSupplier provides credentials for the connect request and responds to challenges.
	// Defaults to NullCredentialsSupplier.
	CredentialsSupplier CredentialsSupplier // [init]

	// AuthEnabled, AuthCredentials, AuthChallenge and AuthResponse are the upstream lirm/aeron-go authentication
	// options. If CredentialsSupplier is nil and AuthEnabled is set, conclude uses a StaticCredentialsSupplier of
	// AuthCredentials and AuthResponse, and a challenge that differs from a non-nil AuthChallenge fails the connect.
	//
	// Deprecated: use CredentialsSupplier.
	AuthEnabled bool // [init]
	// Deprecated: use CredentialsSupplier.
	AuthCredentials []uint8 // [init]
	// Deprecated: use CredentialsSupplier.
	AuthChallenge []uint8 // [init]
	// Deprecated: use CredentialsSupplier.
	AuthResponse []uint8 // [init]

	// ErrorHandler, if set, replaces Listeners.ErrorListener for the connected Archive. It is also invoked for errors
	// raised while closing.
	ErrorHandler func(error) // [init]

	// RecordingSignalConsumer, if set, replaces Listeners.RecordingSignalListener for the connected Archive.
	RecordingSignalConsumer func(*codecs.RecordingSignalEvent) // [init]

	// AgentInvoker, if set, is invoked on each AsyncConnect.Poll() (Java Context.agentInvoker). The Go aeron client
	// always runs its conductor on its own goroutine so there is no conductor invoker to run.
	AgentInvoker func() int // [init]

	// Aeron is an existing client to use. If nil a client is created from the aeron.Context passed to NewArchive and
	// OwnsAeronClient is forced to true.
	Aeron *aeron.Aeron // [init]

	// OwnsAeronClient, when true, closes the Aeron client when the Archive (or a failed AsyncConnect) is closed.
	// When false the control publication and subscription are closed individually instead.
	OwnsAeronClient bool // [init]

	// Lock guards every control request/response exchange on an Archive (Java Context.lock). Defaults to a
	// sync.Mutex. Supply a no-op Locker only when an Archive is confined to a single goroutine.
	Lock sync.Locker // [init]

	concluded bool
}

const (
	controlModeResponse            = "response"
	responseCorrelationIDParamName = "response-correlation-id"
)

// Defaults matching Java AeronArchive.Configuration.
const (
	ControlTermBufferLengthDefault = 64 * 1024
	ControlTermBufferSparseDefault = true
	ControlMtuLengthDefault        = 1408
	MaxClientNameLength            = 100
	MessageRetryAttemptsDefault    = 3
	MessageTimeoutDefault          = 10 * time.Second
)

// These are the Options used by default for an Archive object
var defaultOptions = Options{
	RequestChannel:          "aeron:udp?endpoint=localhost:8010",
	RequestStream:           10,
	ResponseChannel:         "aeron:udp?endpoint=localhost:0",
	ResponseStream:          20,
	RecordingEventsChannel:  "aeron:udp?control-mode=dynamic|control=localhost:8030",
	RecordingEventsStream:   30,
	ArchiveLoglevel:         zapcore.WarnLevel,
	AeronLoglevel:           zapcore.WarnLevel,
	Timeout:                 MessageTimeoutDefault,
	MessageRetryAttempts:    MessageRetryAttemptsDefault,
	RangeChecking:           false,
	ControlTermBufferLength: ControlTermBufferLengthDefault,
	ControlTermBufferSparse: ControlTermBufferSparseDefault,
	ControlMtuLength:        ControlMtuLengthDefault,
}

// DefaultOptions creates and returns a new Options from the defaults.
func DefaultOptions() *Options {
	options := defaultOptions
	return &options
}

// Clone returns a shallow copy of the options which has not been concluded.
func (o *Options) Clone() *Options {
	c := *o
	c.concluded = false
	return &c
}

// IsConcluded reports whether these options have been concluded by a connect.
func (o *Options) IsConcluded() bool {
	return o.concluded
}

// conclude validates the options and applies defaults in place, mirroring Java AeronArchive.Context.conclude().
// The Aeron client is not created here; see NewAsyncConnect.
func (o *Options) conclude() error {
	if o.concluded {
		return fmt.Errorf("archive options already concluded")
	}
	if o.RequestChannel == "" {
		return fmt.Errorf("archive options RequestChannel must be set")
	}
	if o.ResponseChannel == "" {
		return fmt.Errorf("archive options ResponseChannel must be set")
	}
	if len(o.ClientName) > MaxClientNameLength {
		return fmt.Errorf("archive options ClientName length must be <= %d", MaxClientNameLength)
	}
	if o.Timeout <= 0 {
		return fmt.Errorf("archive options Timeout must be > 0, got %s", o.Timeout)
	}
	if o.MessageRetryAttempts == 0 {
		o.MessageRetryAttempts = MessageRetryAttemptsDefault
	}
	if o.MessageRetryAttempts < 0 {
		return fmt.Errorf("archive options MessageRetryAttempts must be > 0, got %d", o.MessageRetryAttempts)
	}
	if o.ControlTermBufferLength == 0 {
		o.ControlTermBufferLength = ControlTermBufferLengthDefault
	}
	if o.ControlMtuLength == 0 {
		o.ControlMtuLength = ControlMtuLengthDefault
	}

	request, err := o.applyDefaultParams(o.RequestChannel)
	if err != nil {
		return err
	}
	response, err := o.applyDefaultParams(o.ResponseChannel)
	if err != nil {
		return err
	}
	o.RequestChannel = request.String()
	o.ResponseChannel = response.String()

	if o.IdleStrategy == nil {
		o.IdleStrategy = idlestrategy.NewDefaultBackoffIdleStrategy()
	}
	if o.CredentialsSupplier == nil {
		if o.AuthEnabled {
			o.CredentialsSupplier = StaticCredentialsSupplier{Credentials: o.AuthCredentials, ChallengeResponse: o.AuthResponse}
		} else {
			o.CredentialsSupplier = NullCredentialsSupplier{}
		}
	}
	if o.Lock == nil {
		o.Lock = &sync.Mutex{}
	}
	o.concluded = true
	return nil
}

// applySessionID sets the session-id of the control request and response channels to the id allocated by the
// driver, as Java Context.conclude() does with aeron.nextSessionId(controlRequestStreamId). Channels using
// control-mode=response are left alone.
func (o *Options) applySessionID(nextSessionID func(streamID int32) (int32, error)) error {
	response, err := aeron.ParseChannelUri(o.ResponseChannel)
	if err != nil {
		return err
	}
	if isControlModeResponse(response) {
		return nil
	}
	request, err := aeron.ParseChannelUri(o.RequestChannel)
	if err != nil {
		return err
	}
	id, err := nextSessionID(o.RequestStream)
	if err != nil {
		return err
	}
	sessionID := strconv.Itoa(int(id))
	request.Set(aeron.SessionIdParamName, sessionID)
	response.Set(aeron.SessionIdParamName, sessionID)
	o.RequestChannel = request.String()
	o.ResponseChannel = response.String()
	return nil
}

func (o *Options) applyDefaultParams(channel string) (aeron.ChannelUri, error) {
	uri, err := aeron.ParseChannelUri(channel)
	if err != nil {
		return aeron.ChannelUri{}, err
	}
	if uri.Get(aeron.TermLengthParamName) == "" {
		uri.Set(aeron.TermLengthParamName, strconv.Itoa(o.ControlTermBufferLength))
	}
	if uri.Get(aeron.MtuLengthParamName) == "" {
		uri.Set(aeron.MtuLengthParamName, strconv.Itoa(o.ControlMtuLength))
	}
	if uri.Get(aeron.SparseParamName) == "" {
		uri.Set(aeron.SparseParamName, strconv.FormatBool(o.ControlTermBufferSparse))
	}
	return uri, nil
}

func isControlModeResponse(uri aeron.ChannelUri) bool {
	return uri.Get(aeron.MdcControlModeParamName) == controlModeResponse
}

// CredentialsSupplier supplies credentials to the archive on connect and answers authentication challenges.
// Equivalent to Java io.aeron.security.CredentialsSupplier.
type CredentialsSupplier interface {
	// EncodedCredentials returns the credentials sent in the connect request.
	EncodedCredentials() []byte
	// OnChallenge returns the credentials to send in response to an encoded challenge from the archive.
	OnChallenge(encodedChallenge []byte) []byte
}

// NullCredentialsSupplier supplies empty credentials.
type NullCredentialsSupplier struct{}

func (NullCredentialsSupplier) EncodedCredentials() []byte  { return nil }
func (NullCredentialsSupplier) OnChallenge(_ []byte) []byte { return nil }

// StaticCredentialsSupplier supplies fixed credentials and a fixed challenge response.
type StaticCredentialsSupplier struct {
	Credentials       []byte
	ChallengeResponse []byte
}

func (s StaticCredentialsSupplier) EncodedCredentials() []byte  { return s.Credentials }
func (s StaticCredentialsSupplier) OnChallenge(_ []byte) []byte { return s.ChallengeResponse }

// CredentialsSupplierFuncs adapts a pair of functions to a CredentialsSupplier. Nil functions return nil.
type CredentialsSupplierFuncs struct {
	Credentials func() []byte
	Challenge   func(encodedChallenge []byte) []byte
}

func (f CredentialsSupplierFuncs) EncodedCredentials() []byte {
	if f.Credentials == nil {
		return nil
	}
	return f.Credentials()
}

func (f CredentialsSupplierFuncs) OnChallenge(encodedChallenge []byte) []byte {
	if f.Challenge == nil {
		return nil
	}
	return f.Challenge(encodedChallenge)
}
