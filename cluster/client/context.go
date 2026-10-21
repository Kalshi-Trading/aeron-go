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

package client

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/lirm/aeron-go/aeron"
	"github.com/lirm/aeron-go/aeron/idlestrategy"
)

const (
	// DefaultMessageTimeout matches AeronCluster.Configuration.MESSAGE_TIMEOUT_DEFAULT_NS.
	DefaultMessageTimeout = 5 * time.Second
	// DefaultIngressStreamId matches AeronCluster.Configuration.INGRESS_STREAM_ID_DEFAULT.
	DefaultIngressStreamId int32 = 101
	// DefaultEgressStreamId matches AeronCluster.Configuration.EGRESS_STREAM_ID_DEFAULT.
	DefaultEgressStreamId int32 = 102
	// DefaultEgressChannel is used when Context.EgressChannel is left at its NewContext value.
	DefaultEgressChannel = "aeron:udp?endpoint=localhost:0"
	// DefaultLeaderHeartbeatTimeout matches ConsensusModule.Configuration.LEADER_HEARTBEAT_TIMEOUT_DEFAULT_NS
	// and is used to derive NewLeaderTimeout when the cluster does not report its own value.
	DefaultLeaderHeartbeatTimeout = 10 * time.Second
	// MaxClientNameLength matches Aeron.Configuration.MAX_CLIENT_NAME_LENGTH.
	MaxClientNameLength = 100
)

var (
	// ErrConfiguration is wrapped by errors from Context.Conclude and endpoint parsing.
	ErrConfiguration = errors.New("cluster client configuration error")
	// ErrCluster is wrapped by errors reported by the cluster (EventCode.ERROR) or protocol violations.
	ErrCluster = errors.New("cluster error")
	// ErrAuthenticationRejected is returned when the cluster rejects the session credentials.
	ErrAuthenticationRejected = errors.New("cluster authentication rejected")
	// ErrTimeout is wrapped by connect and new leader publication timeouts.
	ErrTimeout = errors.New("cluster client timeout")
	// ErrConcurrentConclude is returned when a Context is concluded more than once (Java
	// ConcurrentConcludeException). The context, and any Aeron client it owns, is left untouched.
	ErrConcurrentConclude = errors.New("cluster client context already concluded")
)

// Context holds the configuration for an AeronCluster client, mirroring io.aeron.cluster.client.AeronCluster.Context.
// Create with NewContext, adjust fields, then pass to Connect or NewAsyncConnect which conclude it. A Context must
// not be reused for more than one connection.
type Context struct {
	// MessageTimeout bounds connect and new leader ingress publication creation.
	MessageTimeout time.Duration
	// NewLeaderTimeout is how long the client waits for a new leader after losing the egress image or ingress
	// connection before closing. Zero means 2x the leader heartbeat timeout reported by the cluster on connect.
	NewLeaderTimeout time.Duration
	// IngressEndpoints is a comma separated list of memberId=host:port. Empty means use IngressChannel as is.
	IngressEndpoints string
	// IngressChannel is the ingress channel; when IngressEndpoints is set the endpoint is substituted per member.
	IngressChannel  string
	IngressStreamId int32
	// EgressChannel must be a channel the cluster can reach; for UDP rejoin=false is applied on conclude.
	EgressChannel  string
	EgressStreamId int32
	// IdleStrategy used while blocking in Connect and when retrying sends.
	IdleStrategy idlestrategy.Idler
	// AeronDirectoryName is used when Aeron is nil to connect a new client.
	AeronDirectoryName string
	// Aeron is an existing client to use. If nil one is created on conclude and OwnsAeronClient is set.
	Aeron *aeron.Aeron
	// OwnsAeronClient closes Aeron when the cluster client (or a failed connect) is closed.
	OwnsAeronClient bool
	// IsIngressExclusive selects exclusive (default) or concurrent ingress publications.
	IsIngressExclusive bool
	// CredentialsSupplier provides credentials and challenge responses. Defaults to NullCredentialsSupplier.
	CredentialsSupplier CredentialsSupplier
	// ErrorHandler receives asynchronous errors. Defaults to logging.
	ErrorHandler func(error)
	// EgressListener is invoked by AeronCluster.PollEgress.
	EgressListener EgressListener
	// ControlledEgressListener is invoked by AeronCluster.ControlledPollEgress.
	ControlledEgressListener ControlledEgressListener
	// ClientName is sent in the connect request client info and used for a created Aeron client.
	ClientName string
	// AgentInvoker, if set, is invoked while connecting/awaiting publications (e.g. an embedded driver).
	AgentInvoker func() int

	isConcluded bool
}

// NewContext returns a Context populated with the Java client defaults. IngressChannel must still be set.
func NewContext() *Context {
	return &Context{
		MessageTimeout:     DefaultMessageTimeout,
		IngressStreamId:    DefaultIngressStreamId,
		EgressChannel:      DefaultEgressChannel,
		EgressStreamId:     DefaultEgressStreamId,
		IsIngressExclusive: true,
	}
}

// IsConcluded reports whether Conclude has been called.
func (ctx *Context) IsConcluded() bool {
	return ctx.isConcluded
}

// Conclude validates the configuration, applies defaults and connects an Aeron client if one was not supplied.
func (ctx *Context) Conclude() error {
	if ctx.isConcluded {
		return ErrConcurrentConclude
	}
	ctx.isConcluded = true

	if ctx.IngressChannel == "" {
		return fmt.Errorf("%w: IngressChannel must be specified", ErrConfiguration)
	}
	if strings.HasPrefix(ctx.IngressChannel, "aeron:ipc") && ctx.IngressEndpoints != "" {
		return fmt.Errorf("%w: IngressEndpoints must be empty when using IPC ingress", ErrConfiguration)
	}
	if ctx.EgressChannel == "" {
		return fmt.Errorf("%w: EgressChannel must be specified", ErrConfiguration)
	}
	egressUri, err := aeron.ParseChannelUri(ctx.EgressChannel)
	if err != nil {
		return fmt.Errorf("%w: invalid EgressChannel: %v", ErrConfiguration, err)
	}
	if egressUri.IsUdp() {
		egressUri.Set("rejoin", "false")
		ctx.EgressChannel = egressUri.String()
	}
	if _, err := aeron.ParseChannelUri(ctx.IngressChannel); err != nil {
		return fmt.Errorf("%w: invalid IngressChannel: %v", ErrConfiguration, err)
	}
	if len(ctx.ClientName) > MaxClientNameLength {
		return fmt.Errorf("%w: ClientName length must be <= %d", ErrConfiguration, MaxClientNameLength)
	}
	if ctx.MessageTimeout <= 0 {
		ctx.MessageTimeout = DefaultMessageTimeout
	}
	if ctx.NewLeaderTimeout < 0 {
		return fmt.Errorf("%w: NewLeaderTimeout must be positive or zero (derived)", ErrConfiguration)
	}
	if ctx.ErrorHandler == nil {
		ctx.ErrorHandler = func(err error) { logger.Errorf("cluster client error: %v", err) }
	}
	if ctx.IdleStrategy == nil {
		ctx.IdleStrategy = idlestrategy.NewDefaultBackoffIdleStrategy()
	}
	if ctx.CredentialsSupplier == nil {
		ctx.CredentialsSupplier = NullCredentialsSupplier{}
	}
	if ctx.EgressListener == nil {
		ctx.EgressListener = missingEgressListener{}
	}
	if ctx.ControlledEgressListener == nil {
		ctx.ControlledEgressListener = missingControlledEgressListener{}
	}
	if ctx.Aeron == nil {
		clientName := ctx.ClientName
		if clientName == "" {
			clientName = "cluster-client"
		}
		aeronCtx := aeron.NewContext().ErrorHandler(ctx.ErrorHandler).ClientName(clientName)
		if ctx.AeronDirectoryName != "" {
			aeronCtx.AeronDir(ctx.AeronDirectoryName)
		}
		a, err := aeron.Connect(aeronCtx)
		if err != nil {
			return err
		}
		ctx.Aeron = a
		ctx.OwnsAeronClient = true
	}
	return nil
}

// Close closes the Aeron client if it is owned by this context.
func (ctx *Context) Close() error {
	if ctx.OwnsAeronClient && ctx.Aeron != nil && !ctx.Aeron.IsClosed() {
		return ctx.Aeron.Close()
	}
	return nil
}

func (ctx *Context) newLeaderTimeout() time.Duration {
	if ctx.NewLeaderTimeout > 0 {
		return ctx.NewLeaderTimeout
	}
	return 2 * DefaultLeaderHeartbeatTimeout
}

func (ctx *Context) runAgentInvokers() int {
	if ctx.AgentInvoker != nil {
		return ctx.AgentInvoker()
	}
	return 0
}

func (ctx *Context) idle(workCount int) {
	ctx.IdleStrategy.Idle(workCount)
}

func (ctx *Context) resetIdle() {
	if r, ok := ctx.IdleStrategy.(interface{ Reset() }); ok {
		r.Reset()
	}
}

// CredentialsSupplier mirrors io.aeron.security.CredentialsSupplier.
type CredentialsSupplier interface {
	// EncodedCredentials are sent in the SessionConnectRequest.
	EncodedCredentials() []byte
	// OnChallenge returns the encoded credentials sent in reply to a Challenge.
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

// CredentialsSupplierFuncs adapts functions to a CredentialsSupplier.
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
