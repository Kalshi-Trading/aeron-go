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
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/lirm/aeron-go/aeron/logbuffer"
)

// ChannelUriStringBuilder is a type-safe builder for channel URI strings, a port of
// io.aeron.ChannelUriStringBuilder.
//
// Setters are fluent. The first validation error encountered is recorded (the offending setter leaves the field
// unchanged) and returned by Build, Validate and Err. Unset a parameter with Unset(paramName) and reset everything
// with Clear. String-valued parameters treat "" as unset.
type ChannelUriStringBuilder struct {
	prefix                string
	media                 string
	endpoint              *string
	networkInterface      *string
	controlEndpoint       *string
	controlMode           *string
	tags                  *string
	alias                 *string
	cc                    *string
	fc                    *string
	mediaRcvTsOffset      *string
	channelRcvTsOffset    *string
	channelSndTsOffset    *string
	responseCorrelationId *string
	responseEndpoint      *string

	reliable *bool
	sparse   *bool
	eos      *bool
	tether   *bool
	group    *bool
	rejoin   *bool
	ssc      *bool

	ttl                     *int32
	mtu                     *int32
	termLength              *int32
	initialTermId           *int32
	termId                  *int32
	termOffset              *int32
	socketSndbufLength      *int32
	socketRcvbufLength      *int32
	socketTos               *int32
	receiverWindowLength    *int32
	maxResend               *int32
	streamId                *int32
	publicationWindowLength *int32

	sessionId                    *int64
	groupTag                     *int64
	lingerNs                     *int64
	nakDelayNs                   *int64
	untetheredWindowLimitTimeout *int64
	untetheredLingerTimeout      *int64
	untetheredRestingTimeout     *int64

	isSessionIdTagged bool
	err               error
}

func cubPtr[T any](v T) *T {
	return &v
}

// NewChannelUriStringBuilder returns an empty builder.
func NewChannelUriStringBuilder() *ChannelUriStringBuilder {
	return &ChannelUriStringBuilder{}
}

// NewChannelUriStringBuilderFromString returns a builder initialised from an existing channel URI string.
func NewChannelUriStringBuilderFromString(uri string) (*ChannelUriStringBuilder, error) {
	channelUri, err := ParseChannelUri(uri)
	if err != nil {
		return nil, err
	}
	return NewChannelUriStringBuilderFromUri(channelUri)
}

// NewChannelUriStringBuilderFromUri returns a builder initialised from all known parameters of an existing
// ChannelUri. Unknown parameters are dropped.
func NewChannelUriStringBuilderFromUri(channelUri ChannelUri) (*ChannelUriStringBuilder, error) {
	b := NewChannelUriStringBuilder().Prefix(channelUri.Prefix()).Media(channelUri.Media())
	b.CopyFrom(channelUri, channelUriBuilderParams...)
	if b.err != nil {
		return nil, b.err
	}
	return b, nil
}

// channelUriBuilderParams are all parameters known by the builder, in build order.
var channelUriBuilderParams = []string{
	TagsParamName,
	EndpointParamName,
	InterfaceParamName,
	MdcControlParamName,
	MdcControlModeParamName,
	MtuLengthParamName,
	TermLengthParamName,
	InitialTermIdParamName,
	TermIdParamName,
	TermOffsetParamName,
	SessionIdParamName,
	TtlParamName,
	ReliableStreamParamName,
	LingerParamName,
	AliasParamName,
	CongestionControlParamName,
	FlowControlParamName,
	GroupTagParamName,
	SparseParamName,
	EosParamName,
	TetherParamName,
	GroupParamName,
	RejoinParamName,
	SpiesSimulateConnectionParamName,
	SocketSndbufParamName,
	SocketRcvbufParamName,
	SocketTosParamName,
	ReceiverWindowLengthParamName,
	MediaRcvTimestampOffsetParamName,
	ChannelRcvTimestampOffsetParamName,
	ChannelSndTimestampOffsetParamName,
	ResponseEndpointParamName,
	ResponseCorrelationIdParamName,
	NakDelayParamName,
	UntetheredWindowLimitTimeoutParamName,
	UntetheredLingerTimeoutParamName,
	UntetheredRestingTimeoutParamName,
	MaxResendParamName,
	StreamIdParamName,
	PublicationWindowLengthParamName,
}

func (b *ChannelUriStringBuilder) fail(err error) *ChannelUriStringBuilder {
	if b.err == nil {
		b.err = err
	}
	return b
}

func (b *ChannelUriStringBuilder) failf(format string, args ...interface{}) *ChannelUriStringBuilder {
	return b.fail(fmt.Errorf(format, args...))
}

// Err returns the first error recorded by a setter, if any.
func (b *ChannelUriStringBuilder) Err() error {
	return b.err
}

// Clear resets all fields, including any recorded error.
func (b *ChannelUriStringBuilder) Clear() *ChannelUriStringBuilder {
	*b = ChannelUriStringBuilder{}
	return b
}

// Validate checks the combination of parameters is valid, returning the first error found (including errors
// recorded by setters).
func (b *ChannelUriStringBuilder) Validate() error {
	if b.err != nil {
		return b.err
	}
	if b.media == "" {
		return errors.New("media type is mandatory")
	}
	if b.media == UdpMedia && b.endpoint == nil && b.controlEndpoint == nil {
		return errors.New("either 'endpoint' or 'control' must be specified for UDP.")
	}
	anyNonNil := b.initialTermId != nil || b.termId != nil || b.termOffset != nil
	anyNil := b.initialTermId == nil || b.termId == nil || b.termOffset == nil
	if anyNonNil {
		if anyNil {
			return errors.New(
				"either all or none of the parameters ['initialTermId', 'termId', 'termOffset'] must be provided")
		}
		if *b.termId-*b.initialTermId < 0 {
			return fmt.Errorf("difference greater than 2^31 - 1: termId=%d - initialTermId=%d",
				*b.termId, *b.initialTermId)
		}
		if b.termLength != nil && *b.termOffset > *b.termLength {
			return fmt.Errorf("termOffset=%d > termLength=%d", *b.termOffset, *b.termLength)
		}
	}
	return nil
}

// Prefix sets the prefix, which must be "" or "aeron-spy".
func (b *ChannelUriStringBuilder) Prefix(prefix string) *ChannelUriStringBuilder {
	if prefix != "" && prefix != SpyQualifier {
		return b.failf("invalid prefix: %s", prefix)
	}
	b.prefix = prefix
	return b
}

// GetPrefix returns the prefix.
func (b *ChannelUriStringBuilder) GetPrefix() string { return b.prefix }

// Media sets the media, which must be "udp" or "ipc".
func (b *ChannelUriStringBuilder) Media(media string) *ChannelUriStringBuilder {
	if media != UdpMedia && media != IpcMedia {
		return b.failf("invalid media: %s", media)
	}
	b.media = media
	return b
}

// GetMedia returns the media.
func (b *ChannelUriStringBuilder) GetMedia() string { return b.media }

// Endpoint sets the endpoint address (host:port) for UDP.
func (b *ChannelUriStringBuilder) Endpoint(endpoint string) *ChannelUriStringBuilder {
	b.endpoint = &endpoint
	return b
}

// GetEndpoint returns the endpoint.
func (b *ChannelUriStringBuilder) GetEndpoint() string { return strValue(b.endpoint) }

// NetworkInterface sets the interface (address[:port][/prefix]) for UDP.
func (b *ChannelUriStringBuilder) NetworkInterface(networkInterface string) *ChannelUriStringBuilder {
	b.networkInterface = &networkInterface
	return b
}

// GetNetworkInterface returns the interface.
func (b *ChannelUriStringBuilder) GetNetworkInterface() string { return strValue(b.networkInterface) }

// ControlEndpoint sets the MDC/response control endpoint.
func (b *ChannelUriStringBuilder) ControlEndpoint(controlEndpoint string) *ChannelUriStringBuilder {
	b.controlEndpoint = &controlEndpoint
	return b
}

// GetControlEndpoint returns the control endpoint.
func (b *ChannelUriStringBuilder) GetControlEndpoint() string { return strValue(b.controlEndpoint) }

// ControlMode sets the control mode: "manual", "dynamic" or "response" (use Unset to clear it).
func (b *ChannelUriStringBuilder) ControlMode(controlMode string) *ChannelUriStringBuilder {
	if controlMode != MdcControlModeManual && controlMode != MdcControlModeDynamic &&
		controlMode != ControlModeResponse {
		return b.failf("invalid control mode: %s", controlMode)
	}
	b.controlMode = &controlMode
	return b
}

// GetControlMode returns the control mode.
func (b *ChannelUriStringBuilder) GetControlMode() string { return strValue(b.controlMode) }

// Tags sets the raw tags param, e.g. "1001,1002".
func (b *ChannelUriStringBuilder) Tags(tags string) *ChannelUriStringBuilder {
	b.tags = &tags
	return b
}

// ChannelTag sets the tags param to a single channel tag.
func (b *ChannelUriStringBuilder) ChannelTag(channelTag int64) *ChannelUriStringBuilder {
	return b.Tags(strconv.FormatInt(channelTag, 10))
}

// ChannelAndEntityTags sets the tags param to "channelTag,entityTag".
func (b *ChannelUriStringBuilder) ChannelAndEntityTags(channelTag int64, entityTag int64) *ChannelUriStringBuilder {
	return b.Tags(strconv.FormatInt(channelTag, 10) + "," + strconv.FormatInt(entityTag, 10))
}

// GetTags returns the tags param.
func (b *ChannelUriStringBuilder) GetTags() string { return strValue(b.tags) }

// IsSessionIDTagged sets whether the session id is rendered as a tag reference ("tag:<id>").
func (b *ChannelUriStringBuilder) IsSessionIDTagged(isSessionIdTagged bool) *ChannelUriStringBuilder {
	b.isSessionIdTagged = isSessionIdTagged
	return b
}

// GetIsSessionIDTagged returns whether the session id is tagged.
func (b *ChannelUriStringBuilder) GetIsSessionIDTagged() bool { return b.isSessionIdTagged }

// Alias sets the alias param.
func (b *ChannelUriStringBuilder) Alias(alias string) *ChannelUriStringBuilder {
	b.alias = &alias
	return b
}

// GetAlias returns the alias.
func (b *ChannelUriStringBuilder) GetAlias() string { return strValue(b.alias) }

// CongestionControl sets the cc param, e.g. "static" or "cubic".
func (b *ChannelUriStringBuilder) CongestionControl(congestionControl string) *ChannelUriStringBuilder {
	b.cc = &congestionControl
	return b
}

// GetCongestionControl returns the cc param.
func (b *ChannelUriStringBuilder) GetCongestionControl() string { return strValue(b.cc) }

// FlowControl sets the raw fc param, e.g. "max", "min,g:/2,t:5s" or "tagged,g:100/2".
func (b *ChannelUriStringBuilder) FlowControl(flowControl string) *ChannelUriStringBuilder {
	b.fc = &flowControl
	return b
}

// MaxFlowControl sets fc=max.
func (b *ChannelUriStringBuilder) MaxFlowControl() *ChannelUriStringBuilder {
	return b.FlowControl("max")
}

// MinFlowControl sets fc=min with an optional min group size and timeout (e.g. "5s"); nil/"" omit an option.
func (b *ChannelUriStringBuilder) MinFlowControl(minGroupSize *int32, timeout string) *ChannelUriStringBuilder {
	value := "min"
	if minGroupSize != nil {
		value += ",g:/" + strconv.Itoa(int(*minGroupSize))
	}
	if timeout != "" {
		value += ",t:" + timeout
	}
	return b.FlowControl(value)
}

// TaggedFlowControl sets fc=tagged with an optional group tag, min group size and timeout; nil/"" omit an option.
func (b *ChannelUriStringBuilder) TaggedFlowControl(
	groupTag *int64, minGroupSize *int32, timeout string) *ChannelUriStringBuilder {
	value := "tagged"
	if groupTag != nil || minGroupSize != nil {
		value += ",g:"
		if groupTag != nil {
			value += strconv.FormatInt(*groupTag, 10)
		}
		if minGroupSize != nil {
			value += "/" + strconv.Itoa(int(*minGroupSize))
		}
	}
	if timeout != "" {
		value += ",t:" + timeout
	}
	return b.FlowControl(value)
}

// GetFlowControl returns the fc param.
func (b *ChannelUriStringBuilder) GetFlowControl() string { return strValue(b.fc) }

// Reliable sets the reliable param.
func (b *ChannelUriStringBuilder) Reliable(reliable bool) *ChannelUriStringBuilder {
	b.reliable = cubPtr(reliable)
	return b
}

// GetReliable returns the reliable param and whether it is set.
func (b *ChannelUriStringBuilder) GetReliable() (bool, bool) { return getOpt(b.reliable) }

// Ttl sets the multicast TTL (0-255).
func (b *ChannelUriStringBuilder) Ttl(ttl int32) *ChannelUriStringBuilder {
	if ttl < 0 || ttl > 255 {
		return b.failf("TTL not in range 0-255: %d", ttl)
	}
	b.ttl = cubPtr(ttl)
	return b
}

// GetTtl returns the ttl and whether it is set.
func (b *ChannelUriStringBuilder) GetTtl() (int32, bool) { return getOpt(b.ttl) }

// Mtu sets the MTU (32-65504, multiple of 32).
func (b *ChannelUriStringBuilder) Mtu(mtu int32) *ChannelUriStringBuilder {
	if mtu < 32 || mtu > 65504 {
		return b.failf("MTU not in range 32-65504: %d", mtu)
	}
	if mtu&(logbuffer.FrameAlignment-1) != 0 {
		return b.failf("MTU not a multiple of FRAME_ALIGNMENT: mtu=%d", mtu)
	}
	b.mtu = cubPtr(mtu)
	return b
}

// GetMtu returns the mtu and whether it is set.
func (b *ChannelUriStringBuilder) GetMtu() (int32, bool) { return getOpt(b.mtu) }

// TermLength sets the term length (power of 2 between 64k and 1g).
func (b *ChannelUriStringBuilder) TermLength(termLength int32) *ChannelUriStringBuilder {
	if err := uriCheckTermLength(termLength); err != nil {
		return b.fail(err)
	}
	b.termLength = cubPtr(termLength)
	return b
}

// GetTermLength returns the term length and whether it is set.
func (b *ChannelUriStringBuilder) GetTermLength() (int32, bool) { return getOpt(b.termLength) }

// InitialTermID sets the init-term-id param.
func (b *ChannelUriStringBuilder) InitialTermID(initialTermId int32) *ChannelUriStringBuilder {
	b.initialTermId = cubPtr(initialTermId)
	return b
}

// GetInitialTermID returns init-term-id and whether it is set.
func (b *ChannelUriStringBuilder) GetInitialTermID() (int32, bool) { return getOpt(b.initialTermId) }

// TermID sets the term-id param.
func (b *ChannelUriStringBuilder) TermID(termId int32) *ChannelUriStringBuilder {
	b.termId = cubPtr(termId)
	return b
}

// GetTermID returns term-id and whether it is set.
func (b *ChannelUriStringBuilder) GetTermID() (int32, bool) { return getOpt(b.termId) }

// TermOffset sets the term-offset param (0-1g, multiple of 32).
func (b *ChannelUriStringBuilder) TermOffset(termOffset int32) *ChannelUriStringBuilder {
	if termOffset < 0 || termOffset > uriTermMaxLength {
		return b.failf("term offset not in range 0-1g: %d", termOffset)
	}
	if termOffset&(logbuffer.FrameAlignment-1) != 0 {
		return b.failf("term offset not multiple of FRAME_ALIGNMENT: %d", termOffset)
	}
	b.termOffset = cubPtr(termOffset)
	return b
}

// GetTermOffset returns term-offset and whether it is set.
func (b *ChannelUriStringBuilder) GetTermOffset() (int32, bool) { return getOpt(b.termOffset) }

// SessionID sets an explicit (untagged) session id.
func (b *ChannelUriStringBuilder) SessionID(sessionId int32) *ChannelUriStringBuilder {
	b.isSessionIdTagged = false
	b.sessionId = cubPtr(int64(sessionId))
	return b
}

// TaggedSessionID sets the session id to a tag reference, rendered as "session-id=tag:<tag>".
func (b *ChannelUriStringBuilder) TaggedSessionID(tag int64) *ChannelUriStringBuilder {
	b.isSessionIdTagged = true
	b.sessionId = cubPtr(tag)
	return b
}

// GetSessionID returns the session id (or tag if tagged) and whether it is set.
func (b *ChannelUriStringBuilder) GetSessionID() (int64, bool) { return getOpt(b.sessionId) }

// Linger sets the linger timeout.
func (b *ChannelUriStringBuilder) Linger(linger time.Duration) *ChannelUriStringBuilder {
	return b.setNonNegativeNs(&b.lingerNs, LingerParamName, int64(linger))
}

// GetLinger returns the linger timeout and whether it is set.
func (b *ChannelUriStringBuilder) GetLinger() (time.Duration, bool) { return getDuration(b.lingerNs) }

// Sparse sets the sparse param.
func (b *ChannelUriStringBuilder) Sparse(sparse bool) *ChannelUriStringBuilder {
	b.sparse = cubPtr(sparse)
	return b
}

// GetSparse returns sparse and whether it is set.
func (b *ChannelUriStringBuilder) GetSparse() (bool, bool) { return getOpt(b.sparse) }

// Eos sets the eos param.
func (b *ChannelUriStringBuilder) Eos(eos bool) *ChannelUriStringBuilder {
	b.eos = cubPtr(eos)
	return b
}

// GetEos returns eos and whether it is set.
func (b *ChannelUriStringBuilder) GetEos() (bool, bool) { return getOpt(b.eos) }

// Tether sets the tether param.
func (b *ChannelUriStringBuilder) Tether(tether bool) *ChannelUriStringBuilder {
	b.tether = cubPtr(tether)
	return b
}

// GetTether returns tether and whether it is set.
func (b *ChannelUriStringBuilder) GetTether() (bool, bool) { return getOpt(b.tether) }

// Group sets the group param.
func (b *ChannelUriStringBuilder) Group(group bool) *ChannelUriStringBuilder {
	b.group = cubPtr(group)
	return b
}

// GetGroup returns group and whether it is set.
func (b *ChannelUriStringBuilder) GetGroup() (bool, bool) { return getOpt(b.group) }

// Rejoin sets the rejoin param.
func (b *ChannelUriStringBuilder) Rejoin(rejoin bool) *ChannelUriStringBuilder {
	b.rejoin = cubPtr(rejoin)
	return b
}

// GetRejoin returns rejoin and whether it is set.
func (b *ChannelUriStringBuilder) GetRejoin() (bool, bool) { return getOpt(b.rejoin) }

// SpiesSimulateConnection sets the ssc param.
func (b *ChannelUriStringBuilder) SpiesSimulateConnection(ssc bool) *ChannelUriStringBuilder {
	b.ssc = cubPtr(ssc)
	return b
}

// GetSpiesSimulateConnection returns ssc and whether it is set.
func (b *ChannelUriStringBuilder) GetSpiesSimulateConnection() (bool, bool) { return getOpt(b.ssc) }

// GroupTag sets the gtag param.
func (b *ChannelUriStringBuilder) GroupTag(groupTag int64) *ChannelUriStringBuilder {
	b.groupTag = cubPtr(groupTag)
	return b
}

// GetGroupTag returns gtag and whether it is set.
func (b *ChannelUriStringBuilder) GetGroupTag() (int64, bool) { return getOpt(b.groupTag) }

// InitialPosition sets init-term-id, term-id, term-offset and term-length so a publication starts at position.
func (b *ChannelUriStringBuilder) InitialPosition(
	position int64, initialTermId int32, termLength int32) *ChannelUriStringBuilder {
	if position < 0 {
		return b.failf("invalid position=%d < 0", position)
	}
	if position&int64(logbuffer.FrameAlignment-1) != 0 {
		return b.failf("invalid position=%d does not have frame alignment=%d", position, logbuffer.FrameAlignment)
	}
	bitsToShift, err := uriPositionBitsToShift(termLength)
	if err != nil {
		return b.fail(err)
	}
	b.initialTermId = cubPtr(initialTermId)
	b.termId = cubPtr(uriComputeTermIdFromPosition(position, bitsToShift, initialTermId))
	b.termOffset = cubPtr(int32(position & int64(termLength-1)))
	b.termLength = cubPtr(termLength)
	return b
}

// SocketSndbufLength sets so-sndbuf.
func (b *ChannelUriStringBuilder) SocketSndbufLength(length int32) *ChannelUriStringBuilder {
	return b.setNonNegative32(&b.socketSndbufLength, SocketSndbufParamName, length)
}

// GetSocketSndbufLength returns so-sndbuf and whether it is set.
func (b *ChannelUriStringBuilder) GetSocketSndbufLength() (int32, bool) {
	return getOpt(b.socketSndbufLength)
}

// SocketRcvbufLength sets so-rcvbuf.
func (b *ChannelUriStringBuilder) SocketRcvbufLength(length int32) *ChannelUriStringBuilder {
	return b.setNonNegative32(&b.socketRcvbufLength, SocketRcvbufParamName, length)
}

// GetSocketRcvbufLength returns so-rcvbuf and whether it is set.
func (b *ChannelUriStringBuilder) GetSocketRcvbufLength() (int32, bool) {
	return getOpt(b.socketRcvbufLength)
}

// SocketTos sets so-tos (IP_TOS, 0-255). Added in Aeron 1.54; older drivers ignore it.
func (b *ChannelUriStringBuilder) SocketTos(tos int32) *ChannelUriStringBuilder {
	if tos < 0 || tos > 255 {
		return b.failf("%s must be between 0 and 255 inclusive: %d", SocketTosParamName, tos)
	}
	b.socketTos = cubPtr(tos)
	return b
}

// GetSocketTos returns so-tos and whether it is set.
func (b *ChannelUriStringBuilder) GetSocketTos() (int32, bool) { return getOpt(b.socketTos) }

// ReceiverWindowLength sets rcv-wnd.
func (b *ChannelUriStringBuilder) ReceiverWindowLength(length int32) *ChannelUriStringBuilder {
	return b.setNonNegative32(&b.receiverWindowLength, ReceiverWindowLengthParamName, length)
}

// GetReceiverWindowLength returns rcv-wnd and whether it is set.
func (b *ChannelUriStringBuilder) GetReceiverWindowLength() (int32, bool) {
	return getOpt(b.receiverWindowLength)
}

func validateTimestampOffset(name string, offset string) error {
	if offset != ReservedOffset {
		if _, err := strconv.ParseInt(offset, 10, 32); err != nil {
			return fmt.Errorf("%s must be a number or the value '%s' found: %s", name, ReservedOffset, offset)
		}
	}
	return nil
}

// MediaReceiveTimestampOffset sets media-rcv-ts-offset: a number or "reserved".
func (b *ChannelUriStringBuilder) MediaReceiveTimestampOffset(offset string) *ChannelUriStringBuilder {
	if err := validateTimestampOffset("mediaReceiveTimestampOffset", offset); err != nil {
		return b.fail(err)
	}
	b.mediaRcvTsOffset = &offset
	return b
}

// GetMediaReceiveTimestampOffset returns media-rcv-ts-offset.
func (b *ChannelUriStringBuilder) GetMediaReceiveTimestampOffset() string {
	return strValue(b.mediaRcvTsOffset)
}

// ChannelReceiveTimestampOffset sets channel-rcv-ts-offset: a number or "reserved".
func (b *ChannelUriStringBuilder) ChannelReceiveTimestampOffset(offset string) *ChannelUriStringBuilder {
	if err := validateTimestampOffset("channelReceiveTimestampOffset", offset); err != nil {
		return b.fail(err)
	}
	b.channelRcvTsOffset = &offset
	return b
}

// GetChannelReceiveTimestampOffset returns channel-rcv-ts-offset.
func (b *ChannelUriStringBuilder) GetChannelReceiveTimestampOffset() string {
	return strValue(b.channelRcvTsOffset)
}

// ChannelSendTimestampOffset sets channel-snd-ts-offset: a number or "reserved".
func (b *ChannelUriStringBuilder) ChannelSendTimestampOffset(offset string) *ChannelUriStringBuilder {
	if err := validateTimestampOffset("channelSendTimestampOffset", offset); err != nil {
		return b.fail(err)
	}
	b.channelSndTsOffset = &offset
	return b
}

// GetChannelSendTimestampOffset returns channel-snd-ts-offset.
func (b *ChannelUriStringBuilder) GetChannelSendTimestampOffset() string {
	return strValue(b.channelSndTsOffset)
}

// ResponseEndpoint sets response-endpoint.
func (b *ChannelUriStringBuilder) ResponseEndpoint(responseEndpoint string) *ChannelUriStringBuilder {
	b.responseEndpoint = &responseEndpoint
	return b
}

// GetResponseEndpoint returns response-endpoint.
func (b *ChannelUriStringBuilder) GetResponseEndpoint() string { return strValue(b.responseEndpoint) }

// ResponseCorrelationID sets response-correlation-id to a correlation id (>= -1), typically the registration id of
// the subscription the response publication should respond to.
func (b *ChannelUriStringBuilder) ResponseCorrelationID(correlationId int64) *ChannelUriStringBuilder {
	return b.ResponseCorrelationIDString(strconv.FormatInt(correlationId, 10))
}

// ResponseCorrelationIDString sets response-correlation-id from a string: a number >= -1 or "prototype".
func (b *ChannelUriStringBuilder) ResponseCorrelationIDString(correlationId string) *ChannelUriStringBuilder {
	if correlationId != PrototypeCorrelationId {
		v, err := strconv.ParseInt(correlationId, 10, 64)
		if err != nil || v < -1 {
			return b.failf("responseCorrelationId must be a number greater than or equal to -1, "+
				"or the value '%s' found: %s", PrototypeCorrelationId, correlationId)
		}
	}
	b.responseCorrelationId = &correlationId
	return b
}

// GetResponseCorrelationId returns response-correlation-id.
func (b *ChannelUriStringBuilder) GetResponseCorrelationID() string {
	return strValue(b.responseCorrelationId)
}

// NakDelay sets nak-delay.
func (b *ChannelUriStringBuilder) NakDelay(nakDelay time.Duration) *ChannelUriStringBuilder {
	return b.setNonNegativeNs(&b.nakDelayNs, NakDelayParamName, int64(nakDelay))
}

// GetNakDelay returns nak-delay and whether it is set.
func (b *ChannelUriStringBuilder) GetNakDelay() (time.Duration, bool) {
	return getDuration(b.nakDelayNs)
}

// UntetheredWindowLimitTimeout sets untethered-window-limit-timeout.
func (b *ChannelUriStringBuilder) UntetheredWindowLimitTimeout(timeout time.Duration) *ChannelUriStringBuilder {
	return b.setNonNegativeNs(&b.untetheredWindowLimitTimeout, UntetheredWindowLimitTimeoutParamName, int64(timeout))
}

// GetUntetheredWindowLimitTimeout returns untethered-window-limit-timeout and whether it is set.
func (b *ChannelUriStringBuilder) GetUntetheredWindowLimitTimeout() (time.Duration, bool) {
	return getDuration(b.untetheredWindowLimitTimeout)
}

// UntetheredLingerTimeout sets untethered-linger-timeout.
func (b *ChannelUriStringBuilder) UntetheredLingerTimeout(timeout time.Duration) *ChannelUriStringBuilder {
	return b.setNonNegativeNs(&b.untetheredLingerTimeout, UntetheredLingerTimeoutParamName, int64(timeout))
}

// GetUntetheredLingerTimeout returns untethered-linger-timeout and whether it is set.
func (b *ChannelUriStringBuilder) GetUntetheredLingerTimeout() (time.Duration, bool) {
	return getDuration(b.untetheredLingerTimeout)
}

// UntetheredRestingTimeout sets untethered-resting-timeout.
func (b *ChannelUriStringBuilder) UntetheredRestingTimeout(timeout time.Duration) *ChannelUriStringBuilder {
	return b.setNonNegativeNs(&b.untetheredRestingTimeout, UntetheredRestingTimeoutParamName, int64(timeout))
}

// GetUntetheredRestingTimeout returns untethered-resting-timeout and whether it is set.
func (b *ChannelUriStringBuilder) GetUntetheredRestingTimeout() (time.Duration, bool) {
	return getDuration(b.untetheredRestingTimeout)
}

// MaxResend sets max-resend.
func (b *ChannelUriStringBuilder) MaxResend(maxResend int32) *ChannelUriStringBuilder {
	return b.setNonNegative32(&b.maxResend, MaxResendParamName, maxResend)
}

// GetMaxResend returns max-resend and whether it is set.
func (b *ChannelUriStringBuilder) GetMaxResend() (int32, bool) { return getOpt(b.maxResend) }

// StreamID sets stream-id (used e.g. by archive replay/recording channels).
func (b *ChannelUriStringBuilder) StreamID(streamId int32) *ChannelUriStringBuilder {
	b.streamId = cubPtr(streamId)
	return b
}

// GetStreamID returns stream-id and whether it is set.
func (b *ChannelUriStringBuilder) GetStreamID() (int32, bool) { return getOpt(b.streamId) }

// PublicationWindowLength sets pub-wnd.
func (b *ChannelUriStringBuilder) PublicationWindowLength(length int32) *ChannelUriStringBuilder {
	return b.setNonNegative32(&b.publicationWindowLength, PublicationWindowLengthParamName, length)
}

// GetPublicationWindowLength returns pub-wnd and whether it is set.
func (b *ChannelUriStringBuilder) GetPublicationWindowLength() (int32, bool) {
	return getOpt(b.publicationWindowLength)
}

// CopyFrom copies the given params from channelUri using SetParam, unsetting any that are absent from channelUri.
func (b *ChannelUriStringBuilder) CopyFrom(channelUri ChannelUri, paramNames ...string) *ChannelUriStringBuilder {
	for _, name := range paramNames {
		if value, ok := channelUri.Lookup(name); ok {
			b.SetParam(name, value)
		} else {
			b.Unset(name)
		}
	}
	return b
}

// SetParam sets a param from its string form, with the same parsing and validation as the Java builder applies when
// copying from a ChannelUri (sizes accept k/m/g suffixes, durations accept s/ms/us/ns suffixes, session-id accepts
// "tag:<n>"). Unknown param names are an error.
func (b *ChannelUriStringBuilder) SetParam(name string, value string) *ChannelUriStringBuilder {
	switch name {
	case EndpointParamName:
		return b.Endpoint(value)
	case InterfaceParamName:
		return b.NetworkInterface(value)
	case MdcControlParamName:
		return b.ControlEndpoint(value)
	case MdcControlModeParamName:
		return b.ControlMode(value)
	case TagsParamName:
		return b.Tags(value)
	case AliasParamName:
		return b.Alias(value)
	case CongestionControlParamName:
		return b.CongestionControl(value)
	case FlowControlParamName:
		return b.FlowControl(value)
	case MediaRcvTimestampOffsetParamName:
		return b.MediaReceiveTimestampOffset(value)
	case ChannelRcvTimestampOffsetParamName:
		return b.ChannelReceiveTimestampOffset(value)
	case ChannelSndTimestampOffsetParamName:
		return b.ChannelSendTimestampOffset(value)
	case ResponseEndpointParamName:
		return b.ResponseEndpoint(value)
	case ResponseCorrelationIdParamName:
		return b.ResponseCorrelationIDString(value)
	case ReliableStreamParamName:
		return b.Reliable(parseBool(value))
	case SparseParamName:
		return b.Sparse(parseBool(value))
	case EosParamName:
		return b.Eos(parseBool(value))
	case TetherParamName:
		return b.Tether(parseBool(value))
	case GroupParamName:
		return b.Group(parseBool(value))
	case RejoinParamName:
		return b.Rejoin(parseBool(value))
	case SpiesSimulateConnectionParamName:
		return b.SpiesSimulateConnection(parseBool(value))
	case TtlParamName:
		v, err := strconv.ParseInt(value, 10, 32)
		if err != nil {
			return b.failf("'ttl' must be a value integer: %w", err)
		}
		return b.Ttl(int32(v))
	case MtuLengthParamName:
		v, err := uriParseSize(name, value)
		if err != nil {
			return b.fail(err)
		}
		if v > math.MaxInt32 {
			return b.failf("%s %d > %d", name, v, math.MaxInt32)
		}
		return b.Mtu(int32(v))
	case TermLengthParamName:
		v, err := uriParseSize(name, value)
		if err != nil {
			return b.fail(err)
		}
		if v > math.MaxInt32 {
			return b.failf("term length more than max length of %d: value=%d", uriTermMaxLength, v)
		}
		return b.TermLength(int32(v))
	case InitialTermIdParamName, TermIdParamName, TermOffsetParamName, StreamIdParamName, MaxResendParamName,
		SocketTosParamName:
		v, err := strconv.ParseInt(value, 10, 32)
		if err != nil {
			return b.failf("'%s' must be a valid integer: %w", name, err)
		}
		switch name {
		case InitialTermIdParamName:
			return b.InitialTermID(int32(v))
		case TermIdParamName:
			return b.TermID(int32(v))
		case TermOffsetParamName:
			return b.TermOffset(int32(v))
		case StreamIdParamName:
			return b.StreamID(int32(v))
		case MaxResendParamName:
			return b.MaxResend(int32(v))
		default:
			return b.SocketTos(int32(v))
		}
	case SessionIdParamName:
		if IsTagged(value) {
			tag, err := GetTag(value)
			if err != nil {
				return b.failf("'session-id' tag must be a valid long: %w", err)
			}
			return b.TaggedSessionID(tag)
		}
		v, err := strconv.ParseInt(value, 10, 32)
		if err != nil {
			return b.failf("'session-id' must be a valid integer: %w", err)
		}
		return b.SessionID(int32(v))
	case GroupTagParamName:
		v, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return b.failf("'gtag' must be a valid long value: %w", err)
		}
		return b.GroupTag(v)
	case SocketSndbufParamName, SocketRcvbufParamName, ReceiverWindowLengthParamName,
		PublicationWindowLengthParamName:
		v, err := uriParseSize(name, value)
		if err != nil {
			return b.fail(err)
		}
		if v > math.MaxInt32 {
			return b.failf("value exceeds maximum permitted: value=%d", v)
		}
		switch name {
		case SocketSndbufParamName:
			return b.SocketSndbufLength(int32(v))
		case SocketRcvbufParamName:
			return b.SocketRcvbufLength(int32(v))
		case ReceiverWindowLengthParamName:
			return b.ReceiverWindowLength(int32(v))
		default:
			return b.PublicationWindowLength(int32(v))
		}
	case LingerParamName, NakDelayParamName, UntetheredWindowLimitTimeoutParamName,
		UntetheredLingerTimeoutParamName, UntetheredRestingTimeoutParamName:
		v, err := uriParseDuration(name, value)
		if err != nil {
			return b.fail(err)
		}
		return b.setNonNegativeNs(b.durationField(name), name, v)
	default:
		return b.failf("unknown channel parameter: %s", name)
	}
}

// Unset clears a single param by name ("prefix" and "media" are also accepted).
func (b *ChannelUriStringBuilder) Unset(name string) *ChannelUriStringBuilder {
	switch name {
	case "prefix":
		b.prefix = ""
	case "media":
		b.media = ""
	case EndpointParamName:
		b.endpoint = nil
	case InterfaceParamName:
		b.networkInterface = nil
	case MdcControlParamName:
		b.controlEndpoint = nil
	case MdcControlModeParamName:
		b.controlMode = nil
	case TagsParamName:
		b.tags = nil
	case AliasParamName:
		b.alias = nil
	case CongestionControlParamName:
		b.cc = nil
	case FlowControlParamName:
		b.fc = nil
	case MediaRcvTimestampOffsetParamName:
		b.mediaRcvTsOffset = nil
	case ChannelRcvTimestampOffsetParamName:
		b.channelRcvTsOffset = nil
	case ChannelSndTimestampOffsetParamName:
		b.channelSndTsOffset = nil
	case ResponseEndpointParamName:
		b.responseEndpoint = nil
	case ResponseCorrelationIdParamName:
		b.responseCorrelationId = nil
	case ReliableStreamParamName:
		b.reliable = nil
	case SparseParamName:
		b.sparse = nil
	case EosParamName:
		b.eos = nil
	case TetherParamName:
		b.tether = nil
	case GroupParamName:
		b.group = nil
	case RejoinParamName:
		b.rejoin = nil
	case SpiesSimulateConnectionParamName:
		b.ssc = nil
	case TtlParamName:
		b.ttl = nil
	case MtuLengthParamName:
		b.mtu = nil
	case TermLengthParamName:
		b.termLength = nil
	case InitialTermIdParamName:
		b.initialTermId = nil
	case TermIdParamName:
		b.termId = nil
	case TermOffsetParamName:
		b.termOffset = nil
	case SocketSndbufParamName:
		b.socketSndbufLength = nil
	case SocketRcvbufParamName:
		b.socketRcvbufLength = nil
	case SocketTosParamName:
		b.socketTos = nil
	case ReceiverWindowLengthParamName:
		b.receiverWindowLength = nil
	case MaxResendParamName:
		b.maxResend = nil
	case StreamIdParamName:
		b.streamId = nil
	case PublicationWindowLengthParamName:
		b.publicationWindowLength = nil
	case SessionIdParamName:
		b.sessionId = nil
		b.isSessionIdTagged = false
	case GroupTagParamName:
		b.groupTag = nil
	case LingerParamName, NakDelayParamName, UntetheredWindowLimitTimeoutParamName,
		UntetheredLingerTimeoutParamName, UntetheredRestingTimeoutParamName:
		*b.durationField(name) = nil
	default:
		return b.failf("unknown channel parameter: %s", name)
	}
	return b
}

func (b *ChannelUriStringBuilder) durationField(name string) **int64 {
	switch name {
	case LingerParamName:
		return &b.lingerNs
	case NakDelayParamName:
		return &b.nakDelayNs
	case UntetheredWindowLimitTimeoutParamName:
		return &b.untetheredWindowLimitTimeout
	case UntetheredLingerTimeoutParamName:
		return &b.untetheredLingerTimeout
	default:
		return &b.untetheredRestingTimeout
	}
}

// Build validates the parameters and renders the channel URI string.
func (b *ChannelUriStringBuilder) Build() (string, error) {
	if b.err != nil {
		return "", b.err
	}
	return b.render(), nil
}

// MustBuild is like Build but panics on error.
func (b *ChannelUriStringBuilder) MustBuild() string {
	s, err := b.Build()
	if err != nil {
		panic(err)
	}
	return s
}

// String renders the channel URI string, ignoring any recorded error (see Build).
func (b *ChannelUriStringBuilder) String() string {
	return b.render()
}

func (b *ChannelUriStringBuilder) render() string {
	var sb strings.Builder
	sb.Grow(64)
	if b.prefix != "" {
		sb.WriteString(b.prefix)
		sb.WriteByte(':')
	}
	sb.WriteString(AeronPrefix)
	sb.WriteString(b.media)
	sb.WriteByte('?')

	appendOptStr(&sb, TagsParamName, b.tags)
	appendOptStr(&sb, EndpointParamName, b.endpoint)
	appendOptStr(&sb, InterfaceParamName, b.networkInterface)
	appendOptStr(&sb, MdcControlParamName, b.controlEndpoint)
	appendOptStr(&sb, MdcControlModeParamName, b.controlMode)
	appendSize(&sb, MtuLengthParamName, b.mtu)
	appendSize(&sb, TermLengthParamName, b.termLength)
	appendInt(&sb, InitialTermIdParamName, b.initialTermId)
	appendInt(&sb, TermIdParamName, b.termId)
	appendInt(&sb, TermOffsetParamName, b.termOffset)
	if b.sessionId != nil {
		v := strconv.FormatInt(*b.sessionId, 10)
		if b.isSessionIdTagged {
			v = TagPrefix + v
		}
		appendStr(&sb, SessionIdParamName, v)
	}
	appendInt(&sb, TtlParamName, b.ttl)
	appendBool(&sb, ReliableStreamParamName, b.reliable)
	appendDuration(&sb, LingerParamName, b.lingerNs)
	appendOptStr(&sb, AliasParamName, b.alias)
	appendOptStr(&sb, CongestionControlParamName, b.cc)
	appendOptStr(&sb, FlowControlParamName, b.fc)
	appendInt(&sb, GroupTagParamName, b.groupTag)
	appendBool(&sb, SparseParamName, b.sparse)
	appendBool(&sb, EosParamName, b.eos)
	appendBool(&sb, TetherParamName, b.tether)
	appendBool(&sb, GroupParamName, b.group)
	appendBool(&sb, RejoinParamName, b.rejoin)
	appendBool(&sb, SpiesSimulateConnectionParamName, b.ssc)
	appendSize(&sb, SocketSndbufParamName, b.socketSndbufLength)
	appendSize(&sb, SocketRcvbufParamName, b.socketRcvbufLength)
	appendInt(&sb, SocketTosParamName, b.socketTos)
	appendSize(&sb, ReceiverWindowLengthParamName, b.receiverWindowLength)
	appendOptStr(&sb, MediaRcvTimestampOffsetParamName, b.mediaRcvTsOffset)
	appendOptStr(&sb, ChannelRcvTimestampOffsetParamName, b.channelRcvTsOffset)
	appendOptStr(&sb, ChannelSndTimestampOffsetParamName, b.channelSndTsOffset)
	appendOptStr(&sb, ResponseEndpointParamName, b.responseEndpoint)
	appendOptStr(&sb, ResponseCorrelationIdParamName, b.responseCorrelationId)
	appendDuration(&sb, NakDelayParamName, b.nakDelayNs)
	appendDuration(&sb, UntetheredWindowLimitTimeoutParamName, b.untetheredWindowLimitTimeout)
	appendDuration(&sb, UntetheredLingerTimeoutParamName, b.untetheredLingerTimeout)
	appendDuration(&sb, UntetheredRestingTimeoutParamName, b.untetheredRestingTimeout)
	appendInt(&sb, MaxResendParamName, b.maxResend)
	appendInt(&sb, StreamIdParamName, b.streamId)
	appendSize(&sb, PublicationWindowLengthParamName, b.publicationWindowLength)

	s := sb.String()
	if last := s[len(s)-1]; last == '|' || last == '?' {
		s = s[:len(s)-1]
	}
	return s
}

func appendStr(sb *strings.Builder, name string, value string) {
	sb.WriteString(name)
	sb.WriteByte('=')
	sb.WriteString(value)
	sb.WriteByte('|')
}

func appendOptStr(sb *strings.Builder, name string, value *string) {
	if value != nil {
		appendStr(sb, name, *value)
	}
}

func strValue(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}

func appendInt[T int32 | int64](sb *strings.Builder, name string, value *T) {
	if value != nil {
		appendStr(sb, name, strconv.FormatInt(int64(*value), 10))
	}
}

func appendBool(sb *strings.Builder, name string, value *bool) {
	if value != nil {
		appendStr(sb, name, strconv.FormatBool(*value))
	}
}

func appendSize(sb *strings.Builder, name string, value *int32) {
	if value != nil {
		appendStr(sb, name, uriFormatSize(int64(*value)))
	}
}

func appendDuration(sb *strings.Builder, name string, valueNs *int64) {
	if valueNs != nil {
		appendStr(sb, name, uriFormatDuration(*valueNs))
	}
}

func (b *ChannelUriStringBuilder) setNonNegative32(field **int32, name string, value int32) *ChannelUriStringBuilder {
	if value < 0 {
		return b.failf("`%s` value cannot be negative: %d", name, value)
	}
	*field = cubPtr(value)
	return b
}

func (b *ChannelUriStringBuilder) setNonNegativeNs(field **int64, name string, value int64) *ChannelUriStringBuilder {
	if value < 0 {
		return b.failf("`%s` value cannot be negative: %d", name, value)
	}
	*field = cubPtr(value)
	return b
}

func getOpt[T any](v *T) (T, bool) {
	if v == nil {
		var zero T
		return zero, false
	}
	return *v, true
}

func getDuration(v *int64) (time.Duration, bool) {
	if v == nil {
		return 0, false
	}
	return time.Duration(*v), true
}

// parseBool mirrors Java's Boolean.valueOf: "true" (case-insensitive) is true, anything else false.
func parseBool(value string) bool {
	return strings.EqualFold(value, "true")
}
