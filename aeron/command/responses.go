/*
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

package command

import (
	"fmt"
	"net"

	"github.com/lirm/aeron-go/aeron/atomic"
)

// Flyweights over the media driver -> client responses (io.aeron.command.* in Java 1.53.3).

// ErrorResponse is ON_ERROR (ErrorResponseFlyweight).
//
//	+---------------------------------------------------------------+
//	|              Offending Command Correlation ID                 |
//	|                                                               |
//	+---------------------------------------------------------------+
//	|                         Error Code                            |
//	+---------------------------------------------------------------+
//	|                   Error Message Length                        |
//	+---------------------------------------------------------------+
//	|                       Error Message                          ...
//	+---------------------------------------------------------------+
type ErrorResponse struct {
	flyweightBase
}

const (
	errorResponseOffendingCorrelationIDOffset int32 = 0
	errorResponseErrorCodeOffset              int32 = errorResponseOffendingCorrelationIDOffset + sizeOfInt64
	errorResponseErrorMessageOffset           int32 = errorResponseErrorCodeOffset + sizeOfInt32
)

// Wrap the buffer at the given offset.
func (m *ErrorResponse) Wrap(buffer *atomic.Buffer, offset int32) *ErrorResponse {
	m.wrap(buffer, offset)
	return m
}

// OffendingCommandCorrelationID is the correlation id of the command that failed.
func (m *ErrorResponse) OffendingCommandCorrelationID() int64 {
	return m.getInt64(errorResponseOffendingCorrelationIDOffset)
}

// SetOffendingCommandCorrelationID sets the offending command correlation id.
func (m *ErrorResponse) SetOffendingCommandCorrelationID(v int64) {
	m.putInt64(errorResponseOffendingCorrelationIDOffset, v)
}

// ErrorCode value (one of the ErrorCode* constants).
func (m *ErrorResponse) ErrorCode() int32 { return m.getInt32(errorResponseErrorCodeOffset) }

// SetErrorCode sets the error code.
func (m *ErrorResponse) SetErrorCode(v int32) { m.putInt32(errorResponseErrorCodeOffset, v) }

// ErrorMessage describing the error.
func (m *ErrorResponse) ErrorMessage() string { return m.getString(errorResponseErrorMessageOffset) }

// SetErrorMessage sets the error message.
func (m *ErrorResponse) SetErrorMessage(v string) { m.putString(errorResponseErrorMessageOffset, v) }

// Length of the encoded message.
func (m *ErrorResponse) Length() int32 {
	return errorResponseErrorMessageOffset + sizeOfInt32 + m.getInt32(errorResponseErrorMessageOffset)
}

// PublicationBuffersReady is ON_PUBLICATION_READY / ON_EXCLUSIVE_PUBLICATION_READY (PublicationBuffersReadyFlyweight).
//
//	+---------------------------------------------------------------+
//	|                         Correlation ID                        |
//	|                                                               |
//	+---------------------------------------------------------------+
//	|                        Registration ID                        |
//	|                                                               |
//	+---------------------------------------------------------------+
//	|                          Session ID                           |
//	+---------------------------------------------------------------+
//	|                           Stream ID                           |
//	+---------------------------------------------------------------+
//	|                  Publication Limit Counter ID                 |
//	+---------------------------------------------------------------+
//	|                  Channel Status Indicator ID                  |
//	+---------------------------------------------------------------+
//	|                         Log File Length                       |
//	+---------------------------------------------------------------+
//	|                          Log File Name                       ...
//	+---------------------------------------------------------------+
type PublicationBuffersReady struct {
	flyweightBase
}

const (
	pubReadyCorrelationIDOffset            int32 = 0
	pubReadyRegistrationIDOffset           int32 = pubReadyCorrelationIDOffset + sizeOfInt64
	pubReadySessionIDOffset                int32 = pubReadyRegistrationIDOffset + sizeOfInt64
	pubReadyStreamIDOffset                 int32 = pubReadySessionIDOffset + sizeOfInt32
	pubReadyPublicationLimitCounterIDOffet int32 = pubReadyStreamIDOffset + sizeOfInt32
	pubReadyChannelStatusIndicatorIDOffset int32 = pubReadyPublicationLimitCounterIDOffet + sizeOfInt32
	pubReadyLogFileNameOffset              int32 = pubReadyChannelStatusIndicatorIDOffset + sizeOfInt32
)

// Wrap the buffer at the given offset.
func (m *PublicationBuffersReady) Wrap(buffer *atomic.Buffer, offset int32) *PublicationBuffersReady {
	m.wrap(buffer, offset)
	return m
}

// CorrelationID of the ADD_PUBLICATION command.
func (m *PublicationBuffersReady) CorrelationID() int64 {
	return m.getInt64(pubReadyCorrelationIDOffset)
}

// SetCorrelationID sets the correlation id.
func (m *PublicationBuffersReady) SetCorrelationID(v int64) {
	m.putInt64(pubReadyCorrelationIDOffset, v)
}

// RegistrationID of the (possibly shared) publication.
func (m *PublicationBuffersReady) RegistrationID() int64 {
	return m.getInt64(pubReadyRegistrationIDOffset)
}

// SetRegistrationID sets the registration id.
func (m *PublicationBuffersReady) SetRegistrationID(v int64) {
	m.putInt64(pubReadyRegistrationIDOffset, v)
}

// SessionID of the publication.
func (m *PublicationBuffersReady) SessionID() int32 { return m.getInt32(pubReadySessionIDOffset) }

// SetSessionID sets the session id.
func (m *PublicationBuffersReady) SetSessionID(v int32) { m.putInt32(pubReadySessionIDOffset, v) }

// StreamID of the publication.
func (m *PublicationBuffersReady) StreamID() int32 { return m.getInt32(pubReadyStreamIDOffset) }

// SetStreamID sets the stream id.
func (m *PublicationBuffersReady) SetStreamID(v int32) { m.putInt32(pubReadyStreamIDOffset, v) }

// PublicationLimitCounterID is the counter id of the publication limit.
func (m *PublicationBuffersReady) PublicationLimitCounterID() int32 {
	return m.getInt32(pubReadyPublicationLimitCounterIDOffet)
}

// SetPublicationLimitCounterID sets the publication limit counter id.
func (m *PublicationBuffersReady) SetPublicationLimitCounterID(v int32) {
	m.putInt32(pubReadyPublicationLimitCounterIDOffet, v)
}

// ChannelStatusIndicatorID is the counter id of the channel status indicator.
func (m *PublicationBuffersReady) ChannelStatusIndicatorID() int32 {
	return m.getInt32(pubReadyChannelStatusIndicatorIDOffset)
}

// SetChannelStatusIndicatorID sets the channel status indicator id.
func (m *PublicationBuffersReady) SetChannelStatusIndicatorID(v int32) {
	m.putInt32(pubReadyChannelStatusIndicatorIDOffset, v)
}

// LogFileName of the publication's log buffers.
func (m *PublicationBuffersReady) LogFileName() string { return m.getString(pubReadyLogFileNameOffset) }

// SetLogFileName sets the log file name.
func (m *PublicationBuffersReady) SetLogFileName(v string) { m.putString(pubReadyLogFileNameOffset, v) }

// Length of the encoded message.
func (m *PublicationBuffersReady) Length() int32 {
	return pubReadyLogFileNameOffset + sizeOfInt32 + m.getInt32(pubReadyLogFileNameOffset)
}

// ImageBuffersReady is ON_AVAILABLE_IMAGE (ImageBuffersReadyFlyweight).
//
//	+---------------------------------------------------------------+
//	|                       Correlation ID                          |
//	|                                                               |
//	+---------------------------------------------------------------+
//	|                         Session ID                            |
//	+---------------------------------------------------------------+
//	|                         Stream ID                             |
//	+---------------------------------------------------------------+
//	|                Subscription Registration ID                   |
//	|                                                               |
//	+---------------------------------------------------------------+
//	|                    Subscriber Position ID                     |
//	+---------------------------------------------------------------+
//	|                       Log File Length                         |
//	+---------------------------------------------------------------+
//	|                        Log File Name                         ...
//	+---------------------------------------------------------------+
//	|                   (padding to 4-byte alignment)               |
//	+---------------------------------------------------------------+
//	|                    Source Identity Length                     |
//	+---------------------------------------------------------------+
//	|                    Source Identity Name                      ...
//	+---------------------------------------------------------------+
//
// Set the log file name before the source identity: the latter's offset depends on the former's length.
type ImageBuffersReady struct {
	flyweightBase
}

const (
	imageReadyCorrelationIDOffset              int32 = 0
	imageReadySessionIDOffset                  int32 = imageReadyCorrelationIDOffset + sizeOfInt64
	imageReadyStreamIDOffset                   int32 = imageReadySessionIDOffset + sizeOfInt32
	imageReadySubscriptionRegistrationIDOffset int32 = imageReadyStreamIDOffset + sizeOfInt32
	imageReadySubscriberPositionIDOffset       int32 = imageReadySubscriptionRegistrationIDOffset + sizeOfInt64
	imageReadyLogFileNameOffset                int32 = imageReadySubscriberPositionIDOffset + sizeOfInt32
)

// Wrap the buffer at the given offset.
func (m *ImageBuffersReady) Wrap(buffer *atomic.Buffer, offset int32) *ImageBuffersReady {
	m.wrap(buffer, offset)
	return m
}

// CorrelationID of the image.
func (m *ImageBuffersReady) CorrelationID() int64 { return m.getInt64(imageReadyCorrelationIDOffset) }

// SetCorrelationID sets the correlation id.
func (m *ImageBuffersReady) SetCorrelationID(v int64) { m.putInt64(imageReadyCorrelationIDOffset, v) }

// SessionID of the image.
func (m *ImageBuffersReady) SessionID() int32 { return m.getInt32(imageReadySessionIDOffset) }

// SetSessionID sets the session id.
func (m *ImageBuffersReady) SetSessionID(v int32) { m.putInt32(imageReadySessionIDOffset, v) }

// StreamID of the image.
func (m *ImageBuffersReady) StreamID() int32 { return m.getInt32(imageReadyStreamIDOffset) }

// SetStreamID sets the stream id.
func (m *ImageBuffersReady) SetStreamID(v int32) { m.putInt32(imageReadyStreamIDOffset, v) }

// SubscriptionRegistrationID of the subscription the image is for.
func (m *ImageBuffersReady) SubscriptionRegistrationID() int64 {
	return m.getInt64(imageReadySubscriptionRegistrationIDOffset)
}

// SetSubscriptionRegistrationID sets the subscription registration id.
func (m *ImageBuffersReady) SetSubscriptionRegistrationID(v int64) {
	m.putInt64(imageReadySubscriptionRegistrationIDOffset, v)
}

// SubscriberPositionID is the counter id of the subscriber position.
func (m *ImageBuffersReady) SubscriberPositionID() int32 {
	return m.getInt32(imageReadySubscriberPositionIDOffset)
}

// SetSubscriberPositionID sets the subscriber position counter id.
func (m *ImageBuffersReady) SetSubscriberPositionID(v int32) {
	m.putInt32(imageReadySubscriberPositionIDOffset, v)
}

// LogFileName of the image's log buffers.
func (m *ImageBuffersReady) LogFileName() string { return m.getString(imageReadyLogFileNameOffset) }

// SetLogFileName sets the log file name.
func (m *ImageBuffersReady) SetLogFileName(v string) { m.putString(imageReadyLogFileNameOffset, v) }

func (m *ImageBuffersReady) sourceIdentityOffset() int32 {
	return imageReadyLogFileNameOffset + sizeOfInt32 + align4(m.getInt32(imageReadyLogFileNameOffset))
}

// SourceIdentity of the image's source (e.g. its address).
func (m *ImageBuffersReady) SourceIdentity() string { return m.getString(m.sourceIdentityOffset()) }

// SetSourceIdentity sets the source identity.
func (m *ImageBuffersReady) SetSourceIdentity(v string) { m.putString(m.sourceIdentityOffset(), v) }

// Length of the encoded message.
func (m *ImageBuffersReady) Length() int32 {
	off := m.sourceIdentityOffset()
	return off + sizeOfInt32 + m.getInt32(off)
}

// SubscriptionReady is ON_SUBSCRIPTION_READY (SubscriptionReadyFlyweight).
//
//	+---------------------------------------------------------------+
//	|                         Correlation ID                        |
//	|                                                               |
//	+---------------------------------------------------------------+
//	|                  Channel Status Indicator ID                  |
//	+---------------------------------------------------------------+
type SubscriptionReady struct {
	flyweightBase
}

const (
	subReadyCorrelationIDOffset            int32 = 0
	subReadyChannelStatusIndicatorIDOffset int32 = subReadyCorrelationIDOffset + sizeOfInt64
	// SubscriptionReadyLength is the length of an ON_SUBSCRIPTION_READY message.
	SubscriptionReadyLength int32 = subReadyChannelStatusIndicatorIDOffset + sizeOfInt32
)

// Wrap the buffer at the given offset.
func (m *SubscriptionReady) Wrap(buffer *atomic.Buffer, offset int32) *SubscriptionReady {
	m.wrap(buffer, offset)
	return m
}

// CorrelationID of the ADD_SUBSCRIPTION command.
func (m *SubscriptionReady) CorrelationID() int64 { return m.getInt64(subReadyCorrelationIDOffset) }

// SetCorrelationID sets the correlation id.
func (m *SubscriptionReady) SetCorrelationID(v int64) { m.putInt64(subReadyCorrelationIDOffset, v) }

// ChannelStatusIndicatorID is the counter id of the channel status indicator.
func (m *SubscriptionReady) ChannelStatusIndicatorID() int32 {
	return m.getInt32(subReadyChannelStatusIndicatorIDOffset)
}

// SetChannelStatusIndicatorID sets the channel status indicator id.
func (m *SubscriptionReady) SetChannelStatusIndicatorID(v int32) {
	m.putInt32(subReadyChannelStatusIndicatorIDOffset, v)
}

// Length of the encoded message.
func (m *SubscriptionReady) Length() int32 { return SubscriptionReadyLength }

// OperationSucceeded is ON_OPERATION_SUCCESS (OperationSucceededFlyweight).
//
//	+---------------------------------------------------------------+
//	|                         Correlation ID                        |
//	|                                                               |
//	+---------------------------------------------------------------+
type OperationSucceeded struct {
	flyweightBase
}

// OperationSucceededLength is the length of an ON_OPERATION_SUCCESS message.
const OperationSucceededLength int32 = sizeOfInt64

// Wrap the buffer at the given offset.
func (m *OperationSucceeded) Wrap(buffer *atomic.Buffer, offset int32) *OperationSucceeded {
	m.wrap(buffer, offset)
	return m
}

// CorrelationID of the command that succeeded.
func (m *OperationSucceeded) CorrelationID() int64 { return m.getInt64(0) }

// SetCorrelationID sets the correlation id.
func (m *OperationSucceeded) SetCorrelationID(v int64) { m.putInt64(0, v) }

// Length of the encoded message.
func (m *OperationSucceeded) Length() int32 { return OperationSucceededLength }

// ImageMessage is ON_UNAVAILABLE_IMAGE (ImageMessageFlyweight).
//
//	+---------------------------------------------------------------+
//	|                        Correlation ID                         |
//	|                                                               |
//	+---------------------------------------------------------------+
//	|                 Subscription Registration ID                  |
//	|                                                               |
//	+---------------------------------------------------------------+
//	|                          Stream ID                            |
//	+---------------------------------------------------------------+
//	|                        Channel Length                         |
//	+---------------------------------------------------------------+
//	|                        Channel (ASCII)                       ...
//	+---------------------------------------------------------------+
type ImageMessage struct {
	flyweightBase
}

const (
	imageMessageCorrelationIDOffset              int32 = 0
	imageMessageSubscriptionRegistrationIDOffset int32 = imageMessageCorrelationIDOffset + sizeOfInt64
	imageMessageStreamIDOffset                   int32 = imageMessageSubscriptionRegistrationIDOffset + sizeOfInt64
	imageMessageChannelOffset                    int32 = imageMessageStreamIDOffset + sizeOfInt32
)

// Wrap the buffer at the given offset.
func (m *ImageMessage) Wrap(buffer *atomic.Buffer, offset int32) *ImageMessage {
	m.wrap(buffer, offset)
	return m
}

// CorrelationID of the image.
func (m *ImageMessage) CorrelationID() int64 { return m.getInt64(imageMessageCorrelationIDOffset) }

// SetCorrelationID sets the correlation id.
func (m *ImageMessage) SetCorrelationID(v int64) { m.putInt64(imageMessageCorrelationIDOffset, v) }

// SubscriptionRegistrationID of the subscription the image belonged to.
func (m *ImageMessage) SubscriptionRegistrationID() int64 {
	return m.getInt64(imageMessageSubscriptionRegistrationIDOffset)
}

// SetSubscriptionRegistrationID sets the subscription registration id.
func (m *ImageMessage) SetSubscriptionRegistrationID(v int64) {
	m.putInt64(imageMessageSubscriptionRegistrationIDOffset, v)
}

// StreamID of the image.
func (m *ImageMessage) StreamID() int32 { return m.getInt32(imageMessageStreamIDOffset) }

// SetStreamID sets the stream id.
func (m *ImageMessage) SetStreamID(v int32) { m.putInt32(imageMessageStreamIDOffset, v) }

// Channel of the image.
func (m *ImageMessage) Channel() string { return m.getString(imageMessageChannelOffset) }

// SetChannel sets the channel.
func (m *ImageMessage) SetChannel(v string) { m.putString(imageMessageChannelOffset, v) }

// Length of the encoded message.
func (m *ImageMessage) Length() int32 {
	return imageMessageChannelOffset + sizeOfInt32 + m.getInt32(imageMessageChannelOffset)
}

// CounterUpdate is ON_COUNTER_READY / ON_UNAVAILABLE_COUNTER (CounterUpdateFlyweight).
//
//	+---------------------------------------------------------------+
//	|                         Correlation ID                        |
//	|                                                               |
//	+---------------------------------------------------------------+
//	|                           Counter ID                          |
//	+---------------------------------------------------------------+
type CounterUpdate struct {
	flyweightBase
}

// CounterUpdateLength is the length of an ON_COUNTER_READY / ON_UNAVAILABLE_COUNTER message.
const CounterUpdateLength int32 = sizeOfInt64 + sizeOfInt32

// Wrap the buffer at the given offset.
func (m *CounterUpdate) Wrap(buffer *atomic.Buffer, offset int32) *CounterUpdate {
	m.wrap(buffer, offset)
	return m
}

// CorrelationID (registration id) of the counter.
func (m *CounterUpdate) CorrelationID() int64 { return m.getInt64(0) }

// SetCorrelationID sets the correlation id.
func (m *CounterUpdate) SetCorrelationID(v int64) { m.putInt64(0, v) }

// CounterID of the counter.
func (m *CounterUpdate) CounterID() int32 { return m.getInt32(sizeOfInt64) }

// SetCounterID sets the counter id.
func (m *CounterUpdate) SetCounterID(v int32) { m.putInt32(sizeOfInt64, v) }

// Length of the encoded message.
func (m *CounterUpdate) Length() int32 { return CounterUpdateLength }

// StaticCounter is ON_STATIC_COUNTER (StaticCounterFlyweight).
//
//	+---------------------------------------------------------------+
//	|                         Correlation ID                        |
//	|                                                               |
//	+---------------------------------------------------------------+
//	|                           Counter ID                          |
//	+---------------------------------------------------------------+
type StaticCounter struct {
	flyweightBase
}

// StaticCounterLength is the length of an ON_STATIC_COUNTER message.
const StaticCounterLength int32 = sizeOfInt64 + sizeOfInt32

// Wrap the buffer at the given offset.
func (m *StaticCounter) Wrap(buffer *atomic.Buffer, offset int32) *StaticCounter {
	m.wrap(buffer, offset)
	return m
}

// CorrelationID of the ADD_STATIC_COUNTER command.
func (m *StaticCounter) CorrelationID() int64 { return m.getInt64(0) }

// SetCorrelationID sets the correlation id.
func (m *StaticCounter) SetCorrelationID(v int64) { m.putInt64(0, v) }

// CounterID of the static counter.
func (m *StaticCounter) CounterID() int32 { return m.getInt32(sizeOfInt64) }

// SetCounterID sets the counter id.
func (m *StaticCounter) SetCounterID(v int32) { m.putInt32(sizeOfInt64, v) }

// Length of the encoded message.
func (m *StaticCounter) Length() int32 { return StaticCounterLength }

// ClientTimeout is ON_CLIENT_TIMEOUT (ClientTimeoutFlyweight).
//
//	+---------------------------------------------------------------+
//	|                         Client ID                             |
//	|                                                               |
//	+---------------------------------------------------------------+
type ClientTimeout struct {
	flyweightBase
}

// ClientTimeoutLength is the length of an ON_CLIENT_TIMEOUT message.
const ClientTimeoutLength int32 = sizeOfInt64

// Wrap the buffer at the given offset.
func (m *ClientTimeout) Wrap(buffer *atomic.Buffer, offset int32) *ClientTimeout {
	m.wrap(buffer, offset)
	return m
}

// ClientID of the client that timed out.
func (m *ClientTimeout) ClientID() int64 { return m.getInt64(0) }

// SetClientID sets the client id.
func (m *ClientTimeout) SetClientID(v int64) { m.putInt64(0, v) }

// Length of the encoded message.
func (m *ClientTimeout) Length() int32 { return ClientTimeoutLength }

// NextAvailableSessionID is ON_NEXT_AVAILABLE_SESSION_ID (NextAvailableSessionIdFlyweight).
//
//	+---------------------------------------------------------------+
//	|                         Correlation ID                        |
//	|                                                               |
//	+---------------------------------------------------------------+
//	|                    Next Available Session ID                  |
//	+---------------------------------------------------------------+
type NextAvailableSessionID struct {
	flyweightBase
}

// NextAvailableSessionIDLength is the length of an ON_NEXT_AVAILABLE_SESSION_ID message.
const NextAvailableSessionIDLength int32 = sizeOfInt64 + sizeOfInt32

// Wrap the buffer at the given offset.
func (m *NextAvailableSessionID) Wrap(buffer *atomic.Buffer, offset int32) *NextAvailableSessionID {
	m.wrap(buffer, offset)
	return m
}

// CorrelationID of the GET_NEXT_AVAILABLE_SESSION_ID command.
func (m *NextAvailableSessionID) CorrelationID() int64 { return m.getInt64(0) }

// SetCorrelationID sets the correlation id.
func (m *NextAvailableSessionID) SetCorrelationID(v int64) { m.putInt64(0, v) }

// NextSessionID is the next available session id.
func (m *NextAvailableSessionID) NextSessionID() int32 { return m.getInt32(sizeOfInt64) }

// SetNextSessionID sets the next session id.
func (m *NextAvailableSessionID) SetNextSessionID(v int32) { m.putInt32(sizeOfInt64, v) }

// Length of the encoded message.
func (m *NextAvailableSessionID) Length() int32 { return NextAvailableSessionIDLength }

// PublicationErrorFrame is ON_PUBLICATION_ERROR (PublicationErrorFrameFlyweight): an error frame received by a
// publication from a receiver.
//
//	+---------------------------------------------------------------+
//	|                 Publication Registration ID                   |
//	|                                                               |
//	+---------------------------------------------------------------+
//	|                 Destination Registration ID                   |
//	|                                                               |
//	+---------------------------------------------------------------+
//	|                          Session ID                           |
//	+---------------------------------------------------------------+
//	|                           Stream ID                           |
//	+---------------------------------------------------------------+
//	|                          Receiver ID                          |
//	|                                                               |
//	+---------------------------------------------------------------+
//	|                           Group Tag                           |
//	|                                                               |
//	+-------------------------------+-------------------------------+
//	|         Address Type          |            UDP Port           |
//	+-------------------------------+-------------------------------+
//	|           IPv4 or IPv6 Address padded out to 16 bytes         |
//	|                                                               |
//	|                                                               |
//	|                                                               |
//	+---------------------------------------------------------------+
//	|                          Error Code                           |
//	+---------------------------------------------------------------+
//	|                      Error Message Length                     |
//	+---------------------------------------------------------------+
//	|                         Error Message                        ...
//	+---------------------------------------------------------------+
type PublicationErrorFrame struct {
	flyweightBase
}

const (
	pubErrRegistrationIDOffset            int32 = 0
	pubErrDestinationRegistrationIDOffset int32 = pubErrRegistrationIDOffset + sizeOfInt64
	pubErrSessionIDOffset                 int32 = pubErrDestinationRegistrationIDOffset + sizeOfInt64
	pubErrStreamIDOffset                  int32 = pubErrSessionIDOffset + sizeOfInt32
	pubErrReceiverIDOffset                int32 = pubErrStreamIDOffset + sizeOfInt32
	pubErrGroupTagOffset                  int32 = pubErrReceiverIDOffset + sizeOfInt64
	pubErrAddressTypeOffset               int32 = pubErrGroupTagOffset + sizeOfInt64
	pubErrAddressPortOffset               int32 = pubErrAddressTypeOffset + sizeOfInt16
	pubErrAddressOffset                   int32 = pubErrAddressPortOffset + sizeOfInt16
	pubErrErrorCodeOffset                 int32 = pubErrAddressOffset + ipv6AddressLength
	pubErrErrorMessageOffset              int32 = pubErrErrorCodeOffset + sizeOfInt32

	ipv4AddressLength int32 = 4
	ipv6AddressLength int32 = 16

	// AddressTypeIPv4 marks an IPv4 source address in a PublicationErrorFrame.
	AddressTypeIPv4 uint16 = 1
	// AddressTypeIPv6 marks an IPv6 source address in a PublicationErrorFrame.
	AddressTypeIPv6 uint16 = 2
)

// Wrap the buffer at the given offset.
func (m *PublicationErrorFrame) Wrap(buffer *atomic.Buffer, offset int32) *PublicationErrorFrame {
	m.wrap(buffer, offset)
	return m
}

// RegistrationID of the publication that received the error frame.
func (m *PublicationErrorFrame) RegistrationID() int64 { return m.getInt64(pubErrRegistrationIDOffset) }

// SetRegistrationID sets the registration id.
func (m *PublicationErrorFrame) SetRegistrationID(v int64) { m.putInt64(pubErrRegistrationIDOffset, v) }

// DestinationRegistrationID of the MDC destination the error came from (or -1).
func (m *PublicationErrorFrame) DestinationRegistrationID() int64 {
	return m.getInt64(pubErrDestinationRegistrationIDOffset)
}

// SetDestinationRegistrationID sets the destination registration id.
func (m *PublicationErrorFrame) SetDestinationRegistrationID(v int64) {
	m.putInt64(pubErrDestinationRegistrationIDOffset, v)
}

// SessionID of the publication.
func (m *PublicationErrorFrame) SessionID() int32 { return m.getInt32(pubErrSessionIDOffset) }

// SetSessionID sets the session id.
func (m *PublicationErrorFrame) SetSessionID(v int32) { m.putInt32(pubErrSessionIDOffset, v) }

// StreamID of the publication.
func (m *PublicationErrorFrame) StreamID() int32 { return m.getInt32(pubErrStreamIDOffset) }

// SetStreamID sets the stream id.
func (m *PublicationErrorFrame) SetStreamID(v int32) { m.putInt32(pubErrStreamIDOffset, v) }

// ReceiverID of the receiver that sent the error frame.
func (m *PublicationErrorFrame) ReceiverID() int64 { return m.getInt64(pubErrReceiverIDOffset) }

// SetReceiverID sets the receiver id.
func (m *PublicationErrorFrame) SetReceiverID(v int64) { m.putInt64(pubErrReceiverIDOffset, v) }

// GroupTag of the receiver (or the null value).
func (m *PublicationErrorFrame) GroupTag() int64 { return m.getInt64(pubErrGroupTagOffset) }

// SetGroupTag sets the group tag.
func (m *PublicationErrorFrame) SetGroupTag(v int64) { m.putInt64(pubErrGroupTagOffset, v) }

// SourceAddress of the receiver that sent the error frame.
func (m *PublicationErrorFrame) SourceAddress() (*net.UDPAddr, error) {
	addressType := m.buffer.GetUInt16(m.offset + pubErrAddressTypeOffset)
	port := int(m.buffer.GetUInt16(m.offset + pubErrAddressPortOffset))
	var length int32
	switch addressType {
	case AddressTypeIPv4:
		length = ipv4AddressLength
	case AddressTypeIPv6:
		length = ipv6AddressLength
	default:
		return nil, fmt.Errorf("unknown address type: %d", addressType)
	}
	ip := net.IP(m.buffer.GetBytesArray(m.offset+pubErrAddressOffset, length))
	return &net.UDPAddr{IP: ip, Port: port}, nil
}

// SetSourceAddress sets the source address; IPv4 (and IPv4-mapped IPv6) addresses are encoded as IPv4.
func (m *PublicationErrorFrame) SetSourceAddress(addr *net.UDPAddr) error {
	var ip []byte
	var addressType uint16
	if ip4 := addr.IP.To4(); ip4 != nil {
		ip, addressType = make([]byte, ipv6AddressLength), AddressTypeIPv4
		copy(ip, ip4)
	} else if ip16 := addr.IP.To16(); ip16 != nil {
		ip, addressType = []byte(ip16), AddressTypeIPv6
	} else {
		return fmt.Errorf("invalid IP address: %v", addr.IP)
	}
	m.buffer.PutUInt16(m.offset+pubErrAddressTypeOffset, addressType)
	m.buffer.PutUInt16(m.offset+pubErrAddressPortOffset, uint16(addr.Port))
	m.buffer.PutBytesArray(m.offset+pubErrAddressOffset, &ip, 0, ipv6AddressLength)
	return nil
}

// ErrorCode value from the error frame.
func (m *PublicationErrorFrame) ErrorCode() int32 { return m.getInt32(pubErrErrorCodeOffset) }

// SetErrorCode sets the error code.
func (m *PublicationErrorFrame) SetErrorCode(v int32) { m.putInt32(pubErrErrorCodeOffset, v) }

// ErrorMessage from the error frame.
func (m *PublicationErrorFrame) ErrorMessage() string { return m.getString(pubErrErrorMessageOffset) }

// SetErrorMessage sets the error message.
func (m *PublicationErrorFrame) SetErrorMessage(v string) { m.putString(pubErrErrorMessageOffset, v) }

// Length of the encoded message.
func (m *PublicationErrorFrame) Length() int32 {
	return pubErrErrorMessageOffset + sizeOfInt32 + m.getInt32(pubErrErrorMessageOffset)
}

// ValidateLength checks a received ON_PUBLICATION_ERROR of the given length holds the fixed fields and the message.
func (m *PublicationErrorFrame) ValidateLength(length int32) error {
	if length < pubErrErrorMessageOffset+sizeOfInt32 {
		return malformed("publication error frame too short: length=%d", length)
	}
	if msgLength := m.getInt32(pubErrErrorMessageOffset); msgLength < 0 ||
		msgLength > length-pubErrErrorMessageOffset-sizeOfInt32 {
		return malformed("invalid publication error frame message length=%d for length=%d", msgLength, length)
	}
	return nil
}

// Value copies the frame into a PublicationError that is safe to retain after the callback returns.
func (m *PublicationErrorFrame) Value() PublicationError {
	addr, _ := m.SourceAddress()
	return PublicationError{
		RegistrationID:            m.RegistrationID(),
		DestinationRegistrationID: m.DestinationRegistrationID(),
		SessionID:                 m.SessionID(),
		StreamID:                  m.StreamID(),
		ReceiverID:                m.ReceiverID(),
		GroupTag:                  m.GroupTag(),
		SourceAddress:             addr,
		ErrorCode:                 m.ErrorCode(),
		ErrorMessage:              m.ErrorMessage(),
	}
}

// PublicationError is a copy of a PublicationErrorFrame (io.aeron.status.PublicationErrorFrame in Java).
type PublicationError struct {
	RegistrationID            int64
	DestinationRegistrationID int64
	SessionID                 int32
	StreamID                  int32
	ReceiverID                int64
	GroupTag                  int64
	SourceAddress             *net.UDPAddr
	ErrorCode                 int32
	ErrorMessage              string
}
