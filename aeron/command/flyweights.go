/*
Copyright 2016 Stanislav Liberman
Copyright (C) 2022 Talos, Inc.

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

	"github.com/lirm/aeron-go/aeron/atomic"
)

// Flyweights over the client -> media driver commands. Offsets and lengths mirror io.aeron.command.* in Java 1.53.3
// exactly. All fields are little-endian; strings and byte arrays are encoded as an int32 length followed by the bytes.

const (
	sizeOfInt16 int32 = 2
	sizeOfInt32 int32 = 4
	sizeOfInt64 int32 = 8
)

func align4(v int32) int32 {
	return (v + 3) &^ 3
}

// ControlProtocolError is returned by ValidateLength when a command is malformed (ControlProtocolException in Java).
type ControlProtocolError struct {
	Code    int32
	Message string
}

func (e *ControlProtocolError) Error() string {
	return fmt.Sprintf("%s: %s", ErrorCodeName(e.Code), e.Message)
}

func malformed(format string, args ...interface{}) error {
	return &ControlProtocolError{Code: ErrorCodeMalformedCommand, Message: fmt.Sprintf(format, args...)}
}

// flyweightBase holds the wrapped buffer and the offset of the message within it.
type flyweightBase struct {
	buffer *atomic.Buffer
	offset int32
}

func (f *flyweightBase) wrap(buffer *atomic.Buffer, offset int32) {
	f.buffer = buffer
	f.offset = offset
}

// Buffer returns the wrapped buffer.
func (f *flyweightBase) Buffer() *atomic.Buffer {
	return f.buffer
}

// Offset returns the offset of the message within the wrapped buffer.
func (f *flyweightBase) Offset() int32 {
	return f.offset
}

func (f *flyweightBase) getInt32(fieldOffset int32) int32 {
	return f.buffer.GetInt32(f.offset + fieldOffset)
}

func (f *flyweightBase) putInt32(fieldOffset int32, v int32) {
	f.buffer.PutInt32(f.offset+fieldOffset, v)
}

func (f *flyweightBase) getInt64(fieldOffset int32) int64 {
	return f.buffer.GetInt64(f.offset + fieldOffset)
}

func (f *flyweightBase) putInt64(fieldOffset int32, v int64) {
	f.buffer.PutInt64(f.offset+fieldOffset, v)
}

// getString reads a length-prefixed ASCII string (Java DirectBuffer.getStringAscii).
func (f *flyweightBase) getString(fieldOffset int32) string {
	length := f.getInt32(fieldOffset)
	if length <= 0 {
		return ""
	}
	return string(f.buffer.GetBytesArray(f.offset+fieldOffset+sizeOfInt32, length))
}

// putString writes a length-prefixed string (Java MutableDirectBuffer.putStringAscii) and returns the bytes written.
func (f *flyweightBase) putString(fieldOffset int32, value string) int32 {
	return f.putBytes(fieldOffset, []byte(value))
}

func (f *flyweightBase) putBytes(fieldOffset int32, value []byte) int32 {
	length := int32(len(value))
	f.putInt32(fieldOffset, length)
	if length > 0 {
		f.buffer.PutBytesArray(f.offset+fieldOffset+sizeOfInt32, &value, 0, length)
	}
	return sizeOfInt32 + length
}

func (f *flyweightBase) putBuffer(fieldOffset int32, src *atomic.Buffer, srcOffset, length int32) {
	f.putInt32(fieldOffset, length)
	if src != nil && length > 0 {
		f.buffer.PutBytes(f.offset+fieldOffset+sizeOfInt32, src, srcOffset, length)
	}
}

func (f *flyweightBase) getBytes(fieldOffset int32) []byte {
	length := f.getInt32(fieldOffset)
	if length <= 0 {
		return nil
	}
	return f.buffer.GetBytesArray(f.offset+fieldOffset+sizeOfInt32, length)
}

// CorrelatedMessage is the base of every client command (CorrelatedMessageFlyweight).
//
//	0                   1                   2                   3
//	0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
//	+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//	|                            Client ID                          |
//	|                                                               |
//	+---------------------------------------------------------------+
//	|                         Correlation ID                        |
//	|                                                               |
//	+---------------------------------------------------------------+
//
// It is also the complete CLIENT_KEEPALIVE and CLIENT_CLOSE command.
type CorrelatedMessage struct {
	flyweightBase
}

const (
	correlatedClientIDOffset      int32 = 0
	correlatedCorrelationIDOffset int32 = correlatedClientIDOffset + sizeOfInt64
	// CorrelatedMessageLength is the length of a CorrelatedMessage.
	CorrelatedMessageLength int32 = correlatedCorrelationIDOffset + sizeOfInt64
)

// Wrap the buffer at the given offset.
func (m *CorrelatedMessage) Wrap(buffer *atomic.Buffer, offset int32) *CorrelatedMessage {
	m.wrap(buffer, offset)
	return m
}

// ClientID is the id of the client sending the command.
func (m *CorrelatedMessage) ClientID() int64 { return m.getInt64(correlatedClientIDOffset) }

// SetClientID sets the client id.
func (m *CorrelatedMessage) SetClientID(v int64) { m.putInt64(correlatedClientIDOffset, v) }

// CorrelationID is the id used to correlate the response with this command.
func (m *CorrelatedMessage) CorrelationID() int64 { return m.getInt64(correlatedCorrelationIDOffset) }

// SetCorrelationID sets the correlation id.
func (m *CorrelatedMessage) SetCorrelationID(v int64) { m.putInt64(correlatedCorrelationIDOffset, v) }

// Length of the encoded message.
func (m *CorrelatedMessage) Length() int32 { return CorrelatedMessageLength }

// ValidateLength checks a received message of the given length is long enough.
func (m *CorrelatedMessage) ValidateLength(msgTypeID, length int32) error {
	if length < CorrelatedMessageLength {
		return malformed("command=%d too short: length=%d", msgTypeID, length)
	}
	return nil
}

// PublicationMessage is ADD_PUBLICATION / ADD_EXCLUSIVE_PUBLICATION (PublicationMessageFlyweight).
//
//	+---------------------------------------------------------------+
//	|                    CorrelatedMessage (16)                     |
//	+---------------------------------------------------------------+
//	|                           Stream ID                           |
//	+---------------------------------------------------------------+
//	|                         Channel Length                        |
//	+---------------------------------------------------------------+
//	|                       Channel (ASCII)                       ...
//	+---------------------------------------------------------------+
type PublicationMessage struct {
	CorrelatedMessage
}

const (
	publicationStreamIDOffset int32 = CorrelatedMessageLength
	publicationChannelOffset  int32 = publicationStreamIDOffset + sizeOfInt32
	publicationMinimumLength  int32 = publicationChannelOffset + sizeOfInt32
)

// ComputePublicationMessageLength returns the encoded length for a channel of the given length.
func ComputePublicationMessageLength(channelLength int) int32 {
	return publicationMinimumLength + int32(channelLength)
}

// Wrap the buffer at the given offset.
func (m *PublicationMessage) Wrap(buffer *atomic.Buffer, offset int32) *PublicationMessage {
	m.wrap(buffer, offset)
	return m
}

// StreamID of the publication.
func (m *PublicationMessage) StreamID() int32 { return m.getInt32(publicationStreamIDOffset) }

// SetStreamID sets the stream id.
func (m *PublicationMessage) SetStreamID(v int32) { m.putInt32(publicationStreamIDOffset, v) }

// Channel URI of the publication.
func (m *PublicationMessage) Channel() string { return m.getString(publicationChannelOffset) }

// SetChannel sets the channel URI.
func (m *PublicationMessage) SetChannel(v string) { m.putString(publicationChannelOffset, v) }

// Length of the encoded message.
func (m *PublicationMessage) Length() int32 {
	return publicationMinimumLength + m.getInt32(publicationChannelOffset)
}

// ValidateLength checks a received message of the given length is long enough.
func (m *PublicationMessage) ValidateLength(msgTypeID, length int32) error {
	if length < publicationMinimumLength {
		return malformed("command=%d too short: length=%d", msgTypeID, length)
	}
	if length < m.Length() {
		return malformed("command=%d too short for channel: length=%d", msgTypeID, length)
	}
	return nil
}

// SubscriptionMessage is ADD_SUBSCRIPTION (SubscriptionMessageFlyweight).
//
//	+---------------------------------------------------------------+
//	|                    CorrelatedMessage (16)                     |
//	+---------------------------------------------------------------+
//	|                 Registration Correlation ID                   |
//	|                                                               |
//	+---------------------------------------------------------------+
//	|                         Stream ID                             |
//	+---------------------------------------------------------------+
//	|                       Channel Length                          |
//	+---------------------------------------------------------------+
//	|                       Channel (ASCII)                       ...
//	+---------------------------------------------------------------+
type SubscriptionMessage struct {
	CorrelatedMessage
}

const (
	subscriptionRegistrationCorrelationIDOffset int32 = CorrelatedMessageLength
	subscriptionStreamIDOffset                  int32 = subscriptionRegistrationCorrelationIDOffset + sizeOfInt64
	subscriptionChannelOffset                   int32 = subscriptionStreamIDOffset + sizeOfInt32
	subscriptionMinimumLength                   int32 = subscriptionChannelOffset + sizeOfInt32
)

// ComputeSubscriptionMessageLength returns the encoded length for a channel of the given length.
func ComputeSubscriptionMessageLength(channelLength int) int32 {
	return subscriptionMinimumLength + int32(channelLength)
}

// Wrap the buffer at the given offset.
func (m *SubscriptionMessage) Wrap(buffer *atomic.Buffer, offset int32) *SubscriptionMessage {
	m.wrap(buffer, offset)
	return m
}

// RegistrationCorrelationID is the registration id of an existing subscription (or -1 for a new one).
func (m *SubscriptionMessage) RegistrationCorrelationID() int64 {
	return m.getInt64(subscriptionRegistrationCorrelationIDOffset)
}

// SetRegistrationCorrelationID sets the registration correlation id.
func (m *SubscriptionMessage) SetRegistrationCorrelationID(v int64) {
	m.putInt64(subscriptionRegistrationCorrelationIDOffset, v)
}

// StreamID of the subscription.
func (m *SubscriptionMessage) StreamID() int32 { return m.getInt32(subscriptionStreamIDOffset) }

// SetStreamID sets the stream id.
func (m *SubscriptionMessage) SetStreamID(v int32) { m.putInt32(subscriptionStreamIDOffset, v) }

// Channel URI of the subscription.
func (m *SubscriptionMessage) Channel() string { return m.getString(subscriptionChannelOffset) }

// SetChannel sets the channel URI.
func (m *SubscriptionMessage) SetChannel(v string) { m.putString(subscriptionChannelOffset, v) }

// Length of the encoded message.
func (m *SubscriptionMessage) Length() int32 {
	return subscriptionMinimumLength + m.getInt32(subscriptionChannelOffset)
}

// ValidateLength checks a received message of the given length is long enough.
func (m *SubscriptionMessage) ValidateLength(msgTypeID, length int32) error {
	if length < subscriptionMinimumLength {
		return malformed("command=%d too short: length=%d", msgTypeID, length)
	}
	if length < m.Length() {
		return malformed("command=%d too short for channel: length=%d", msgTypeID, length)
	}
	return nil
}

// RemoveMessage is REMOVE_SUBSCRIPTION / REMOVE_COUNTER (RemoveMessageFlyweight and its RemoveSubscriptionFlyweight /
// RemoveCounterFlyweight subclasses, which add no fields).
//
//	+---------------------------------------------------------------+
//	|                    CorrelatedMessage (16)                     |
//	+---------------------------------------------------------------+
//	|                       Registration ID                         |
//	|                                                               |
//	+---------------------------------------------------------------+
type RemoveMessage struct {
	CorrelatedMessage
}

const (
	removeRegistrationIDOffset int32 = CorrelatedMessageLength
	// RemoveMessageLength is the length of REMOVE_SUBSCRIPTION and REMOVE_COUNTER commands.
	RemoveMessageLength int32 = removeRegistrationIDOffset + sizeOfInt64
)

// Wrap the buffer at the given offset.
func (m *RemoveMessage) Wrap(buffer *atomic.Buffer, offset int32) *RemoveMessage {
	m.wrap(buffer, offset)
	return m
}

// RegistrationID of the resource to remove.
func (m *RemoveMessage) RegistrationID() int64 { return m.getInt64(removeRegistrationIDOffset) }

// SetRegistrationID sets the registration id.
func (m *RemoveMessage) SetRegistrationID(v int64) { m.putInt64(removeRegistrationIDOffset, v) }

// Length of the encoded message.
func (m *RemoveMessage) Length() int32 { return RemoveMessageLength }

// ValidateLength checks a received message of the given length is long enough.
func (m *RemoveMessage) ValidateLength(msgTypeID, length int32) error {
	if length < RemoveMessageLength {
		return malformed("command=%d too short: length=%d", msgTypeID, length)
	}
	return nil
}

// RemovePublicationMessage is REMOVE_PUBLICATION (RemovePublicationFlyweight), a RemoveMessage plus a flags field.
//
//	+---------------------------------------------------------------+
//	|                      RemoveMessage (24)                       |
//	+---------------------------------------------------------------+
//	|                            Flags                              |
//	|                                                               |
//	+---------------------------------------------------------------+
type RemovePublicationMessage struct {
	RemoveMessage
}

const (
	removePublicationFlagsOffset int32 = RemoveMessageLength
	// RemovePublicationMessageLength is the length of a REMOVE_PUBLICATION command.
	RemovePublicationMessageLength int32 = removePublicationFlagsOffset + sizeOfInt64
	// RemovePublicationFlagRevoke is set in the flags field to revoke the publication.
	RemovePublicationFlagRevoke int64 = 0x1
)

// Wrap the buffer at the given offset.
func (m *RemovePublicationMessage) Wrap(buffer *atomic.Buffer, offset int32) *RemovePublicationMessage {
	m.wrap(buffer, offset)
	return m
}

// Flags of the command.
func (m *RemovePublicationMessage) Flags() int64 { return m.getInt64(removePublicationFlagsOffset) }

// SetFlags sets the flags field.
func (m *RemovePublicationMessage) SetFlags(v int64) { m.putInt64(removePublicationFlagsOffset, v) }

// Revoke reports whether the revoke flag is set. Older clients send RemoveMessageLength bytes without flags, so pass
// the received message length (or RemovePublicationMessageLength when encoding).
func (m *RemovePublicationMessage) Revoke(messageLength int32) bool {
	return messageLength >= RemovePublicationMessageLength && m.Flags()&RemovePublicationFlagRevoke != 0
}

// SetRevoke sets or clears the revoke flag.
func (m *RemovePublicationMessage) SetRevoke(revoke bool) {
	flags := m.Flags()
	if revoke {
		flags |= RemovePublicationFlagRevoke
	} else {
		flags &^= RemovePublicationFlagRevoke
	}
	m.SetFlags(flags)
}

// Length of the encoded message.
func (m *RemovePublicationMessage) Length() int32 { return RemovePublicationMessageLength }

// DestinationMessage is ADD_DESTINATION / REMOVE_DESTINATION / ADD_RCV_DESTINATION / REMOVE_RCV_DESTINATION
// (DestinationMessageFlyweight).
//
//	+---------------------------------------------------------------+
//	|                    CorrelatedMessage (16)                     |
//	+---------------------------------------------------------------+
//	|                 Registration Correlation ID                   |
//	|                                                               |
//	+---------------------------------------------------------------+
//	|                       Channel Length                          |
//	+---------------------------------------------------------------+
//	|                       Channel (ASCII)                       ...
//	+---------------------------------------------------------------+
type DestinationMessage struct {
	CorrelatedMessage
}

const (
	destinationRegistrationCorrelationIDOffset int32 = CorrelatedMessageLength
	destinationChannelOffset                   int32 = destinationRegistrationCorrelationIDOffset + sizeOfInt64
	destinationMinimumLength                   int32 = destinationChannelOffset + sizeOfInt32
)

// ComputeDestinationMessageLength returns the encoded length for a channel of the given length.
func ComputeDestinationMessageLength(channelLength int) int32 {
	return destinationMinimumLength + int32(channelLength)
}

// Wrap the buffer at the given offset.
func (m *DestinationMessage) Wrap(buffer *atomic.Buffer, offset int32) *DestinationMessage {
	m.wrap(buffer, offset)
	return m
}

// RegistrationCorrelationID is the registration id of the publication or subscription.
func (m *DestinationMessage) RegistrationCorrelationID() int64 {
	return m.getInt64(destinationRegistrationCorrelationIDOffset)
}

// SetRegistrationCorrelationID sets the registration correlation id.
func (m *DestinationMessage) SetRegistrationCorrelationID(v int64) {
	m.putInt64(destinationRegistrationCorrelationIDOffset, v)
}

// Channel URI of the destination.
func (m *DestinationMessage) Channel() string { return m.getString(destinationChannelOffset) }

// SetChannel sets the destination channel URI.
func (m *DestinationMessage) SetChannel(v string) { m.putString(destinationChannelOffset, v) }

// Length of the encoded message.
func (m *DestinationMessage) Length() int32 {
	return destinationMinimumLength + m.getInt32(destinationChannelOffset)
}

// ValidateLength checks a received message of the given length is long enough.
func (m *DestinationMessage) ValidateLength(msgTypeID, length int32) error {
	if length < destinationMinimumLength {
		return malformed("command=%d too short: length=%d", msgTypeID, length)
	}
	if length < m.Length() {
		return malformed("command=%d too short for channel: length=%d", msgTypeID, length)
	}
	return nil
}

// DestinationByIDMessage is REMOVE_DESTINATION_BY_ID (DestinationByIdMessageFlyweight).
//
//	+---------------------------------------------------------------+
//	|                    CorrelatedMessage (16)                     |
//	+---------------------------------------------------------------+
//	|                  Resource Registration ID                     |
//	|                                                               |
//	+---------------------------------------------------------------+
//	|                 Destination Registration ID                   |
//	|                                                               |
//	+---------------------------------------------------------------+
type DestinationByIDMessage struct {
	CorrelatedMessage
}

const (
	destinationByIDResourceRegistrationIDOffset    int32 = CorrelatedMessageLength
	destinationByIDDestinationRegistrationIDOffset int32 = destinationByIDResourceRegistrationIDOffset + sizeOfInt64
	// DestinationByIDMessageLength is the length of a REMOVE_DESTINATION_BY_ID command.
	DestinationByIDMessageLength int32 = destinationByIDDestinationRegistrationIDOffset + sizeOfInt64
)

// Wrap the buffer at the given offset.
func (m *DestinationByIDMessage) Wrap(buffer *atomic.Buffer, offset int32) *DestinationByIDMessage {
	m.wrap(buffer, offset)
	return m
}

// ResourceRegistrationID is the registration id of the publication.
func (m *DestinationByIDMessage) ResourceRegistrationID() int64 {
	return m.getInt64(destinationByIDResourceRegistrationIDOffset)
}

// SetResourceRegistrationID sets the resource registration id.
func (m *DestinationByIDMessage) SetResourceRegistrationID(v int64) {
	m.putInt64(destinationByIDResourceRegistrationIDOffset, v)
}

// DestinationRegistrationID is the registration id (correlation id of the ADD_DESTINATION) of the destination.
func (m *DestinationByIDMessage) DestinationRegistrationID() int64 {
	return m.getInt64(destinationByIDDestinationRegistrationIDOffset)
}

// SetDestinationRegistrationID sets the destination registration id.
func (m *DestinationByIDMessage) SetDestinationRegistrationID(v int64) {
	m.putInt64(destinationByIDDestinationRegistrationIDOffset, v)
}

// Length of the encoded message.
func (m *DestinationByIDMessage) Length() int32 { return DestinationByIDMessageLength }

// ValidateLength checks a received message of the given length is long enough.
func (m *DestinationByIDMessage) ValidateLength(msgTypeID, length int32) error {
	if length < DestinationByIDMessageLength {
		return malformed("command=%d too short: length=%d", msgTypeID, length)
	}
	return nil
}

// counterFields implements the shared key/label layout of CounterMessage and StaticCounterMessage: an int32 key
// length at keyLengthOffset, the key, padding to 4-byte alignment, an int32 label length and the label.
type counterFields struct {
	flyweightBase
}

func (c *counterFields) keyLength(keyLengthOffset int32) int32 { return c.getInt32(keyLengthOffset) }

func (c *counterFields) labelLengthOffset(keyLengthOffset int32) int32 {
	return keyLengthOffset + sizeOfInt32 + align4(c.keyLength(keyLengthOffset))
}

func (c *counterFields) labelLength(keyLengthOffset int32) int32 {
	return c.getInt32(c.labelLengthOffset(keyLengthOffset))
}

func (c *counterFields) length(keyLengthOffset int32) int32 {
	return c.labelLengthOffset(keyLengthOffset) + sizeOfInt32 + c.labelLength(keyLengthOffset)
}

func (c *counterFields) validate(keyLengthOffset, msgTypeID, length int32) error {
	if length < keyLengthOffset+2*sizeOfInt32 {
		return malformed("command=%d too short: length=%d", msgTypeID, length)
	}
	if length-c.labelLengthOffset(keyLengthOffset) < sizeOfInt32 {
		return malformed("command=%d too short for key: length=%d", msgTypeID, length)
	}
	if encoded := c.length(keyLengthOffset); length < encoded {
		return malformed("command=%d too short for label: length=%d encodedLength=%d", msgTypeID, length, encoded)
	}
	return nil
}

// computeCounterLength mirrors Java's CounterMessageFlyweight.computeLength, which counts the label length field
// twice (MINIMUM_LENGTH already includes it), so claimed messages are 4 bytes longer than Length().
func computeCounterLength(keyLengthOffset int32, keyLength, labelLength int) int32 {
	return keyLengthOffset + 2*sizeOfInt32 + align4(int32(keyLength)) + sizeOfInt32 + int32(labelLength)
}

// CounterMessage is ADD_COUNTER (CounterMessageFlyweight).
//
//	+---------------------------------------------------------------+
//	|                    CorrelatedMessage (16)                     |
//	+---------------------------------------------------------------+
//	|                        Counter Type ID                        |
//	+---------------------------------------------------------------+
//	|                           Key Length                          |
//	+---------------------------------------------------------------+
//	|                           Key Buffer                         ...
//	+---------------------------------------------------------------+
//	|                   (padding to 4-byte alignment)               |
//	+---------------------------------------------------------------+
//	|                          Label Length                         |
//	+---------------------------------------------------------------+
//	|                          Label (ASCII)                       ...
//	+---------------------------------------------------------------+
//
// Set the key before the label: the label offset depends on the key length.
type CounterMessage struct {
	counterFields
}

const (
	counterTypeIDOffset    int32 = CorrelatedMessageLength
	counterKeyLengthOffset int32 = counterTypeIDOffset + sizeOfInt32
	// CounterMessageKeyBufferOffset is the offset of the key within an ADD_COUNTER message.
	CounterMessageKeyBufferOffset int32 = counterKeyLengthOffset + sizeOfInt32
)

// ComputeCounterMessageLength returns the length to claim for an ADD_COUNTER message (identical to Java).
func ComputeCounterMessageLength(keyLength, labelLength int) int32 {
	return computeCounterLength(counterKeyLengthOffset, keyLength, labelLength)
}

// Wrap the buffer at the given offset.
func (m *CounterMessage) Wrap(buffer *atomic.Buffer, offset int32) *CounterMessage {
	m.wrap(buffer, offset)
	return m
}

// ClientID is the id of the client sending the command.
func (m *CounterMessage) ClientID() int64 { return m.getInt64(correlatedClientIDOffset) }

// SetClientID sets the client id.
func (m *CounterMessage) SetClientID(v int64) { m.putInt64(correlatedClientIDOffset, v) }

// CorrelationID of the command.
func (m *CounterMessage) CorrelationID() int64 { return m.getInt64(correlatedCorrelationIDOffset) }

// SetCorrelationID sets the correlation id.
func (m *CounterMessage) SetCorrelationID(v int64) { m.putInt64(correlatedCorrelationIDOffset, v) }

// TypeID of the counter.
func (m *CounterMessage) TypeID() int32 { return m.getInt32(counterTypeIDOffset) }

// SetTypeID sets the counter type id.
func (m *CounterMessage) SetTypeID(v int32) { m.putInt32(counterTypeIDOffset, v) }

// KeyBufferOffset is the offset of the key relative to the start of the message.
func (m *CounterMessage) KeyBufferOffset() int32 { return CounterMessageKeyBufferOffset }

// KeyBufferLength is the length of the key.
func (m *CounterMessage) KeyBufferLength() int32 { return m.keyLength(counterKeyLengthOffset) }

// Key returns a copy of the key bytes.
func (m *CounterMessage) Key() []byte { return m.getBytes(counterKeyLengthOffset) }

// SetKeyBuffer copies the key from a buffer; src may be nil when length is 0.
func (m *CounterMessage) SetKeyBuffer(src *atomic.Buffer, srcOffset, length int32) {
	m.putBuffer(counterKeyLengthOffset, src, srcOffset, length)
}

// SetKey copies the key bytes.
func (m *CounterMessage) SetKey(key []byte) { m.putBytes(counterKeyLengthOffset, key) }

// LabelBufferOffset is the offset of the label relative to the start of the message.
func (m *CounterMessage) LabelBufferOffset() int32 {
	return m.labelLengthOffset(counterKeyLengthOffset) + sizeOfInt32
}

// LabelBufferLength is the length of the label.
func (m *CounterMessage) LabelBufferLength() int32 { return m.labelLength(counterKeyLengthOffset) }

// Label of the counter.
func (m *CounterMessage) Label() string {
	return m.getString(m.labelLengthOffset(counterKeyLengthOffset))
}

// SetLabelBuffer copies the label from a buffer; src may be nil when length is 0.
func (m *CounterMessage) SetLabelBuffer(src *atomic.Buffer, srcOffset, length int32) {
	m.putBuffer(m.labelLengthOffset(counterKeyLengthOffset), src, srcOffset, length)
}

// SetLabel sets the label.
func (m *CounterMessage) SetLabel(label string) {
	m.putString(m.labelLengthOffset(counterKeyLengthOffset), label)
}

// Length of the encoded message.
func (m *CounterMessage) Length() int32 { return m.length(counterKeyLengthOffset) }

// ValidateLength checks a received message of the given length is long enough.
func (m *CounterMessage) ValidateLength(msgTypeID, length int32) error {
	return m.validate(counterKeyLengthOffset, msgTypeID, length)
}

// StaticCounterMessage is ADD_STATIC_COUNTER (StaticCounterMessageFlyweight).
//
//	+---------------------------------------------------------------+
//	|                    CorrelatedMessage (16)                     |
//	+---------------------------------------------------------------+
//	|                        Registration ID                        |
//	|                                                               |
//	+---------------------------------------------------------------+
//	|                        Counter Type ID                        |
//	+---------------------------------------------------------------+
//	|                   Key Length / Key / padding                 ...
//	+---------------------------------------------------------------+
//	|                   Label Length / Label                       ...
//	+---------------------------------------------------------------+
//
// Set the key before the label: the label offset depends on the key length.
type StaticCounterMessage struct {
	counterFields
}

const (
	staticCounterRegistrationIDOffset int32 = CorrelatedMessageLength
	staticCounterTypeIDOffset         int32 = staticCounterRegistrationIDOffset + sizeOfInt64
	staticCounterKeyLengthOffset      int32 = staticCounterTypeIDOffset + sizeOfInt32
	// StaticCounterMessageKeyBufferOffset is the offset of the key within an ADD_STATIC_COUNTER message.
	StaticCounterMessageKeyBufferOffset int32 = staticCounterKeyLengthOffset + sizeOfInt32
)

// ComputeStaticCounterMessageLength returns the length to claim for an ADD_STATIC_COUNTER message (identical to Java).
func ComputeStaticCounterMessageLength(keyLength, labelLength int) int32 {
	return computeCounterLength(staticCounterKeyLengthOffset, keyLength, labelLength)
}

// Wrap the buffer at the given offset.
func (m *StaticCounterMessage) Wrap(buffer *atomic.Buffer, offset int32) *StaticCounterMessage {
	m.wrap(buffer, offset)
	return m
}

// ClientID is the id of the client sending the command.
func (m *StaticCounterMessage) ClientID() int64 { return m.getInt64(correlatedClientIDOffset) }

// SetClientID sets the client id.
func (m *StaticCounterMessage) SetClientID(v int64) { m.putInt64(correlatedClientIDOffset, v) }

// CorrelationID of the command.
func (m *StaticCounterMessage) CorrelationID() int64 {
	return m.getInt64(correlatedCorrelationIDOffset)
}

// SetCorrelationID sets the correlation id.
func (m *StaticCounterMessage) SetCorrelationID(v int64) {
	m.putInt64(correlatedCorrelationIDOffset, v)
}

// RegistrationID is the fixed registration id of the static counter.
func (m *StaticCounterMessage) RegistrationID() int64 {
	return m.getInt64(staticCounterRegistrationIDOffset)
}

// SetRegistrationID sets the registration id.
func (m *StaticCounterMessage) SetRegistrationID(v int64) {
	m.putInt64(staticCounterRegistrationIDOffset, v)
}

// TypeID of the counter.
func (m *StaticCounterMessage) TypeID() int32 { return m.getInt32(staticCounterTypeIDOffset) }

// SetTypeID sets the counter type id.
func (m *StaticCounterMessage) SetTypeID(v int32) { m.putInt32(staticCounterTypeIDOffset, v) }

// KeyBufferOffset is the offset of the key relative to the start of the message.
func (m *StaticCounterMessage) KeyBufferOffset() int32 { return StaticCounterMessageKeyBufferOffset }

// KeyBufferLength is the length of the key.
func (m *StaticCounterMessage) KeyBufferLength() int32 {
	return m.keyLength(staticCounterKeyLengthOffset)
}

// Key returns a copy of the key bytes.
func (m *StaticCounterMessage) Key() []byte { return m.getBytes(staticCounterKeyLengthOffset) }

// SetKeyBuffer copies the key from a buffer; src may be nil when length is 0.
func (m *StaticCounterMessage) SetKeyBuffer(src *atomic.Buffer, srcOffset, length int32) {
	m.putBuffer(staticCounterKeyLengthOffset, src, srcOffset, length)
}

// SetKey copies the key bytes.
func (m *StaticCounterMessage) SetKey(key []byte) { m.putBytes(staticCounterKeyLengthOffset, key) }

// LabelBufferOffset is the offset of the label relative to the start of the message.
func (m *StaticCounterMessage) LabelBufferOffset() int32 {
	return m.labelLengthOffset(staticCounterKeyLengthOffset) + sizeOfInt32
}

// LabelBufferLength is the length of the label.
func (m *StaticCounterMessage) LabelBufferLength() int32 {
	return m.labelLength(staticCounterKeyLengthOffset)
}

// Label of the counter.
func (m *StaticCounterMessage) Label() string {
	return m.getString(m.labelLengthOffset(staticCounterKeyLengthOffset))
}

// SetLabelBuffer copies the label from a buffer; src may be nil when length is 0.
func (m *StaticCounterMessage) SetLabelBuffer(src *atomic.Buffer, srcOffset, length int32) {
	m.putBuffer(m.labelLengthOffset(staticCounterKeyLengthOffset), src, srcOffset, length)
}

// SetLabel sets the label.
func (m *StaticCounterMessage) SetLabel(label string) {
	m.putString(m.labelLengthOffset(staticCounterKeyLengthOffset), label)
}

// Length of the encoded message.
func (m *StaticCounterMessage) Length() int32 { return m.length(staticCounterKeyLengthOffset) }

// ValidateLength checks a received message of the given length is long enough.
func (m *StaticCounterMessage) ValidateLength(msgTypeID, length int32) error {
	return m.validate(staticCounterKeyLengthOffset, msgTypeID, length)
}

// TerminateDriverMessage is TERMINATE_DRIVER (TerminateDriverFlyweight).
//
//	+---------------------------------------------------------------+
//	|                    CorrelatedMessage (16)                     |
//	+---------------------------------------------------------------+
//	|                         Token Length                          |
//	+---------------------------------------------------------------+
//	|                         Token Buffer                         ...
//	+---------------------------------------------------------------+
type TerminateDriverMessage struct {
	CorrelatedMessage
}

const (
	terminateDriverTokenLengthOffset int32 = CorrelatedMessageLength
	// TerminateDriverTokenBufferOffset is the offset of the token within a TERMINATE_DRIVER message.
	TerminateDriverTokenBufferOffset int32 = terminateDriverTokenLengthOffset + sizeOfInt32
	terminateDriverMinimumLength     int32 = TerminateDriverTokenBufferOffset
)

// ComputeTerminateDriverMessageLength returns the encoded length for a token of the given length.
func ComputeTerminateDriverMessageLength(tokenLength int) int32 {
	return terminateDriverMinimumLength + int32(tokenLength)
}

// Wrap the buffer at the given offset.
func (m *TerminateDriverMessage) Wrap(buffer *atomic.Buffer, offset int32) *TerminateDriverMessage {
	m.wrap(buffer, offset)
	return m
}

// TokenBufferOffset is the offset of the token relative to the start of the message.
func (m *TerminateDriverMessage) TokenBufferOffset() int32 { return TerminateDriverTokenBufferOffset }

// TokenBufferLength is the length of the token.
func (m *TerminateDriverMessage) TokenBufferLength() int32 {
	return m.getInt32(terminateDriverTokenLengthOffset)
}

// Token returns a copy of the token bytes.
func (m *TerminateDriverMessage) Token() []byte { return m.getBytes(terminateDriverTokenLengthOffset) }

// SetToken copies the token bytes (may be nil).
func (m *TerminateDriverMessage) SetToken(token []byte) {
	m.putBytes(terminateDriverTokenLengthOffset, token)
}

// SetTokenBuffer copies the token from a buffer; src may be nil when length is 0.
func (m *TerminateDriverMessage) SetTokenBuffer(src *atomic.Buffer, srcOffset, length int32) {
	m.putBuffer(terminateDriverTokenLengthOffset, src, srcOffset, length)
}

// Length of the encoded message.
func (m *TerminateDriverMessage) Length() int32 {
	return TerminateDriverTokenBufferOffset + m.TokenBufferLength()
}

// ValidateLength checks a received message of the given length is long enough.
func (m *TerminateDriverMessage) ValidateLength(msgTypeID, length int32) error {
	if length < terminateDriverMinimumLength {
		return malformed("command=%d too short: length=%d", msgTypeID, length)
	}
	if length-terminateDriverMinimumLength < m.TokenBufferLength() {
		return malformed("command=%d too short for token buffer: length=%d", msgTypeID, length)
	}
	return nil
}

// RejectImageMessage is REJECT_IMAGE (RejectImageFlyweight).
//
//	+---------------------------------------------------------------+
//	|                    CorrelatedMessage (16)                     |
//	+---------------------------------------------------------------+
//	|                      Image Correlation ID                     |
//	|                                                               |
//	+---------------------------------------------------------------+
//	|                            Position                           |
//	|                                                               |
//	+---------------------------------------------------------------+
//	|                         Reason Length                         |
//	+---------------------------------------------------------------+
//	|                         Reason (ASCII)                       ...
//	+---------------------------------------------------------------+
type RejectImageMessage struct {
	CorrelatedMessage
}

const (
	rejectImageImageCorrelationIDOffset int32 = CorrelatedMessageLength
	rejectImagePositionOffset           int32 = rejectImageImageCorrelationIDOffset + sizeOfInt64
	rejectImageReasonOffset             int32 = rejectImagePositionOffset + sizeOfInt64
	rejectImageMinimumLength            int32 = rejectImageReasonOffset + sizeOfInt32
)

// ComputeRejectImageMessageLength returns the encoded length for a reason of the given length.
func ComputeRejectImageMessageLength(reasonLength int) int32 {
	return rejectImageMinimumLength + int32(reasonLength)
}

// Wrap the buffer at the given offset.
func (m *RejectImageMessage) Wrap(buffer *atomic.Buffer, offset int32) *RejectImageMessage {
	m.wrap(buffer, offset)
	return m
}

// ImageCorrelationID is the correlation id of the image to reject.
func (m *RejectImageMessage) ImageCorrelationID() int64 {
	return m.getInt64(rejectImageImageCorrelationIDOffset)
}

// SetImageCorrelationID sets the image correlation id.
func (m *RejectImageMessage) SetImageCorrelationID(v int64) {
	m.putInt64(rejectImageImageCorrelationIDOffset, v)
}

// Position of the image at the time of rejection.
func (m *RejectImageMessage) Position() int64 { return m.getInt64(rejectImagePositionOffset) }

// SetPosition sets the position.
func (m *RejectImageMessage) SetPosition(v int64) { m.putInt64(rejectImagePositionOffset, v) }

// Reason for the rejection.
func (m *RejectImageMessage) Reason() string { return m.getString(rejectImageReasonOffset) }

// SetReason sets the reason.
func (m *RejectImageMessage) SetReason(v string) { m.putString(rejectImageReasonOffset, v) }

// ReasonBufferLength is the length of the reason.
func (m *RejectImageMessage) ReasonBufferLength() int32 { return m.getInt32(rejectImageReasonOffset) }

// Length of the encoded message.
func (m *RejectImageMessage) Length() int32 { return rejectImageMinimumLength + m.ReasonBufferLength() }

// ValidateLength checks a received message of the given length is long enough.
func (m *RejectImageMessage) ValidateLength(msgTypeID, length int32) error {
	if length < rejectImageMinimumLength || length < m.Length() {
		return malformed("command=%d too short: length=%d", msgTypeID, length)
	}
	return nil
}

// GetNextAvailableSessionIDMessage is GET_NEXT_AVAILABLE_SESSION_ID (GetNextAvailableSessionIdMessageFlyweight).
//
//	+---------------------------------------------------------------+
//	|                    CorrelatedMessage (16)                     |
//	+---------------------------------------------------------------+
//	|                           Stream ID                           |
//	+---------------------------------------------------------------+
type GetNextAvailableSessionIDMessage struct {
	CorrelatedMessage
}

const (
	getNextAvailableSessionIDStreamIDOffset int32 = CorrelatedMessageLength
	// GetNextAvailableSessionIDMessageLength is the length of a GET_NEXT_AVAILABLE_SESSION_ID command.
	GetNextAvailableSessionIDMessageLength int32 = getNextAvailableSessionIDStreamIDOffset + sizeOfInt32
)

// Wrap the buffer at the given offset.
func (m *GetNextAvailableSessionIDMessage) Wrap(buffer *atomic.Buffer, offset int32) *GetNextAvailableSessionIDMessage {
	m.wrap(buffer, offset)
	return m
}

// StreamID to get the next available session id for.
func (m *GetNextAvailableSessionIDMessage) StreamID() int32 {
	return m.getInt32(getNextAvailableSessionIDStreamIDOffset)
}

// SetStreamID sets the stream id.
func (m *GetNextAvailableSessionIDMessage) SetStreamID(v int32) {
	m.putInt32(getNextAvailableSessionIDStreamIDOffset, v)
}

// Length of the encoded message.
func (m *GetNextAvailableSessionIDMessage) Length() int32 {
	return GetNextAvailableSessionIDMessageLength
}

// ValidateLength checks a received message of the given length is long enough.
func (m *GetNextAvailableSessionIDMessage) ValidateLength(msgTypeID, length int32) error {
	if length < GetNextAvailableSessionIDMessageLength {
		return malformed("command=%d too short: length=%d", msgTypeID, length)
	}
	return nil
}
