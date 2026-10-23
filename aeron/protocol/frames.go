// Copyright 2026 Kalshi Trading
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

package protocol

import (
	"fmt"

	"github.com/lirm/aeron-go/aeron/atomic"
)

const (
	frameAlignment int32 = 32
	termMinLength  int32 = 64 * 1024
	termMaxLength  int32 = 1024 * 1024 * 1024
	// MaxUDPPayloadLength is the max length of a UDP payload accepted for a setup frame mtu.
	MaxUDPPayloadLength int32 = 65504
)

func isFrameAligned(v int32) bool { return v&(frameAlignment-1) == 0 }
func isPowerOfTwo(v int32) bool   { return v > 0 && v&(v-1) == 0 }

// Status message (SM) frame.
const (
	SMHeaderLength                     int32 = 36
	SMSendSetupFlag                    uint8 = 0x80
	SMEndOfStreamFlag                  uint8 = 0x40
	smSessionIDFieldOffset             int32 = 8
	smStreamIDFieldOffset              int32 = 12
	smConsumptionTermIDFieldOffset     int32 = 16
	smConsumptionTermOffsetFieldOffset int32 = 20
	smReceiverWindowFieldOffset        int32 = 24
	smReceiverIDFieldOffset            int32 = 28
	// SMGroupTagFieldOffset is the offset of the optional group tag in the application specific feedback.
	SMGroupTagFieldOffset int32 = 36
)

// StatusMessageFlyweight is the flyweight for a status message frame.
type StatusMessageFlyweight struct{ HeaderFlyweight }

func (f *StatusMessageFlyweight) Wrap(buf *atomic.Buffer, offset int32) *StatusMessageFlyweight {
	f.HeaderFlyweight.Wrap(buf, offset)
	return f
}
func (f *StatusMessageFlyweight) SessionID() int32     { return f.getInt32(smSessionIDFieldOffset) }
func (f *StatusMessageFlyweight) SetSessionID(v int32) { f.putInt32(smSessionIDFieldOffset, v) }
func (f *StatusMessageFlyweight) StreamID() int32      { return f.getInt32(smStreamIDFieldOffset) }
func (f *StatusMessageFlyweight) SetStreamID(v int32)  { f.putInt32(smStreamIDFieldOffset, v) }
func (f *StatusMessageFlyweight) ConsumptionTermID() int32 {
	return f.getInt32(smConsumptionTermIDFieldOffset)
}
func (f *StatusMessageFlyweight) SetConsumptionTermID(v int32) {
	f.putInt32(smConsumptionTermIDFieldOffset, v)
}
func (f *StatusMessageFlyweight) ConsumptionTermOffset() int32 {
	return f.getInt32(smConsumptionTermOffsetFieldOffset)
}
func (f *StatusMessageFlyweight) SetConsumptionTermOffset(v int32) {
	f.putInt32(smConsumptionTermOffsetFieldOffset, v)
}
func (f *StatusMessageFlyweight) ReceiverWindowLength() int32 {
	return f.getInt32(smReceiverWindowFieldOffset)
}
func (f *StatusMessageFlyweight) SetReceiverWindowLength(v int32) {
	f.putInt32(smReceiverWindowFieldOffset, v)
}
func (f *StatusMessageFlyweight) ReceiverID() int64 {
	return f.getInt64Unaligned(smReceiverIDFieldOffset)
}
func (f *StatusMessageFlyweight) SetReceiverID(v int64) {
	f.putInt64Unaligned(smReceiverIDFieldOffset, v)
}

// AsfLength is the length of the application specific feedback following the header.
func (f *StatusMessageFlyweight) AsfLength() int32 { return f.FrameLength() - SMHeaderLength }

// GroupTag returns the group tag carried as application specific feedback, or 0 if absent.
// An error is returned if the feedback is longer than a group tag.
func (f *StatusMessageFlyweight) GroupTag() (int64, error) {
	frameLength := f.FrameLength()
	if frameLength > SMHeaderLength {
		if frameLength > SMHeaderLength+8 {
			return 0, fmt.Errorf("SM has longer application specific feedback (%d) than gtag",
				frameLength-SMHeaderLength)
		}
		return f.getInt64Unaligned(SMGroupTagFieldOffset), nil
	}
	return 0, nil
}

// SetGroupTag sets the group tag and the frame length; nil clears it.
func (f *StatusMessageFlyweight) SetGroupTag(groupTag *int64) {
	if groupTag != nil {
		f.SetFrameLength(SMHeaderLength + 8)
		f.putInt64Unaligned(SMGroupTagFieldOffset, *groupTag)
	} else {
		f.SetFrameLength(SMHeaderLength)
	}
}

// IsValid validates the consumption offset and receiver window against the term length.
func (f *StatusMessageFlyweight) IsValid(termLength int32) bool {
	termOffset := f.ConsumptionTermOffset()
	window := f.ReceiverWindowLength()
	return termOffset >= 0 && termOffset < termLength && isFrameAligned(termOffset) &&
		window >= 0 && window <= termLength>>1
}

func (f *StatusMessageFlyweight) String() string {
	return fmt.Sprintf("STATUS{frame-length=%d version=%d flags=%s type=%d session-id=%d stream-id=%d "+
		"consumption-term-id=%d consumption-term-offset=%d receiver-window-length=%d}",
		f.FrameLength(), f.Version(), FlagsToChars(f.Flags()), f.HeaderType(), f.SessionID(), f.StreamID(),
		f.ConsumptionTermID(), f.ConsumptionTermOffset(), f.ReceiverWindowLength())
}

// NAK frame.
const (
	NakHeaderLength          int32 = 28
	nakSessionIDFieldOffset  int32 = 8
	nakStreamIDFieldOffset   int32 = 12
	nakTermIDFieldOffset     int32 = 16
	nakTermOffsetFieldOffset int32 = 20
	nakLengthFieldOffset     int32 = 24
)

// NakFlyweight is the flyweight for a NAK frame.
type NakFlyweight struct{ HeaderFlyweight }

func (f *NakFlyweight) Wrap(buf *atomic.Buffer, offset int32) *NakFlyweight {
	f.HeaderFlyweight.Wrap(buf, offset)
	return f
}
func (f *NakFlyweight) SessionID() int32      { return f.getInt32(nakSessionIDFieldOffset) }
func (f *NakFlyweight) SetSessionID(v int32)  { f.putInt32(nakSessionIDFieldOffset, v) }
func (f *NakFlyweight) StreamID() int32       { return f.getInt32(nakStreamIDFieldOffset) }
func (f *NakFlyweight) SetStreamID(v int32)   { f.putInt32(nakStreamIDFieldOffset, v) }
func (f *NakFlyweight) TermID() int32         { return f.getInt32(nakTermIDFieldOffset) }
func (f *NakFlyweight) SetTermID(v int32)     { f.putInt32(nakTermIDFieldOffset, v) }
func (f *NakFlyweight) TermOffset() int32     { return f.getInt32(nakTermOffsetFieldOffset) }
func (f *NakFlyweight) SetTermOffset(v int32) { f.putInt32(nakTermOffsetFieldOffset, v) }
func (f *NakFlyweight) Length() int32         { return f.getInt32(nakLengthFieldOffset) }
func (f *NakFlyweight) SetLength(v int32)     { f.putInt32(nakLengthFieldOffset, v) }

// IsValid validates the NAK range against the term length.
func (f *NakFlyweight) IsValid(termLength int32) bool {
	termOffset := f.TermOffset()
	length := f.Length()
	return termOffset >= 0 && termOffset < termLength && isFrameAligned(termOffset) &&
		length >= 0 && int64(termOffset)+int64(length) <= int64(termLength)
}

func (f *NakFlyweight) String() string {
	return fmt.Sprintf("NAK{frame-length=%d version=%d flags=%s type=%d term-offset=%d session-id=%d "+
		"stream-id=%d term-id=%d length=%d}",
		f.FrameLength(), f.Version(), FlagsToChars(f.Flags()), f.HeaderType(), f.TermOffset(), f.SessionID(),
		f.StreamID(), f.TermID(), f.Length())
}

// Setup frame.
const (
	SetupHeaderLength             int32 = 40
	SetupSendResponseSetupFlag    uint8 = 0x80
	SetupGroupFlag                uint8 = 0x40
	setupTermOffsetFieldOffset    int32 = 8
	setupSessionIDFieldOffset     int32 = 12
	setupStreamIDFieldOffset      int32 = 16
	setupInitialTermIDFieldOffset int32 = 20
	setupActiveTermIDFieldOffset  int32 = 24
	setupTermLengthFieldOffset    int32 = 28
	setupMtuLengthFieldOffset     int32 = 32
	setupTTLFieldOffset           int32 = 36
)

// SetupFlyweight is the flyweight for a setup frame.
type SetupFlyweight struct{ HeaderFlyweight }

func (f *SetupFlyweight) Wrap(buf *atomic.Buffer, offset int32) *SetupFlyweight {
	f.HeaderFlyweight.Wrap(buf, offset)
	return f
}
func (f *SetupFlyweight) TermOffset() int32        { return f.getInt32(setupTermOffsetFieldOffset) }
func (f *SetupFlyweight) SetTermOffset(v int32)    { f.putInt32(setupTermOffsetFieldOffset, v) }
func (f *SetupFlyweight) SessionID() int32         { return f.getInt32(setupSessionIDFieldOffset) }
func (f *SetupFlyweight) SetSessionID(v int32)     { f.putInt32(setupSessionIDFieldOffset, v) }
func (f *SetupFlyweight) StreamID() int32          { return f.getInt32(setupStreamIDFieldOffset) }
func (f *SetupFlyweight) SetStreamID(v int32)      { f.putInt32(setupStreamIDFieldOffset, v) }
func (f *SetupFlyweight) InitialTermID() int32     { return f.getInt32(setupInitialTermIDFieldOffset) }
func (f *SetupFlyweight) SetInitialTermID(v int32) { f.putInt32(setupInitialTermIDFieldOffset, v) }
func (f *SetupFlyweight) ActiveTermID() int32      { return f.getInt32(setupActiveTermIDFieldOffset) }
func (f *SetupFlyweight) SetActiveTermID(v int32)  { f.putInt32(setupActiveTermIDFieldOffset, v) }
func (f *SetupFlyweight) TermLength() int32        { return f.getInt32(setupTermLengthFieldOffset) }
func (f *SetupFlyweight) SetTermLength(v int32)    { f.putInt32(setupTermLengthFieldOffset, v) }
func (f *SetupFlyweight) MtuLength() int32         { return f.getInt32(setupMtuLengthFieldOffset) }
func (f *SetupFlyweight) SetMtuLength(v int32)     { f.putInt32(setupMtuLengthFieldOffset, v) }
func (f *SetupFlyweight) TTL() int32               { return f.getInt32(setupTTLFieldOffset) }
func (f *SetupFlyweight) SetTTL(v int32)           { f.putInt32(setupTTLFieldOffset, v) }

// IsValid validates term length, term offset and mtu.
func (f *SetupFlyweight) IsValid() bool {
	termOffset := f.TermOffset()
	termLength := f.TermLength()
	mtu := f.MtuLength()
	return termLength >= termMinLength && termLength <= termMaxLength && isPowerOfTwo(termLength) &&
		termOffset >= 0 && termOffset < termLength && isFrameAligned(termOffset) &&
		mtu >= DataHeaderLength && mtu <= MaxUDPPayloadLength && isFrameAligned(mtu)
}

func (f *SetupFlyweight) String() string {
	return fmt.Sprintf("SETUP{frame-length=%d version=%d flags=%s type=%d term-offset=%d session-id=%d "+
		"stream-id=%d initial-term-id=%d active-term-id=%d term-length=%d mtu-length=%d ttl=%d}",
		f.FrameLength(), f.Version(), FlagsToChars(f.Flags()), f.HeaderType(), f.TermOffset(), f.SessionID(),
		f.StreamID(), f.InitialTermID(), f.ActiveTermID(), f.TermLength(), f.MtuLength(), f.TTL())
}

// RTT measurement frame.
const (
	RttmHeaderLength              int32 = 40
	RttmReplyFlag                 uint8 = 0x80
	rttmSessionIDFieldOffset      int32 = 8
	rttmStreamIDFieldOffset       int32 = 12
	rttmEchoTimestampFieldOffset  int32 = 16
	rttmReceptionDeltaFieldOffset int32 = 24
	rttmReceiverIDFieldOffset     int32 = 32
)

// RttMeasurementFlyweight is the flyweight for an RTT measurement frame.
type RttMeasurementFlyweight struct{ HeaderFlyweight }

func (f *RttMeasurementFlyweight) Wrap(buf *atomic.Buffer, offset int32) *RttMeasurementFlyweight {
	f.HeaderFlyweight.Wrap(buf, offset)
	return f
}
func (f *RttMeasurementFlyweight) SessionID() int32     { return f.getInt32(rttmSessionIDFieldOffset) }
func (f *RttMeasurementFlyweight) SetSessionID(v int32) { f.putInt32(rttmSessionIDFieldOffset, v) }
func (f *RttMeasurementFlyweight) StreamID() int32      { return f.getInt32(rttmStreamIDFieldOffset) }
func (f *RttMeasurementFlyweight) SetStreamID(v int32)  { f.putInt32(rttmStreamIDFieldOffset, v) }
func (f *RttMeasurementFlyweight) EchoTimestampNs() int64 {
	return f.getInt64(rttmEchoTimestampFieldOffset)
}
func (f *RttMeasurementFlyweight) SetEchoTimestampNs(v int64) {
	f.putInt64(rttmEchoTimestampFieldOffset, v)
}
func (f *RttMeasurementFlyweight) ReceptionDelta() int64 {
	return f.getInt64(rttmReceptionDeltaFieldOffset)
}
func (f *RttMeasurementFlyweight) SetReceptionDelta(v int64) {
	f.putInt64(rttmReceptionDeltaFieldOffset, v)
}
func (f *RttMeasurementFlyweight) ReceiverID() int64     { return f.getInt64(rttmReceiverIDFieldOffset) }
func (f *RttMeasurementFlyweight) SetReceiverID(v int64) { f.putInt64(rttmReceiverIDFieldOffset, v) }

func (f *RttMeasurementFlyweight) String() string {
	return fmt.Sprintf("RTTM{frame-length=%d version=%d flags=%s type=%d session-id=%d stream-id=%d "+
		"echo-timestamp=%d reception-delta=%d receiver-id=%d}",
		f.FrameLength(), f.Version(), FlagsToChars(f.Flags()), f.HeaderType(), f.SessionID(), f.StreamID(),
		f.EchoTimestampNs(), f.ReceptionDelta(), f.ReceiverID())
}

// Error frame.
const (
	ErrSessionIDFieldOffset   int32 = MinHeaderLength
	ErrStreamIDFieldOffset    int32 = ErrSessionIDFieldOffset + 4
	ErrReceiverIDFieldOffset  int32 = ErrStreamIDFieldOffset + 4
	ErrGroupTagFieldOffset    int32 = ErrReceiverIDFieldOffset + 8
	ErrErrorCodeFieldOffset   int32 = ErrGroupTagFieldOffset + 8
	ErrErrorStringFieldOffset int32 = ErrErrorCodeFieldOffset + 4
	ErrHeaderLength           int32 = ErrErrorStringFieldOffset + 4
	MaxErrorMessageLength     int32 = 1023
	MaxErrorFrameLength       int32 = ErrHeaderLength + MaxErrorMessageLength
	ErrHasGroupIDFlag         uint8 = 0x08
)

// ErrorFlyweight is the flyweight for an error frame sent from a receiver to a sender.
type ErrorFlyweight struct{ HeaderFlyweight }

func (f *ErrorFlyweight) Wrap(buf *atomic.Buffer, offset int32) *ErrorFlyweight {
	f.HeaderFlyweight.Wrap(buf, offset)
	return f
}
func (f *ErrorFlyweight) SessionID() int32      { return f.getInt32(ErrSessionIDFieldOffset) }
func (f *ErrorFlyweight) SetSessionID(v int32)  { f.putInt32(ErrSessionIDFieldOffset, v) }
func (f *ErrorFlyweight) StreamID() int32       { return f.getInt32(ErrStreamIDFieldOffset) }
func (f *ErrorFlyweight) SetStreamID(v int32)   { f.putInt32(ErrStreamIDFieldOffset, v) }
func (f *ErrorFlyweight) ReceiverID() int64     { return f.getInt64(ErrReceiverIDFieldOffset) }
func (f *ErrorFlyweight) SetReceiverID(v int64) { f.putInt64(ErrReceiverIDFieldOffset, v) }
func (f *ErrorFlyweight) GroupTag() int64       { return f.getInt64(ErrGroupTagFieldOffset) }
func (f *ErrorFlyweight) HasGroupTag() bool     { return f.Flags()&ErrHasGroupIDFlag == ErrHasGroupIDFlag }
func (f *ErrorFlyweight) ErrorCode() int32      { return f.getInt32(ErrErrorCodeFieldOffset) }
func (f *ErrorFlyweight) SetErrorCode(v int32)  { f.putInt32(ErrErrorCodeFieldOffset, v) }

// SetGroupTag sets the group tag and the has-group-tag flag; nil clears the flag.
func (f *ErrorFlyweight) SetGroupTag(groupTag *int64) {
	if groupTag == nil {
		f.SetFlags(f.Flags() &^ ErrHasGroupIDFlag)
	} else {
		f.putInt64(ErrGroupTagFieldOffset, *groupTag)
		f.SetFlags(f.Flags() | ErrHasGroupIDFlag)
	}
}

// ErrorMessage returns the ASCII error message.
func (f *ErrorFlyweight) ErrorMessage() string {
	length := f.getInt32(ErrErrorStringFieldOffset)
	if length <= 0 {
		return ""
	}
	return string(f.buf.GetBytesArray(f.offset+ErrErrorStringFieldOffset+4, length))
}

// SetErrorMessage sets the message (truncated to MaxErrorMessageLength) and the frame length.
func (f *ErrorFlyweight) SetErrorMessage(msg string) {
	b := []byte(msg)
	if int32(len(b)) > MaxErrorMessageLength {
		b = b[:MaxErrorMessageLength]
	}
	length := int32(len(b))
	f.putInt32(ErrErrorStringFieldOffset, length)
	if length > 0 {
		f.buf.PutBytesSlice(f.offset+ErrErrorStringFieldOffset+4, b[:length])
	}
	f.SetFrameLength(ErrHeaderLength + length)
}

// IsValid checks the message length is consistent with the frame length.
func (f *ErrorFlyweight) IsValid() bool {
	l := f.getInt32(ErrErrorStringFieldOffset)
	return l >= 0 && l <= MaxErrorMessageLength && ErrHeaderLength+l <= f.FrameLength()
}

func (f *ErrorFlyweight) String() string {
	return fmt.Sprintf("ERROR{frame-length=%d version=%d flags=%s type=%d session-id=%d stream-id=%d "+
		"error-code=%d error-message=%s}",
		f.FrameLength(), f.Version(), FlagsToChars(f.Flags()), f.HeaderType(), f.SessionID(), f.StreamID(),
		f.ErrorCode(), f.ErrorMessage())
}

// Response setup frame.
const (
	RspSetupHeaderLength                 int32 = 20
	rspSetupSessionIDFieldOffset         int32 = 8
	rspSetupStreamIDFieldOffset          int32 = 12
	rspSetupResponseSessionIDFieldOffset int32 = 16
)

// ResponseSetupFlyweight is the flyweight for a response setup frame.
type ResponseSetupFlyweight struct{ HeaderFlyweight }

func (f *ResponseSetupFlyweight) Wrap(buf *atomic.Buffer, offset int32) *ResponseSetupFlyweight {
	f.HeaderFlyweight.Wrap(buf, offset)
	return f
}
func (f *ResponseSetupFlyweight) SessionID() int32     { return f.getInt32(rspSetupSessionIDFieldOffset) }
func (f *ResponseSetupFlyweight) SetSessionID(v int32) { f.putInt32(rspSetupSessionIDFieldOffset, v) }
func (f *ResponseSetupFlyweight) StreamID() int32      { return f.getInt32(rspSetupStreamIDFieldOffset) }
func (f *ResponseSetupFlyweight) SetStreamID(v int32)  { f.putInt32(rspSetupStreamIDFieldOffset, v) }
func (f *ResponseSetupFlyweight) ResponseSessionID() int32 {
	return f.getInt32(rspSetupResponseSessionIDFieldOffset)
}
func (f *ResponseSetupFlyweight) SetResponseSessionID(v int32) {
	f.putInt32(rspSetupResponseSessionIDFieldOffset, v)
}

func (f *ResponseSetupFlyweight) String() string {
	return fmt.Sprintf("RSP_SETUP{frame-length=%d version=%d flags=%s type=%d session-id=%d stream-id=%d "+
		"response-session-id=%d}",
		f.FrameLength(), f.Version(), FlagsToChars(f.Flags()), f.HeaderType(), f.SessionID(), f.StreamID(),
		f.ResponseSessionID())
}
