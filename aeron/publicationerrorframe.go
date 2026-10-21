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

package aeron

import (
	"fmt"
	"net"

	"github.com/lirm/aeron-go/aeron/atomic"
)

// PublicationErrorFrame describes an error frame received by a publication from a receiver, a port of
// io.aeron.status.PublicationErrorFrame. The handler receives a frame that is reused; use Clone to retain it.
type PublicationErrorFrame struct {
	RegistrationID            int64
	DestinationRegistrationID int64
	SessionID                 int32
	StreamID                  int32
	ReceiverID                int64
	GroupTag                  int64
	ErrorCode                 int32
	ErrorMessage              string
	SourceAddress             *net.UDPAddr
}

// PublicationErrorFrameHandler is called when a publication owned by the client receives an error frame.
type PublicationErrorFrameHandler func(frame *PublicationErrorFrame)

// Clone returns a copy of the frame.
func (f *PublicationErrorFrame) Clone() *PublicationErrorFrame {
	c := *f
	if f.SourceAddress != nil {
		addr := *f.SourceAddress
		addr.IP = append(net.IP(nil), f.SourceAddress.IP...)
		c.SourceAddress = &addr
	}
	return &c
}

func (f *PublicationErrorFrame) String() string {
	return fmt.Sprintf("PublicationErrorFrame{registrationId=%d, sessionId=%d, streamId=%d, receiverId=%d, "+
		"destinationRegistrationId=%d, groupTag=%d, errorCode=%d, errorMessage=%q, sourceAddress=%v}",
		f.RegistrationID, f.SessionID, f.StreamID, f.ReceiverID, f.DestinationRegistrationID, f.GroupTag,
		f.ErrorCode, f.ErrorMessage, f.SourceAddress)
}

// PublicationErrorFrameFlyweight layout (io.aeron.command.PublicationErrorFrameFlyweight).
const (
	pubErrRegistrationIDOffset         = 0
	pubErrDestinationRegIDOffset       = pubErrRegistrationIDOffset + 8
	pubErrSessionIDOffset              = pubErrDestinationRegIDOffset + 8
	pubErrStreamIDOffset               = pubErrSessionIDOffset + 4
	pubErrReceiverIDOffset             = pubErrStreamIDOffset + 4
	pubErrGroupTagOffset               = pubErrReceiverIDOffset + 8
	pubErrAddressTypeOffset            = pubErrGroupTagOffset + 8
	pubErrAddressPortOffset            = pubErrAddressTypeOffset + 2
	pubErrAddressOffset                = pubErrAddressPortOffset + 2
	pubErrErrorCodeOffset              = pubErrAddressOffset + 16
	pubErrErrorMessageOffset           = pubErrErrorCodeOffset + 4
	pubErrAddressTypeIPv4        int16 = 1
	pubErrAddressTypeIPv6        int16 = 2
)

// decodePublicationErrorFrame decodes an ON_PUBLICATION_ERROR message into frame.
func decodePublicationErrorFrame(buffer *atomic.Buffer, offset int32, length int32, frame *PublicationErrorFrame) error {
	if length < pubErrErrorMessageOffset+4 {
		return fmt.Errorf("publication error frame too short: %d", length)
	}
	frame.RegistrationID = buffer.GetInt64(offset + pubErrRegistrationIDOffset)
	frame.DestinationRegistrationID = buffer.GetInt64(offset + pubErrDestinationRegIDOffset)
	frame.SessionID = buffer.GetInt32(offset + pubErrSessionIDOffset)
	frame.StreamID = buffer.GetInt32(offset + pubErrStreamIDOffset)
	frame.ReceiverID = buffer.GetInt64(offset + pubErrReceiverIDOffset)
	frame.GroupTag = buffer.GetInt64(offset + pubErrGroupTagOffset)
	frame.ErrorCode = buffer.GetInt32(offset + pubErrErrorCodeOffset)

	frame.SourceAddress = nil
	port := int(buffer.GetUInt16(offset + pubErrAddressPortOffset))
	switch int16(buffer.GetUInt16(offset + pubErrAddressTypeOffset)) {
	case pubErrAddressTypeIPv4:
		frame.SourceAddress = &net.UDPAddr{IP: net.IP(buffer.GetBytesArray(offset+pubErrAddressOffset, 4)), Port: port}
	case pubErrAddressTypeIPv6:
		frame.SourceAddress = &net.UDPAddr{IP: net.IP(buffer.GetBytesArray(offset+pubErrAddressOffset, 16)), Port: port}
	}

	msgLen := buffer.GetInt32(offset + pubErrErrorMessageOffset)
	if msgLen < 0 || pubErrErrorMessageOffset+4+msgLen > length {
		return fmt.Errorf("invalid publication error frame message length: %d", msgLen)
	}
	frame.ErrorMessage = string(buffer.GetBytesArray(offset+pubErrErrorMessageOffset+4, msgLen))
	return nil
}
