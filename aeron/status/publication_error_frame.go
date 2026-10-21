/*
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

package status

import (
	"fmt"
	"net"

	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/util"
)

// Wire layout of the ON_PUBLICATION_ERROR (0x0F0C) driver event (io.aeron.command.PublicationErrorFrameFlyweight).
const (
	PublicationErrorRegistrationIDOffset            int32 = 0
	PublicationErrorDestinationRegistrationIDOffset       = PublicationErrorRegistrationIDOffset + util.SizeOfInt64
	PublicationErrorSessionIDOffset                       = PublicationErrorDestinationRegistrationIDOffset + util.SizeOfInt64
	PublicationErrorStreamIDOffset                        = PublicationErrorSessionIDOffset + util.SizeOfInt32
	PublicationErrorReceiverIDOffset                      = PublicationErrorStreamIDOffset + util.SizeOfInt32
	PublicationErrorGroupTagOffset                        = PublicationErrorReceiverIDOffset + util.SizeOfInt64
	PublicationErrorAddressTypeOffset                     = PublicationErrorGroupTagOffset + util.SizeOfInt64
	PublicationErrorAddressPortOffset                     = PublicationErrorAddressTypeOffset + 2
	PublicationErrorAddressOffset                         = PublicationErrorAddressPortOffset + 2
	PublicationErrorErrorCodeOffset                       = PublicationErrorAddressOffset + 16
	PublicationErrorErrorMessageOffset                    = PublicationErrorErrorCodeOffset + util.SizeOfInt32

	AddressTypeIPv4 int16 = 1
	AddressTypeIPv6 int16 = 2
)

// PublicationErrorFrame is an error frame (e.g. image invalidation) received by a publication and propagated to the
// client by the driver (io.aeron.status.PublicationErrorFrame). It is a plain value; copy it freely.
type PublicationErrorFrame struct {
	RegistrationID            int64
	SessionID                 int32
	StreamID                  int32
	ReceiverID                int64
	DestinationRegistrationID int64
	GroupTag                  int64
	ErrorCode                 int32
	ErrorMessage              string
	// SourceAddress is nil if the driver did not supply an address.
	SourceAddress *net.UDPAddr
}

// DecodePublicationErrorFrame decodes an ON_PUBLICATION_ERROR message at offset of length bytes.
func DecodePublicationErrorFrame(buffer *atomic.Buffer, offset, length int32) (PublicationErrorFrame, error) {
	var f PublicationErrorFrame
	if length < PublicationErrorErrorMessageOffset+util.SizeOfInt32 {
		return f, fmt.Errorf("publication error frame too short: %d", length)
	}
	f.RegistrationID = buffer.GetInt64(offset + PublicationErrorRegistrationIDOffset)
	f.DestinationRegistrationID = buffer.GetInt64(offset + PublicationErrorDestinationRegistrationIDOffset)
	f.SessionID = buffer.GetInt32(offset + PublicationErrorSessionIDOffset)
	f.StreamID = buffer.GetInt32(offset + PublicationErrorStreamIDOffset)
	f.ReceiverID = buffer.GetInt64(offset + PublicationErrorReceiverIDOffset)
	f.GroupTag = buffer.GetInt64(offset + PublicationErrorGroupTagOffset)
	f.ErrorCode = buffer.GetInt32(offset + PublicationErrorErrorCodeOffset)

	addrLen := 0
	switch int16(buffer.GetUInt16(offset + PublicationErrorAddressTypeOffset)) {
	case AddressTypeIPv4:
		addrLen = 4
	case AddressTypeIPv6:
		addrLen = 16
	}
	if addrLen > 0 {
		ip := net.IP(buffer.GetBytesArray(offset+PublicationErrorAddressOffset, int32(addrLen)))
		port := int(buffer.GetUInt16(offset + PublicationErrorAddressPortOffset))
		f.SourceAddress = &net.UDPAddr{IP: ip, Port: port}
	}

	msgLen := buffer.GetInt32(offset + PublicationErrorErrorMessageOffset)
	if msgLen < 0 || PublicationErrorErrorMessageOffset+util.SizeOfInt32+msgLen > length {
		return f, fmt.Errorf("publication error frame message length invalid: %d", msgLen)
	}
	f.ErrorMessage = string(buffer.GetBytesArray(offset+PublicationErrorErrorMessageOffset+util.SizeOfInt32, msgLen))
	return f, nil
}

// EncodePublicationErrorFrame writes f at offset and returns the encoded length. Mainly useful for tests.
func EncodePublicationErrorFrame(buffer *atomic.Buffer, offset int32, f *PublicationErrorFrame) int32 {
	buffer.PutInt64(offset+PublicationErrorRegistrationIDOffset, f.RegistrationID)
	buffer.PutInt64(offset+PublicationErrorDestinationRegistrationIDOffset, f.DestinationRegistrationID)
	buffer.PutInt32(offset+PublicationErrorSessionIDOffset, f.SessionID)
	buffer.PutInt32(offset+PublicationErrorStreamIDOffset, f.StreamID)
	buffer.PutInt64(offset+PublicationErrorReceiverIDOffset, f.ReceiverID)
	buffer.PutInt64(offset+PublicationErrorGroupTagOffset, f.GroupTag)
	addr := make([]byte, 16)
	var addrType int16
	var port uint16
	if f.SourceAddress != nil {
		port = uint16(f.SourceAddress.Port)
		if ip4 := f.SourceAddress.IP.To4(); ip4 != nil {
			addrType = AddressTypeIPv4
			copy(addr, ip4)
		} else {
			addrType = AddressTypeIPv6
			copy(addr, f.SourceAddress.IP.To16())
		}
	}
	buffer.PutUInt16(offset+PublicationErrorAddressTypeOffset, uint16(addrType))
	buffer.PutUInt16(offset+PublicationErrorAddressPortOffset, port)
	buffer.PutBytesArray(offset+PublicationErrorAddressOffset, &addr, 0, 16)
	buffer.PutInt32(offset+PublicationErrorErrorCodeOffset, f.ErrorCode)
	msg := []byte(f.ErrorMessage)
	buffer.PutInt32(offset+PublicationErrorErrorMessageOffset, int32(len(msg)))
	if len(msg) > 0 {
		buffer.PutBytesArray(offset+PublicationErrorErrorMessageOffset+util.SizeOfInt32, &msg, 0, int32(len(msg)))
	}
	return PublicationErrorErrorMessageOffset + util.SizeOfInt32 + int32(len(msg))
}

func (f PublicationErrorFrame) String() string {
	return fmt.Sprintf("PublicationErrorFrame{registrationId=%d, sessionId=%d, streamId=%d, receiverId=%d, "+
		"destinationRegistrationId=%d, groupTag=%d, errorCode=%d, errorMessage=%s, sourceAddress=%v}",
		f.RegistrationID, f.SessionID, f.StreamID, f.ReceiverID, f.DestinationRegistrationID, f.GroupTag,
		f.ErrorCode, f.ErrorMessage, f.SourceAddress)
}
