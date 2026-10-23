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
	"github.com/lirm/aeron-go/aeron/command"
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

// decodePublicationErrorFrame decodes an ON_PUBLICATION_ERROR message into frame.
func decodePublicationErrorFrame(buffer *atomic.Buffer, offset int32, length int32, frame *PublicationErrorFrame) error {
	var fly command.PublicationErrorFrame
	fly.Wrap(buffer, offset)
	if err := fly.ValidateLength(length); err != nil {
		return err
	}
	frame.RegistrationID = fly.RegistrationID()
	frame.DestinationRegistrationID = fly.DestinationRegistrationID()
	frame.SessionID = fly.SessionID()
	frame.StreamID = fly.StreamID()
	frame.ReceiverID = fly.ReceiverID()
	frame.GroupTag = fly.GroupTag()
	frame.ErrorCode = ErrorCodeFromValue(fly.ErrorCode()).Value()
	frame.SourceAddress, _ = fly.SourceAddress()
	frame.ErrorMessage = fly.ErrorMessage()
	return nil
}
