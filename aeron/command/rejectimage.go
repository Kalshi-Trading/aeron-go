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

package command

import (
	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/flyweight"
)

// RejectImage command from client to reject an image (Aeron 1.47+), ControlProtocolEvents.REJECT_IMAGE.
const RejectImage int32 = 0x10

// RejectImageMessage is the flyweight for the REJECT_IMAGE command (Java RejectImageFlyweight):
//
//	clientId(8) correlationId(8) imageCorrelationId(8) position(8) reasonLength(4) reason(ascii)
type RejectImageMessage struct {
	flyweight.FWBase

	ClientID           flyweight.Int64Field
	CorrelationID      flyweight.Int64Field
	ImageCorrelationID flyweight.Int64Field
	Position           flyweight.Int64Field
	Reason             flyweight.StringField
}

// RejectImageMessageMinLength is the length of a RejectImageMessage with an empty reason.
const RejectImageMessageMinLength = 8*4 + 4

func (m *RejectImageMessage) Wrap(buf *atomic.Buffer, offset int) flyweight.Flyweight {
	pos := offset
	pos += m.ClientID.Wrap(buf, pos)
	pos += m.CorrelationID.Wrap(buf, pos)
	pos += m.ImageCorrelationID.Wrap(buf, pos)
	pos += m.Position.Wrap(buf, pos)
	pos += m.Reason.Wrap(buf, pos, m, true)

	m.SetSize(pos - offset)
	return m
}
