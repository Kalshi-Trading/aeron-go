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
	"github.com/lirm/aeron-go/aeron/util"
)

// AddStaticCounter is the client command to add (or find) a static counter (Aeron 1.45+).
const AddStaticCounter int32 = 0x0F

// StaticCounterMessage is the ADD_STATIC_COUNTER command (io.aeron.command.StaticCounterMessageFlyweight):
// clientId, correlationId, registrationId, typeId, keyLength, key (aligned to 4), labelLength, label.
type StaticCounterMessage struct {
	flyweight.FWBase

	buf            *atomic.Buffer
	offset         int
	ClientID       flyweight.Int64Field
	CorrelationID  flyweight.Int64Field
	RegistrationID flyweight.Int64Field
	CounterTypeID  flyweight.Int32Field
	key            flyweight.LengthAndRawDataField
	label          flyweight.LengthAndRawDataField
}

func (m *StaticCounterMessage) Wrap(buf *atomic.Buffer, offset int) flyweight.Flyweight {
	m.buf = buf
	m.offset = offset
	pos := offset
	pos += m.ClientID.Wrap(buf, pos)
	pos += m.CorrelationID.Wrap(buf, pos)
	pos += m.RegistrationID.Wrap(buf, pos)
	pos += m.CounterTypeID.Wrap(buf, pos)
	m.key.Wrap(buf, pos)
	m.wrapLabel()
	m.setSize()
	return m
}

func (m *StaticCounterMessage) getLabelOffset() int {
	return 8 + 8 + 8 + 4 + 4 + int(util.AlignInt32(m.key.Length(), 4))
}

func (m *StaticCounterMessage) setSize() {
	m.SetSize(m.getLabelOffset() + 4 + int(m.label.Length()))
}

func (m *StaticCounterMessage) wrapLabel() {
	m.label.Wrap(m.buf, m.offset+m.getLabelOffset())
}

// CopyKeyBuffer must be called before the label is set.
func (m *StaticCounterMessage) CopyKeyBuffer(buffer *atomic.Buffer, offset int32, length int32) {
	m.key.CopyBuffer(buffer, offset, length)
	m.wrapLabel()
	m.setSize()
}

func (m *StaticCounterMessage) CopyLabelBuffer(buffer *atomic.Buffer, offset int32, length int32) {
	m.wrapLabel()
	m.label.CopyBuffer(buffer, offset, length)
	m.setSize()
}

func (m *StaticCounterMessage) CopyLabelString(label string) {
	m.wrapLabel()
	m.label.CopyString(label)
	m.setSize()
}

// KeyLength returns the length of the key.
func (m *StaticCounterMessage) KeyLength() int32 { return m.key.Length() }

// LabelLength returns the length of the label.
func (m *StaticCounterMessage) LabelLength() int32 { return m.label.Length() }

// LabelString returns the label.
func (m *StaticCounterMessage) LabelString() string { return m.label.GetAsASCII() }
