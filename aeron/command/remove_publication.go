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
	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/flyweight"
)

// RemovePublicationFlagRevoke is set in RemovePublicationMessage.Flags to revoke the publication (Aeron 1.48+).
const RemovePublicationFlagRevoke int64 = 0x1

// RemovePublicationMessage is the RemoveMessage layout extended with a flags field
// (io.aeron.command.RemovePublicationFlyweight).
type RemovePublicationMessage struct {
	flyweight.FWBase

	ClientID       flyweight.Int64Field
	CorrelationID  flyweight.Int64Field
	RegistrationID flyweight.Int64Field
	Flags          flyweight.Int64Field
}

func (m *RemovePublicationMessage) Wrap(buf *atomic.Buffer, offset int) flyweight.Flyweight {
	pos := offset
	pos += m.ClientID.Wrap(buf, pos)
	pos += m.CorrelationID.Wrap(buf, pos)
	pos += m.RegistrationID.Wrap(buf, pos)
	pos += m.Flags.Wrap(buf, pos)

	m.SetSize(pos - offset)
	return m
}
