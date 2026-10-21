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

package driver

import (
	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/command"
)

// RemovePublicationWithRevoke sends a remove publication command carrying the flags field, optionally asking the
// driver to revoke the publication (Java DriverProxy.removePublication(registrationId, revoke)).
func (driver *Proxy) RemovePublicationWithRevoke(registrationID int64, revoke bool) error {
	correlationID := driver.toDriverCommandBuffer.NextCorrelationID()

	logger.Debugf("driver.RemovePublication: clientId=%d correlationId=%d (regId=%d revoke=%v)",
		driver.clientID, correlationID, registrationID, revoke)

	filler := func(buffer *atomic.Buffer, length *int) int32 {
		var message command.RemovePublicationMessage
		message.Wrap(buffer, 0)

		message.ClientID.Set(driver.clientID)
		message.CorrelationID.Set(correlationID)
		message.RegistrationID.Set(registrationID)
		var flags int64
		if revoke {
			flags |= command.RemovePublicationFlagRevoke
		}
		message.Flags.Set(flags)

		*length = message.Size()

		return command.RemovePublication
	}

	return driver.writeCommandToDriver(filler)
}
