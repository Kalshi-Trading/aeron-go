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

package driver

import (
	"errors"

	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/command"
)

// RejectImage sends the REJECT_IMAGE command to the driver for the image with the given correlation id at the given
// position. Returns the correlation id of the command.
func (driver *Proxy) RejectImage(imageCorrelationID int64, position int64, reason string) (int64, error) {
	correlationID := driver.toDriverCommandBuffer.NextCorrelationID()

	logger.Debugf("driver.RejectImage: clientID=%d correlationID=%d imageCorrelationID=%d position=%d reason=%s",
		driver.clientID, correlationID, imageCorrelationID, position, reason)

	buffer := atomic.MakeBuffer(make([]byte, command.RejectImageMessageMinLength+len(reason)))
	var message command.RejectImageMessage
	message.Wrap(buffer, 0)
	message.ClientID.Set(driver.clientID)
	message.CorrelationID.Set(correlationID)
	message.ImageCorrelationID.Set(imageCorrelationID)
	message.Position.Set(position)
	if len(reason) > 0 {
		message.Reason.Set(reason)
	}

	if !driver.toDriverCommandBuffer.Write(command.RejectImage, buffer, 0, int32(message.Size())) {
		return 0, errors.New("failed to write reject image command")
	}
	return correlationID, nil
}
