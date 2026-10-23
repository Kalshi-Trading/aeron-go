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

package aeron

import (
	"github.com/lirm/aeron-go/aeron/status"
)

// ChannelStatus returns the channel endpoint status of this publication (status.Active etc.), status.Active for IPC
// (no status counter) and status.NoIDAllocated-equivalent ChannelStatusNoIdAllocated when closed.
func (pub *publicationBase) ChannelStatus() int64 {
	if pub.IsClosed() {
		return int64(ChannelStatusNoIdAllocated)
	}
	switch pub.channelStatusIndicatorID {
	case 0:
		return status.Initializing
	case status.NoIDAllocated:
		return status.Active
	}
	return pub.conductor.CounterReader().GetCounterValue(pub.channelStatusIndicatorID)
}

// LocalSocketAddresses returns the local socket addresses bound by this publication's channel (e.g. the control
// address of an MDC publication), or nil if the channel is not active.
func (pub *publicationBase) LocalSocketAddresses() []string {
	if pub.IsClosed() || pub.channelStatusIndicatorID == status.NoIDAllocated {
		return nil
	}
	return status.FindAddresses(pub.conductor.CounterReader(), pub.ChannelStatus(), pub.channelStatusIndicatorID)
}
