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

// Package status mirrors io.aeron.status: channel endpoint status values, local socket address status counters and
// publication error frames.
package status

import (
	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/counters"
	"github.com/lirm/aeron-go/aeron/util"
)

// ChannelEndpointStatus values held in a channel status counter (io.aeron.status.ChannelEndpointStatus).
const (
	Initializing int64 = 0
	Errored      int64 = -1
	Active       int64 = 1
	Closing      int64 = 2
)

// NoIDAllocated is the channel status id used when no counter is allocated (e.g. IPC).
const NoIDAllocated int32 = -1

// ChannelOffset is the offset of the channel (int32 length + ASCII) in the key of a channel status counter.
const ChannelOffset int32 = 0

// Counter type ids for channel endpoint status counters.
const (
	SendChannelStatusTypeID    int32 = 6
	ReceiveChannelStatusTypeID int32 = 7
)

// MaxChannelLength is the maximum channel length stored in a channel status counter key.
const MaxChannelLength = counters.MaxKeyLength - (ChannelOffset + util.SizeOfInt32)

// String returns a printable name for a channel endpoint status value, as ChannelEndpointStatus.status does.
func String(status int64) string {
	switch status {
	case Initializing:
		return "INITIALIZING"
	case Errored:
		return "ERRORED"
	case Active:
		return "ACTIVE"
	case Closing:
		return "CLOSING"
	default:
		return "unknown id=" + itoa(status)
	}
}

func itoa(v int64) string {
	if v == 0 {
		return "0"
	}
	neg := v < 0
	if neg {
		v = -v
	}
	var b [20]byte
	i := len(b)
	for v > 0 {
		i--
		b[i] = byte('0' + v%10)
		v /= 10
	}
	if neg {
		i--
		b[i] = '-'
	}
	return string(b[i:])
}

// LocalSocketAddressStatus key layout (io.aeron.status.LocalSocketAddressStatus).
const (
	LocalSocketAddressStatusTypeID int32 = 14
	ChannelStatusIDOffset          int32 = 0
	LocalSocketAddressLengthOffset       = ChannelStatusIDOffset + util.SizeOfInt32
	LocalSocketAddressStringOffset       = LocalSocketAddressLengthOffset + util.SizeOfInt32
	MaxIPv6Length                  int32 = int32(len("[ffff:ffff:ffff:ffff:ffff:ffff:255.255.255.255]:65536"))
)

func scanLocalAddresses(reader *counters.Reader, channelStatus int64, channelStatusID int32, first bool) []string {
	if channelStatus != Active {
		return nil
	}
	var out []string
	reader.ScanForType(LocalSocketAddressStatusTypeID, func(counterID int32, key *atomic.Buffer) bool {
		if key.GetInt32(ChannelStatusIDOffset) == channelStatusID && reader.GetCounterValue(counterID) == Active {
			length := key.GetInt32(LocalSocketAddressLengthOffset)
			if length > 0 && length <= counters.MaxKeyLength-LocalSocketAddressStringOffset {
				out = append(out, string(key.GetBytesArray(LocalSocketAddressStringOffset, length)))
			}
			if first {
				return false
			}
		}
		return true
	})
	return out
}

// FindAddresses returns the bound local socket addresses for the channel whose status counter id is channelStatusID,
// or nil unless channelStatus is Active.
func FindAddresses(reader *counters.Reader, channelStatus int64, channelStatusID int32) []string {
	return scanLocalAddresses(reader, channelStatus, channelStatusID, false)
}

// FindAddress returns the first bound local socket address for the channel, or "" if none or not Active.
func FindAddress(reader *counters.Reader, channelStatus int64, channelStatusID int32) string {
	if a := scanLocalAddresses(reader, channelStatus, channelStatusID, true); len(a) > 0 {
		return a[0]
	}
	return ""
}
