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

package protocol

import (
	"fmt"

	"github.com/lirm/aeron-go/aeron/atomic"
)

// Data frame header layout and flags.
const (
	DataHeaderLength int32 = 32

	BeginFlag                  uint8 = 0x80
	EndFlag                    uint8 = 0x40
	BeginAndEndFlags           uint8 = BeginFlag | EndFlag
	EosFlag                    uint8 = 0x20
	BeginEndAndEosFlags        uint8 = BeginFlag | EndFlag | EosFlag
	RevokedFlag                uint8 = 0x10
	BeginEndEosAndRevokedFlags uint8 = BeginFlag | EndFlag | EosFlag | RevokedFlag
	DefaultReserveValue        int64 = 0
	DataTermOffsetFieldOffset  int32 = 8
	DataSessionIDFieldOffset   int32 = 12
	DataStreamIDFieldOffset    int32 = 16
	DataTermIDFieldOffset      int32 = 20
	DataReservedValueOffset    int32 = 24
	DataOffset                 int32 = DataHeaderLength
)

// DataHeaderFlyweight is the flyweight for a data (or padding) frame header.
type DataHeaderFlyweight struct {
	HeaderFlyweight
}

// Wrap the flyweight around buf at offset.
func (d *DataHeaderFlyweight) Wrap(buf *atomic.Buffer, offset int32) *DataHeaderFlyweight {
	d.HeaderFlyweight.Wrap(buf, offset)
	return d
}

func (d *DataHeaderFlyweight) SessionID() int32         { return d.getInt32(DataSessionIDFieldOffset) }
func (d *DataHeaderFlyweight) SetSessionID(v int32)     { d.putInt32(DataSessionIDFieldOffset, v) }
func (d *DataHeaderFlyweight) StreamID() int32          { return d.getInt32(DataStreamIDFieldOffset) }
func (d *DataHeaderFlyweight) SetStreamID(v int32)      { d.putInt32(DataStreamIDFieldOffset, v) }
func (d *DataHeaderFlyweight) TermID() int32            { return d.getInt32(DataTermIDFieldOffset) }
func (d *DataHeaderFlyweight) SetTermID(v int32)        { d.putInt32(DataTermIDFieldOffset, v) }
func (d *DataHeaderFlyweight) TermOffset() int32        { return d.getInt32(DataTermOffsetFieldOffset) }
func (d *DataHeaderFlyweight) SetTermOffset(v int32)    { d.putInt32(DataTermOffsetFieldOffset, v) }
func (d *DataHeaderFlyweight) ReservedValue() int64     { return d.getInt64(DataReservedValueOffset) }
func (d *DataHeaderFlyweight) SetReservedValue(v int64) { d.putInt64(DataReservedValueOffset, v) }
func (d *DataHeaderFlyweight) DataOffset() int32        { return DataOffset }

// IsHeartbeat returns true if the packet of the given length is a data heartbeat (zero length data frame).
func IsHeartbeat(packet *atomic.Buffer, length int32) bool {
	return length == DataHeaderLength && packet.GetInt32(FrameLengthFieldOffset) == 0
}

// IsEndOfStream returns true if the EOS flag is set on the frame at the start of packet.
func IsEndOfStream(packet *atomic.Buffer) bool {
	return packet.GetUInt8(FlagsFieldOffset)&EosFlag != 0
}

// IsRevoked returns true if the REVOKED flag is set on the frame at the start of packet.
func IsRevoked(packet *atomic.Buffer) bool {
	return packet.GetUInt8(FlagsFieldOffset)&RevokedFlag != 0
}

// CreateDefaultHeader returns a new buffer holding a default data frame header for the given ids.
func CreateDefaultHeader(sessionID, streamID, termID int32) *atomic.Buffer {
	buf := atomic.MakeBuffer(make([]byte, DataHeaderLength))
	var d DataHeaderFlyweight
	d.Wrap(buf, 0)
	d.SetVersion(CurrentVersion)
	d.SetFlags(BeginAndEndFlags)
	d.SetHeaderType(HdrTypeData)
	d.SetSessionID(sessionID)
	d.SetStreamID(streamID)
	d.SetTermID(termID)
	d.SetReservedValue(DefaultReserveValue)
	return buf
}

func (d *DataHeaderFlyweight) String() string {
	return fmt.Sprintf("DATA Header{frame-length=%d version=%d flags=%s type=%d term-offset=%d session-id=%d "+
		"stream-id=%d term-id=%d reserved-value=%d}",
		d.FrameLength(), d.Version(), FlagsToChars(d.Flags()), d.HeaderType(), d.TermOffset(), d.SessionID(),
		d.StreamID(), d.TermID(), d.ReservedValue())
}
