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

package logbuffer

import "github.com/lirm/aeron-go/aeron/protocol"

// DataFrameHeader holds the layout, frame types and flags of the data frame header. See package protocol for the
// full set of frame definitions.
var DataFrameHeader = struct {
	FrameLengthFieldOffset   int32
	VersionFieldOffset       int32
	FlagsFieldOffset         int32
	TypeFieldOffset          int32
	TermOffsetFieldOffset    int32
	SessionIDFieldOffset     int32
	StreamIDFieldOffset      int32
	TermIDFieldOffset        int32
	ReservedValueFieldOffset int32
	DataOffset               int32

	Length int32

	TypePad      uint16
	TypeData     uint16
	TypeNAK      uint16
	TypeSM       uint16
	TypeErr      uint16
	TypeSetup    uint16
	TypeRTTM     uint16
	TypeRes      uint16
	TypeATSData  uint16
	TypeATSSM    uint16
	TypeATSSetup uint16
	TypeRspSetup uint16
	TypeExt      uint16

	BeginFlag                  uint8
	EndFlag                    uint8
	BeginAndEndFlags           uint8
	EOSFlag                    uint8
	BeginEndAndEOSFlags        uint8
	RevokedFlag                uint8
	BeginEndEOSAndRevokedFlags uint8

	CurrentVersion int8
}{
	FrameLengthFieldOffset:   protocol.FrameLengthFieldOffset,
	VersionFieldOffset:       protocol.VersionFieldOffset,
	FlagsFieldOffset:         protocol.FlagsFieldOffset,
	TypeFieldOffset:          protocol.TypeFieldOffset,
	TermOffsetFieldOffset:    protocol.DataTermOffsetFieldOffset,
	SessionIDFieldOffset:     protocol.DataSessionIDFieldOffset,
	StreamIDFieldOffset:      protocol.DataStreamIDFieldOffset,
	TermIDFieldOffset:        protocol.DataTermIDFieldOffset,
	ReservedValueFieldOffset: protocol.DataReservedValueOffset,
	DataOffset:               protocol.DataOffset,

	Length: protocol.DataHeaderLength,

	TypePad:      protocol.HdrTypePad,
	TypeData:     protocol.HdrTypeData,
	TypeNAK:      protocol.HdrTypeNak,
	TypeSM:       protocol.HdrTypeSM,
	TypeErr:      protocol.HdrTypeErr,
	TypeSetup:    protocol.HdrTypeSetup,
	TypeRTTM:     protocol.HdrTypeRttm,
	TypeRes:      protocol.HdrTypeRes,
	TypeATSData:  protocol.HdrTypeAtsData,
	TypeATSSM:    protocol.HdrTypeAtsSM,
	TypeATSSetup: protocol.HdrTypeAtsSetup,
	TypeRspSetup: protocol.HdrTypeRspSetup,
	TypeExt:      protocol.HdrTypeExt,

	BeginFlag:                  protocol.BeginFlag,
	EndFlag:                    protocol.EndFlag,
	BeginAndEndFlags:           protocol.BeginAndEndFlags,
	EOSFlag:                    protocol.EosFlag,
	BeginEndAndEOSFlags:        protocol.BeginEndAndEosFlags,
	RevokedFlag:                protocol.RevokedFlag,
	BeginEndEOSAndRevokedFlags: protocol.BeginEndEosAndRevokedFlags,

	CurrentVersion: int8(protocol.CurrentVersion),
}
