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

// Package protocol contains the Aeron wire protocol frame definitions, mirroring io.aeron.protocol.
// All multi-byte fields are little endian.
package protocol

import (
	"encoding/binary"

	"github.com/lirm/aeron-go/aeron/atomic"
)

// Frame header types.
const (
	HdrTypePad      uint16 = 0x00
	HdrTypeData     uint16 = 0x01
	HdrTypeNak      uint16 = 0x02
	HdrTypeSM       uint16 = 0x03
	HdrTypeErr      uint16 = 0x04
	HdrTypeSetup    uint16 = 0x05
	HdrTypeRttm     uint16 = 0x06
	HdrTypeRes      uint16 = 0x07
	HdrTypeAtsData  uint16 = 0x08
	HdrTypeAtsSM    uint16 = 0x09
	HdrTypeAtsSetup uint16 = 0x0A
	HdrTypeRspSetup uint16 = 0x0B
	HdrTypeExt      uint16 = 0xFFFF
)

// Common header layout.
const (
	CurrentVersion         uint8 = 0x0
	FrameLengthFieldOffset int32 = 0
	VersionFieldOffset     int32 = 4
	FlagsFieldOffset       int32 = 5
	TypeFieldOffset        int32 = 6
	MinHeaderLength        int32 = TypeFieldOffset + 2
)

// HeaderFlyweight is the flyweight for the common 8 byte header shared by all Aeron frames.
type HeaderFlyweight struct {
	buf    *atomic.Buffer
	offset int32
}

// Wrap the flyweight around buf at offset.
func (h *HeaderFlyweight) Wrap(buf *atomic.Buffer, offset int32) *HeaderFlyweight {
	h.buf = buf
	h.offset = offset
	return h
}

// Buffer returns the wrapped buffer.
func (h *HeaderFlyweight) Buffer() *atomic.Buffer { return h.buf }

// Offset returns the offset in the wrapped buffer at which the frame begins.
func (h *HeaderFlyweight) Offset() int32 { return h.offset }

func (h *HeaderFlyweight) Version() uint8         { return h.buf.GetUInt8(h.offset + VersionFieldOffset) }
func (h *HeaderFlyweight) SetVersion(v uint8)     { h.buf.PutUInt8(h.offset+VersionFieldOffset, v) }
func (h *HeaderFlyweight) Flags() uint8           { return h.buf.GetUInt8(h.offset + FlagsFieldOffset) }
func (h *HeaderFlyweight) SetFlags(flags uint8)   { h.buf.PutUInt8(h.offset+FlagsFieldOffset, flags) }
func (h *HeaderFlyweight) HeaderType() uint16     { return h.buf.GetUInt16(h.offset + TypeFieldOffset) }
func (h *HeaderFlyweight) SetHeaderType(t uint16) { h.buf.PutUInt16(h.offset+TypeFieldOffset, t) }
func (h *HeaderFlyweight) FrameLength() int32 {
	return h.buf.GetInt32(h.offset + FrameLengthFieldOffset)
}
func (h *HeaderFlyweight) SetFrameLength(l int32)    { h.buf.PutInt32(h.offset+FrameLengthFieldOffset, l) }
func (h *HeaderFlyweight) getInt32(o int32) int32    { return h.buf.GetInt32(h.offset + o) }
func (h *HeaderFlyweight) putInt32(o int32, v int32) { h.buf.PutInt32(h.offset+o, v) }
func (h *HeaderFlyweight) getInt64(o int32) int64    { return h.buf.GetInt64(h.offset + o) }
func (h *HeaderFlyweight) putInt64(o int32, v int64) { h.buf.PutInt64(h.offset+o, v) }

// getInt64Unaligned reads a little endian int64 byte by byte, for fields not 8 byte aligned.
func (h *HeaderFlyweight) getInt64Unaligned(o int32) int64 {
	return int64(binary.LittleEndian.Uint64(h.buf.GetBytesArray(h.offset+o, 8)))
}

func (h *HeaderFlyweight) putInt64Unaligned(o int32, v int64) {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, uint64(v))
	h.buf.PutBytesArray(h.offset+o, &b, 0, 8)
}

// FlagsToChars renders flags as 8 '0'/'1' characters, most significant bit first.
func FlagsToChars(flags uint8) string {
	chars := []byte("00000000")
	for i := 0; i < 8; i++ {
		if flags&(0x80>>i) != 0 {
			chars[i] = '1'
		}
	}
	return string(chars)
}
