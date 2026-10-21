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
	"net"

	"github.com/lirm/aeron-go/aeron/atomic"
)

// Resolution entry layout, mirroring io.aeron.protocol.ResolutionEntryFlyweight. Entries follow the common header
// of a HdrTypeRes frame; field offsets are relative to the start of the entry.
const (
	ResTypeNameToIP4MD int8 = 0x01
	ResTypeNameToIP6MD int8 = 0x02

	AddressLengthIP4 int32 = 4
	AddressLengthIP6 int32 = 16

	ResSelfFlag uint8 = 0x80

	ResTypeFieldOffset  int32 = 0
	ResFlagsFieldOffset int32 = 1
	UDPPortFieldOffset  int32 = 2
	AgeInMsFieldOffset  int32 = 4
	AddressFieldOffset  int32 = 8
	NameFieldOffsetIP4  int32 = AddressFieldOffset + AddressLengthIP4
	NameFieldOffsetIP6  int32 = AddressFieldOffset + AddressLengthIP6

	MaxNameLength      int32 = 512
	MinIPv4FrameLength int32 = NameFieldOffsetIP4 + 2
	MinIPv6FrameLength int32 = NameFieldOffsetIP6 + 2
)

// ResolutionEntryFlyweight is the flyweight for a name resolution entry carried in a resolution frame.
// capacity bounds the entry for IsValid, like the capacity of the Java UnsafeBuffer the entry is wrapped in.
type ResolutionEntryFlyweight struct {
	buf      *atomic.Buffer
	offset   int32
	capacity int32
}

// Wrap the flyweight around length bytes of buf at offset.
func (f *ResolutionEntryFlyweight) Wrap(buf *atomic.Buffer, offset, length int32) *ResolutionEntryFlyweight {
	atomic.BoundsCheck(offset, length, buf.Capacity())
	f.buf, f.offset, f.capacity = buf, offset, length
	return f
}

// Capacity returns the length of the wrapped region.
func (f *ResolutionEntryFlyweight) Capacity() int32 { return f.capacity }

func (f *ResolutionEntryFlyweight) ResType() int8 {
	return int8(f.buf.GetUInt8(f.offset + ResTypeFieldOffset))
}
func (f *ResolutionEntryFlyweight) SetResType(v int8) *ResolutionEntryFlyweight {
	f.buf.PutInt8(f.offset+ResTypeFieldOffset, v)
	return f
}
func (f *ResolutionEntryFlyweight) Flags() uint8 {
	return f.buf.GetUInt8(f.offset + ResFlagsFieldOffset)
}
func (f *ResolutionEntryFlyweight) SetFlags(v uint8) *ResolutionEntryFlyweight {
	f.buf.PutUInt8(f.offset+ResFlagsFieldOffset, v)
	return f
}
func (f *ResolutionEntryFlyweight) UDPPort() uint16 {
	return f.buf.GetUInt16(f.offset + UDPPortFieldOffset)
}
func (f *ResolutionEntryFlyweight) SetUDPPort(v uint16) *ResolutionEntryFlyweight {
	f.buf.PutUInt16(f.offset+UDPPortFieldOffset, v)
	return f
}
func (f *ResolutionEntryFlyweight) AgeInMs() int32 {
	return f.buf.GetInt32(f.offset + AgeInMsFieldOffset)
}
func (f *ResolutionEntryFlyweight) SetAgeInMs(v int32) *ResolutionEntryFlyweight {
	f.buf.PutInt32(f.offset+AgeInMsFieldOffset, v)
	return f
}

// SetAddress stores address, which must be the length required by ResType.
func (f *ResolutionEntryFlyweight) SetAddress(address []byte) error {
	length, err := ResAddressLength(f.ResType())
	if err != nil {
		return err
	}
	if int32(len(address)) != length {
		return fmt.Errorf("invalid address length: %d", len(address))
	}
	f.buf.PutBytesSlice(f.offset+AddressFieldOffset, address)
	return nil
}

// Address returns a copy of the address bytes for ResType.
func (f *ResolutionEntryFlyweight) Address() ([]byte, error) {
	length, err := ResAddressLength(f.ResType())
	if err != nil {
		return nil, err
	}
	return f.buf.GetBytesArray(f.offset+AddressFieldOffset, length), nil
}

// AddressString renders the address, or "unknown RES_TYPE=n" like Java appendAddress.
func (f *ResolutionEntryFlyweight) AddressString() string {
	address, err := f.Address()
	if err != nil {
		return fmt.Sprintf("unknown RES_TYPE=%d", f.ResType())
	}
	if f.ResType() == ResTypeNameToIP6MD {
		return "[" + net.IP(address).String() + "]"
	}
	return net.IP(address).String()
}

// SetName stores the length prefixed name after the address for ResType.
func (f *ResolutionEntryFlyweight) SetName(name []byte) error {
	nameOffset, err := ResNameOffset(f.ResType())
	if err != nil {
		return err
	}
	f.buf.PutUInt16(f.offset+nameOffset, uint16(len(name)))
	if len(name) > 0 {
		f.buf.PutBytesSlice(f.offset+nameOffset+2, name)
	}
	return nil
}

// Name returns a copy of the name.
func (f *ResolutionEntryFlyweight) Name() ([]byte, error) {
	nameOffset, err := ResNameOffset(f.ResType())
	if err != nil {
		return nil, err
	}
	length := int32(int16(f.buf.GetUInt16(f.offset + nameOffset)))
	if length <= 0 {
		return []byte{}, nil
	}
	return f.buf.GetBytesArray(f.offset+nameOffset+2, length), nil
}

// EntryLength returns the 8 byte aligned length of this entry.
func (f *ResolutionEntryFlyweight) EntryLength() (int32, error) {
	nameOffset, err := ResNameOffset(f.ResType())
	if err != nil {
		return 0, err
	}
	return align8(nameOffset + 2 + int32(int16(f.buf.GetUInt16(f.offset+nameOffset)))), nil
}

// IsValid checks the name length against MaxNameLength and the wrapped capacity.
func (f *ResolutionEntryFlyweight) IsValid() bool {
	capacity := f.capacity
	if capacity >= MinIPv4FrameLength {
		switch resType := f.ResType(); {
		case resType == ResTypeNameToIP4MD:
			nameLength := int32(int16(f.buf.GetUInt16(f.offset + NameFieldOffsetIP4)))
			return nameLength > 0 && nameLength <= MaxNameLength && capacity-MinIPv4FrameLength >= nameLength
		case resType == ResTypeNameToIP6MD && capacity >= MinIPv6FrameLength:
			nameLength := int32(int16(f.buf.GetUInt16(f.offset + NameFieldOffsetIP6)))
			return nameLength > 0 && nameLength <= MaxNameLength && capacity-MinIPv6FrameLength >= nameLength
		}
	}
	return false
}

// ResNameOffset returns the offset of the name field for resType.
func ResNameOffset(resType int8) (int32, error) {
	switch resType {
	case ResTypeNameToIP4MD:
		return NameFieldOffsetIP4, nil
	case ResTypeNameToIP6MD:
		return NameFieldOffsetIP6, nil
	}
	return 0, fmt.Errorf("unknown RES_TYPE=%d", resType)
}

// ResAddressLength returns the address length for resType.
func ResAddressLength(resType int8) (int32, error) {
	switch resType {
	case ResTypeNameToIP4MD:
		return AddressLengthIP4, nil
	case ResTypeNameToIP6MD:
		return AddressLengthIP6, nil
	}
	return 0, fmt.Errorf("unknown RES_TYPE=%d", resType)
}

// ResEntryLengthRequired returns the aligned entry length needed for a name of nameLength bytes.
func ResEntryLengthRequired(resType int8, nameLength int32) (int32, error) {
	nameOffset, err := ResNameOffset(resType)
	if err != nil {
		return 0, err
	}
	return align8(nameOffset + 2 + nameLength), nil
}

// IsAnyLocalAddress returns true if address is the all zeros IPv4 or IPv6 address.
func IsAnyLocalAddress(address []byte) bool {
	if int32(len(address)) != AddressLengthIP4 && int32(len(address)) != AddressLengthIP6 {
		return false
	}
	for _, b := range address {
		if b != 0 {
			return false
		}
	}
	return true
}

func align8(v int32) int32 { return (v + 7) &^ 7 }
