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
	"testing"

	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/stretchr/testify/require"
)

func newResEntry(capacity int32) *ResolutionEntryFlyweight {
	backing := make([]byte, capacity+8)
	var f ResolutionEntryFlyweight
	return f.Wrap(atomic.MakeBuffer(backing), 8, capacity)
}

// Ports io.aeron.protocol.ResolutionEntryFlyweightTest.
func TestResolutionEntryInvalid(t *testing.T) {
	for _, c := range []int32{0, 1, MinIPv4FrameLength - 1} {
		require.False(t, newResEntry(c).IsValid(), "capacity %d", c)
	}
	for _, rt := range []int8{-1, 5, 127, -128} {
		require.False(t, newResEntry(MinIPv4FrameLength).SetResType(rt).IsValid(), "resType %d", rt)
	}
	require.False(t, newResEntry(MinIPv6FrameLength-1).SetResType(ResTypeNameToIP6MD).IsValid())

	for _, tc := range []struct {
		resType    int8
		nameOffset int32
		capacity   int32
	}{
		{ResTypeNameToIP4MD, NameFieldOffsetIP4, MinIPv4FrameLength},
		{ResTypeNameToIP6MD, NameFieldOffsetIP6, MinIPv6FrameLength},
	} {
		for _, l := range []int16{-32768, -3, 0, 32767, int16(MaxNameLength + 1), 5} {
			e := newResEntry(tc.capacity).SetResType(tc.resType)
			e.buf.PutUInt16(e.offset+tc.nameOffset, uint16(l))
			require.False(t, e.IsValid(), "resType %d name length %d", tc.resType, l)
		}
	}
}

func TestResolutionEntryValidAndRoundTrip(t *testing.T) {
	name := []byte("test name")
	for _, rt := range []int8{ResTypeNameToIP4MD, ResTypeNameToIP6MD} {
		e := newResEntry(MinIPv6FrameLength + int32(len(name)))
		e.SetResType(rt)
		require.NoError(t, e.SetName(name))
		require.True(t, e.IsValid())
		got, err := e.Name()
		require.NoError(t, err)
		require.Equal(t, name, got)
		nameOffset, _ := ResNameOffset(rt)
		length, err := e.EntryLength()
		require.NoError(t, err)
		require.Equal(t, align8(nameOffset+2+int32(len(name))), length)
		required, _ := ResEntryLengthRequired(rt, int32(len(name)))
		require.Equal(t, length, required)
	}

	e := newResEntry(64).SetResType(ResTypeNameToIP4MD).SetFlags(ResSelfFlag).SetUDPPort(40123).SetAgeInMs(77)
	require.Error(t, e.SetAddress([]byte{1, 2, 3}))
	require.NoError(t, e.SetAddress([]byte{10, 0, 0, 1}))
	require.Equal(t, "10.0.0.1", e.AddressString())
	require.Equal(t, uint8(ResSelfFlag), e.Flags())
	require.Equal(t, uint16(40123), e.UDPPort())
	require.Equal(t, int32(77), e.AgeInMs())
	require.Equal(t, []byte{0x01, 0x80, 0xBB, 0x9C, 77, 0, 0, 0, 10, 0, 0, 1}, e.buf.GetBytesArray(e.offset, 12))

	_, err := newResEntry(64).SetResType(9).Address()
	require.Error(t, err)
	require.Equal(t, "unknown RES_TYPE=9", newResEntry(64).SetResType(9).AddressString())
}

func TestResolutionEntryFormatsIPv6(t *testing.T) {
	e := newResEntry(MinIPv6FrameLength + 9).SetResType(ResTypeNameToIP6MD)
	require.NoError(t, e.SetName([]byte("test name")))
	require.NoError(t, e.SetAddress([]byte{0xfe, 0x80, 0, 0, 0, 0, 0, 0, 0x54, 0xd3, 0x41, 0x22, 0xe7, 0x38, 0xa8, 0x62}))
	require.Equal(t, "[fe80::54d3:4122:e738:a862]", e.AddressString())
}

func TestIsAnyLocalAddress(t *testing.T) {
	require.True(t, IsAnyLocalAddress(make([]byte, 4)))
	require.True(t, IsAnyLocalAddress(make([]byte, 16)))
	require.False(t, IsAnyLocalAddress([]byte{0, 0, 0, 1}))
	require.False(t, IsAnyLocalAddress(make([]byte, 5)))
}
