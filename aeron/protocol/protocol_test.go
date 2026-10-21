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
	"strings"
	"testing"

	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/stretchr/testify/assert"
)

func TestHeaderTypesAndFlagsMatchJava(t *testing.T) {
	assert.Equal(t, []uint16{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 0xFFFF}, []uint16{HdrTypePad, HdrTypeData,
		HdrTypeNak, HdrTypeSM, HdrTypeErr, HdrTypeSetup, HdrTypeRttm, HdrTypeRes, HdrTypeAtsData, HdrTypeAtsSM,
		HdrTypeAtsSetup, HdrTypeRspSetup, HdrTypeExt})
	assert.Equal(t, []uint8{0x80, 0x40, 0xC0, 0x20, 0xE0, 0x10, 0xF0}, []uint8{BeginFlag, EndFlag,
		BeginAndEndFlags, EosFlag, BeginEndAndEosFlags, RevokedFlag, BeginEndEosAndRevokedFlags})
	assert.Equal(t, []int32{8, 32, 36, 28, 40, 40, 20, 40, 1063}, []int32{MinHeaderLength, DataHeaderLength,
		SMHeaderLength, NakHeaderLength, SetupHeaderLength, RttmHeaderLength, RspSetupHeaderLength,
		ErrHeaderLength, MaxErrorFrameLength})
	assert.Equal(t, "11100000", FlagsToChars(BeginEndAndEosFlags))
	assert.Equal(t, "00010000", FlagsToChars(RevokedFlag))
}

func TestDataHeaderFlyweight(t *testing.T) {
	buf := CreateDefaultHeader(1, 2, 3)
	var d DataHeaderFlyweight
	d.Wrap(buf, 0)
	assert.Equal(t, int32(1), d.SessionID())
	assert.Equal(t, int32(2), d.StreamID())
	assert.Equal(t, int32(3), d.TermID())
	assert.Equal(t, BeginAndEndFlags, d.Flags())
	assert.Equal(t, HdrTypeData, d.HeaderType())
	assert.True(t, IsHeartbeat(buf, DataHeaderLength))
	d.SetFlags(BeginEndEosAndRevokedFlags)
	assert.True(t, IsEndOfStream(buf))
	assert.True(t, IsRevoked(buf))
	d.SetFrameLength(64)
	assert.False(t, IsHeartbeat(buf, DataHeaderLength))
	assert.Equal(t, int32(1), buf.GetInt32(DataSessionIDFieldOffset))
	assert.True(t, strings.HasPrefix(d.String(), "DATA Header{frame-length=64 version=0 flags=11110000 type=1"))
}

func TestStatusMessageFlyweight(t *testing.T) {
	buf := atomic.MakeBuffer(make([]byte, 64))
	var sm StatusMessageFlyweight
	sm.Wrap(buf, 0)
	sm.SetHeaderType(HdrTypeSM)
	sm.SetConsumptionTermOffset(64)
	sm.SetReceiverWindowLength(1024)
	sm.SetReceiverID(-123456789)
	sm.SetGroupTag(nil)
	assert.Equal(t, SMHeaderLength, sm.FrameLength())
	tag, err := sm.GroupTag()
	assert.NoError(t, err)
	assert.Equal(t, int64(0), tag)
	v := int64(0x0102030405060708)
	sm.SetGroupTag(&v)
	assert.Equal(t, SMHeaderLength+8, sm.FrameLength())
	tag, err = sm.GroupTag()
	assert.NoError(t, err)
	assert.Equal(t, v, tag)
	assert.Equal(t, int64(-123456789), sm.ReceiverID())
	assert.Equal(t, uint8(0x08), buf.GetUInt8(36))
	assert.True(t, sm.IsValid(65536))
	assert.False(t, sm.IsValid(1024))
	sm.SetFrameLength(SMHeaderLength + 9)
	_, err = sm.GroupTag()
	assert.Error(t, err)
}

func TestNakSetupRttmRspSetup(t *testing.T) {
	buf := atomic.MakeBuffer(make([]byte, 64))
	var nak NakFlyweight
	nak.Wrap(buf, 0)
	nak.SetTermOffset(64)
	nak.SetLength(128)
	nak.SetTermID(9)
	assert.Equal(t, int32(9), buf.GetInt32(16))
	assert.Equal(t, int32(128), buf.GetInt32(24))
	assert.True(t, nak.IsValid(65536))
	assert.False(t, nak.IsValid(128))

	var s SetupFlyweight
	s.Wrap(buf, 0)
	s.SetTermOffset(0)
	s.SetTermLength(65536)
	s.SetMtuLength(1408)
	s.SetTTL(3)
	assert.True(t, s.IsValid())
	assert.Equal(t, int32(3), buf.GetInt32(36))
	s.SetMtuLength(1409)
	assert.False(t, s.IsValid())

	var r RttMeasurementFlyweight
	r.Wrap(buf, 0)
	r.SetEchoTimestampNs(1)
	r.SetReceptionDelta(2)
	r.SetReceiverID(3)
	assert.Equal(t, []int64{1, 2, 3}, []int64{buf.GetInt64(16), buf.GetInt64(24), buf.GetInt64(32)})

	var rs ResponseSetupFlyweight
	rs.Wrap(buf, 0)
	rs.SetResponseSessionID(77)
	assert.Equal(t, int32(77), buf.GetInt32(16))
}

func TestErrorFlyweight(t *testing.T) {
	buf := atomic.MakeBuffer(make([]byte, MaxErrorFrameLength+8))
	var e ErrorFlyweight
	e.Wrap(buf, 0)
	e.SetHeaderType(HdrTypeErr)
	e.SetErrorCode(5)
	e.SetErrorMessage("bad")
	assert.Equal(t, ErrHeaderLength+3, e.FrameLength())
	assert.Equal(t, "bad", e.ErrorMessage())
	assert.True(t, e.IsValid())
	assert.False(t, e.HasGroupTag())
	g := int64(42)
	e.SetGroupTag(&g)
	assert.True(t, e.HasGroupTag())
	assert.Equal(t, g, e.GroupTag())
	e.SetGroupTag(nil)
	assert.False(t, e.HasGroupTag())
	e.SetErrorMessage(strings.Repeat("x", 2000))
	assert.Equal(t, MaxErrorFrameLength, e.FrameLength())
}
