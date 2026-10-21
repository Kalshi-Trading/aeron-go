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

import (
	"testing"

	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/protocol"
	"github.com/stretchr/testify/assert"
)

// Ported from HeaderTest.
func TestHeaderConstructor(t *testing.T) {
	ctx := "ctx"
	h := NewHeader(-1000, 20, ctx)
	assert.Equal(t, int32(-1000), h.InitialTermId())
	assert.Equal(t, int32(20), h.PositionBitsToShift())
	assert.Equal(t, ctx, h.Context())
	assert.Equal(t, NullValue, h.FragmentedFrameLength())
	h.SetInitialTermID(5).SetPositionBitsToShift(16).SetContext(nil)
	assert.Equal(t, int32(5), h.InitialTermId())
	assert.Equal(t, int32(16), h.PositionBitsToShift())
	assert.Nil(t, h.Context())
}

func TestHeaderPositionIsEndOfMessageInLog(t *testing.T) {
	cases := []struct {
		initialTermID, shift, frameLength, termOffset, termID int32
		expected                                              int64
	}{
		{0, 2, 100, 1024, 5, 1172},
		{42, 16, 13, 4096, 46, 266272},
		{1, 30, 1024, 1073741824, 111, 119185343488},
	}
	for _, c := range cases {
		buf := atomic.MakeBuffer(make([]byte, 64))
		var d protocol.DataHeaderFlyweight
		d.Wrap(buf, 16)
		d.SetFrameLength(c.frameLength)
		d.SetTermID(c.termID)
		d.SetTermOffset(c.termOffset)

		h := NewHeader(c.initialTermID, c.shift, nil)
		h.SetBuffer(buf).SetOffset(16)
		assert.Equal(t, c.expected, h.Position())
		assert.Equal(t, NullValue, h.FragmentedFrameLength())
	}
}

func TestHeaderPositionUsesFragmentedFrameLength(t *testing.T) {
	buf := atomic.MakeBuffer(make([]byte, 64))
	var d protocol.DataHeaderFlyweight
	d.Wrap(buf, 0)
	d.SetFrameLength(1408)
	d.SetTermOffset(1024)
	d.SetTermID(3)
	h := NewHeader(1, 16, nil)
	h.SetBuffer(buf)
	h.SetFragmentedFrameLength(ComputeFragmentedFrameLength(5000, 1376))
	assert.Equal(t, int32(5152), h.FragmentedFrameLength())
	assert.Equal(t, int64(2)<<16+1024+5152, h.Position())
	assert.Equal(t, int32(1024+5152), h.NextTermOffset())
	h.SetFragmentedFrameLength(NullValue)
	assert.Equal(t, int64(2)<<16+1024+1408, h.Position())

	// zero value header behaves as unfragmented
	var zero Header
	zero.SetBuffer(buf).SetPositionBitsToShift(16).SetInitialTermID(1)
	assert.Equal(t, int64(2)<<16+1024+1408, zero.Position())
}

func TestHeaderReadsDataFromBuffer(t *testing.T) {
	buf := atomic.MakeBuffer(make([]byte, 256))
	var d protocol.DataHeaderFlyweight
	d.Wrap(buf, 103)
	d.SetFrameLength(2080)
	d.SetVersion(0x3)
	d.SetFlags(0x1A)
	d.SetHeaderType(0x6)
	d.SetTermOffset(-46234)
	d.SetSessionID(333)
	d.SetStreamID(5)
	d.SetTermID(-9)
	d.SetReservedValue(909090909090909)

	var h Header
	h.SetBuffer(buf).SetOffset(103)
	assert.Equal(t, int32(103), h.Offset())
	assert.Equal(t, int32(2080), h.FrameLength())
	assert.Equal(t, uint8(0x1A), h.Flags())
	assert.Equal(t, uint16(0x6), h.Type())
	assert.Equal(t, int32(-46234), h.TermOffset())
	assert.Equal(t, int32(333), h.SessionId())
	assert.Equal(t, int32(5), h.StreamId())
	assert.Equal(t, int32(-9), h.TermId())
	assert.Equal(t, int64(909090909090909), h.ReservedValue())
	assert.Same(t, h.Buffer(), h.Buffer())
}
