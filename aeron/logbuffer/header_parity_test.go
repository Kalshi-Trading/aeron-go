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
	"github.com/stretchr/testify/require"
)

func TestHeaderParityAccessors(t *testing.T) {
	buf := atomic.MakeBuffer(make([]byte, 1024))
	frameOffset := int32(128)
	buf.PutInt32(frameOffset, 100)
	buf.PutUInt8(frameOffset+DataFrameHeader.FlagsFieldOffset, 0xC0)
	buf.PutUInt16(frameOffset+DataFrameHeader.TypeFieldOffset, DataFrameHeader.TypeData)
	buf.PutInt32(frameOffset+DataFrameHeader.TermOffsetFieldOffset, 4096+frameOffset)
	buf.PutInt32(frameOffset+DataFrameHeader.SessionIDFieldOffset, 7)
	buf.PutInt32(frameOffset+DataFrameHeader.StreamIDFieldOffset, 8)
	buf.PutInt32(frameOffset+DataFrameHeader.TermIDFieldOffset, 12)
	buf.PutInt64(frameOffset+DataFrameHeader.ReservedValueFieldOffset, -99)

	ctx := "ctx"
	h := NewHeader(10, 16).SetContext(ctx)
	h.Wrap(buf.Ptr(), buf.Capacity())
	h.SetOffset(frameOffset)

	require.Equal(t, frameOffset, h.Offset())
	require.Equal(t, int32(100), h.FrameLength())
	require.Equal(t, uint8(0xC0), h.Flags())
	require.Equal(t, DataFrameHeader.TypeData, h.Type())
	require.Equal(t, 4096+frameOffset, h.TermOffset())
	require.Equal(t, 4096+frameOffset+128, h.NextTermOffset())
	require.Equal(t, int32(7), h.SessionId())
	require.Equal(t, int32(8), h.StreamId())
	require.Equal(t, int32(12), h.TermId())
	require.Equal(t, int64(-99), h.ReservedValue())
	require.Equal(t, int64(-99), h.GetReservedValue())
	require.Equal(t, int32(10), h.InitialTermId())
	require.Equal(t, int32(16), h.PositionBitsToShift())
	require.Equal(t, int64(2<<16)+int64(4096+frameOffset+128), h.Position())
	require.Equal(t, ctx, h.Context())
	require.Equal(t, buf.Ptr(), h.Buffer().Ptr())
	require.Equal(t, int32(-1), h.FragmentedFrameLength())

	h.SetFragmentedFrameLength(1000)
	require.Equal(t, int32(1000), h.FragmentedFrameLength())
	require.Equal(t, 4096+frameOffset+1024, h.NextTermOffset())
	h.SetFragmentedFrameLength(-1)
	require.Equal(t, 4096+frameOffset+128, h.NextTermOffset())
}
