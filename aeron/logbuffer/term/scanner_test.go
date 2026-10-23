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

package term

import (
	"testing"

	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/logbuffer"
	"github.com/stretchr/testify/require"
)

func newTerm() *atomic.Buffer {
	return atomic.MakeBuffer(make([]byte, 64*1024))
}

func putFrame(buf *atomic.Buffer, offset int32, frameLength int32, typ uint16) int32 {
	buf.PutUInt16(offset+logbuffer.DataFrameHeader.TypeFieldOffset, typ)
	buf.PutInt32(offset, frameLength)
	return offset + ((frameLength + logbuffer.FrameAlignment - 1) &^ (logbuffer.FrameAlignment - 1))
}

func TestScanBlock(t *testing.T) {
	buf := newTerm()
	next := putFrame(buf, 0, 40, logbuffer.DataFrameHeader.TypeData)    // 64
	next = putFrame(buf, next, 100, logbuffer.DataFrameHeader.TypeData) // 128 -> 192
	pad := putFrame(buf, next, 32, logbuffer.DataFrameHeader.TypeData)  // 192 -> 224
	putFrame(buf, pad, buf.Capacity()-pad, logbuffer.DataFrameHeader.TypePad)

	require.Equal(t, int32(192), ScanBlock(buf, 0, 200), "stops before a frame crossing the limit")
	require.Equal(t, int32(64), ScanBlock(buf, 0, 64))
	require.Equal(t, int32(0), ScanBlock(buf, 0, 63))
	require.Equal(t, int32(224), ScanBlock(buf, 0, buf.Capacity()), "padding not first terminates block")
	require.Equal(t, buf.Capacity(), ScanBlock(buf, pad, buf.Capacity()), "leading padding is consumed")
	require.Equal(t, int32(1000), ScanBlock(newTerm(), 1000, 2000), "no data")
}

func TestScanForAvailability(t *testing.T) {
	buf := newTerm()
	next := putFrame(buf, 0, 40, logbuffer.DataFrameHeader.TypeData)
	next = putFrame(buf, next, 100, logbuffer.DataFrameHeader.TypeData)
	putFrame(buf, next, buf.Capacity()-next, logbuffer.DataFrameHeader.TypePad)

	r := ScanForAvailability(buf, 0, 1024)
	require.Equal(t, int32(192+32), Available(r), "padding header is counted as available")
	require.Equal(t, buf.Capacity()-192-32, Padding(r))

	r = ScanForAvailability(buf, 0, 100)
	require.Equal(t, int32(64), Available(r))
	require.Equal(t, int32(0), Padding(r))

	r = ScanForAvailability(buf, 0, 32)
	require.Equal(t, int32(-64), Available(r), "first frame larger than max length")

	require.Equal(t, int32(-5), Available(Pack(7, -5)))
	require.Equal(t, int32(7), Padding(Pack(7, -5)))
}

func TestControlledPollActionString(t *testing.T) {
	require.Equal(t, "ABORT", ControlledPollActionAbort.String())
	require.Equal(t, "BREAK", ControlledPollAction(ControlledPollActionBreak).String())
	require.Equal(t, "COMMIT", ControlledPollAction(ControlledPollActionCommit).String())
	require.Equal(t, "CONTINUE", ControlledPollAction(ControlledPollActionContinue).String())
	var a ControlledPollAction = ControlledPollActionBreak
	require.Equal(t, ControlledPollAction(ControlledPollActionBreak), a)
}
