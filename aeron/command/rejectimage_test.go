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

package command

import (
	"testing"

	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/stretchr/testify/require"
)

func TestRejectImageMessageLayout(t *testing.T) {
	reason := "bad data"
	buf := atomic.MakeBuffer(make([]byte, RejectImageMessageMinLength+len(reason)))
	var m RejectImageMessage
	m.Wrap(buf, 0)
	m.ClientID.Set(1)
	m.CorrelationID.Set(2)
	m.ImageCorrelationID.Set(3)
	m.Position.Set(4096)
	m.Reason.Set(reason)

	require.Equal(t, int32(0x10), RejectImage)
	require.Equal(t, RejectImageMessageMinLength+len(reason), m.Size())
	require.Equal(t, int64(1), buf.GetInt64(0))
	require.Equal(t, int64(2), buf.GetInt64(8))
	require.Equal(t, int64(3), buf.GetInt64(16))
	require.Equal(t, int64(4096), buf.GetInt64(24))
	require.Equal(t, int32(len(reason)), buf.GetInt32(32))
	require.Equal(t, reason, string(buf.GetBytesArray(36, int32(len(reason)))))
}
