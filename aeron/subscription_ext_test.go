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

package aeron

import (
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func mockImageWithID(t *testing.T, correlationID int64) *MockImage {
	img := NewMockImage(t)
	img.On("CorrelationID").Return(correlationID).Maybe()
	return img
}

func TestImageListCopyOnWrite(t *testing.T) {
	list := NewImageList()
	a, b, c := mockImageWithID(t, 1), mockImageWithID(t, 2), mockImageWithID(t, 3)
	list.add(a)
	list.add(b)
	snapshot := list.Get()
	list.add(c)
	require.Equal(t, []Image{a, b}, snapshot, "add must not write into a published array")

	snapshot = list.Get()
	require.Equal(t, Image(a), list.remove(1))
	require.Equal(t, []Image{a, b, c}, snapshot, "remove must not mutate a published array")
	require.Equal(t, []Image{b, c}, list.Get(), "order is preserved")
	require.Nil(t, list.remove(42))
}

func TestSubscriptionImageAccessors(t *testing.T) {
	cc := NewMockReceivingConductor(t)
	sub := NewSubscription(cc, "aeron:udp?endpoint=localhost:0", 5, 1001, 0, nil, nil)
	require.True(t, sub.HasNoImages())
	require.Nil(t, sub.ImageAtIndex(0))
	require.Equal(t, ChannelStatusInitializing, sub.ChannelStatus())

	a, b := mockImageWithID(t, 1), mockImageWithID(t, 2)
	a.On("SessionID").Return(int32(10)).Maybe()
	b.On("SessionID").Return(int32(20)).Maybe()
	sub.addImage(a)
	sub.addImage(b)
	require.Equal(t, 2, sub.ImageCount())
	require.Equal(t, Image(b), sub.ImageAtIndex(1))
	require.Equal(t, Image(b), sub.ImageBySessionID(20))
	images := sub.Images()
	images[0] = nil
	require.Equal(t, Image(a), sub.ImageAtIndex(0), "Images returns a copy")
	n := 0
	sub.ForEachImage(func(Image) { n++ })
	require.Equal(t, 2, n)
	require.Contains(t, sub.String(), "imageCount=2")

	require.Equal(t, Image(a), sub.removeImage(1))
	require.Equal(t, []Image{b}, sub.Images())
}

func TestSubscriptionRejectImage(t *testing.T) {
	cc := NewMockReceivingConductor(t)
	sub := NewSubscription(cc, "aeron:udp?endpoint=localhost:20121", 5, 1001, -1, nil, nil)
	require.Equal(t, ChannelStatusActive, sub.ChannelStatus())
	cc.On("rejectImage", int64(99), int64(4096), "bad").Return(nil).Once()
	require.NoError(t, sub.RejectImage(99, 4096, "bad"))

	spy := NewSubscription(cc, "aeron-spy:aeron:udp?endpoint=localhost:20121", 6, 1001, -1, nil, nil)
	require.ErrorIs(t, spy.RejectImage(99, 4096, "bad"), ErrSpyRejectImage)

	cc.On("releaseSubscription", sub).Run(func(args mock.Arguments) {
		args.Get(0).(*Subscription).isClosed.Set(true)
	}).Return(nil).Once()
	require.NoError(t, sub.Close())
	require.Error(t, sub.RejectImage(99, 4096, "bad"))
}

func TestSubscriptionAddRemoveRealImage(t *testing.T) {
	ti := newTestImage(t)
	cc := NewMockReceivingConductor(t)
	sub := NewSubscription(cc, "aeron:ipc", 11, 1001, -1, nil, nil)
	sub.addImage(ti.image)
	require.Same(t, sub, ti.Subscription())
	require.True(t, sub.IsConnected())

	cc.On("rejectImage", int64(99), int64(0), "why").Return(nil).Once()
	require.NoError(t, ti.Reject("why"))

	require.Equal(t, Image(ti.image), sub.removeImage(99))
	require.True(t, ti.IsClosed(), "removed images are marked closed before lingering")
	require.False(t, sub.IsConnected())
}
