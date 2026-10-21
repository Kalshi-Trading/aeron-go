/*
Copyright 2026 Kalshi Trading

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package aeron

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAsyncRemoveSubscriptionWhilePending(t *testing.T) {
	tc := newTestConductor(t)
	regID, err := tc.AsyncAddSubscription(Channel, StreamId1)
	require.NoError(t, err)

	require.NoError(t, tc.AsyncRemoveSubscription(regID))
	require.Equal(t, 1, tc.proxy.called("RemoveSubscription"), "Java sends REMOVE_SUBSCRIPTION for a pending add")
	require.False(t, tc.IsCommandActive(regID))

	tc.OnSubscriptionReady(regID, 1)
	sub, err := tc.GetSubscription(regID)
	require.Nil(t, sub, "late ready response must not resurrect the subscription")
	require.True(t, errors.Is(err, ErrUnknownRegistration), "%v", err)
}

func TestAsyncRemoveExclusivePublicationWhilePending(t *testing.T) {
	tc := newTestConductor(t)
	regID, err := tc.AsyncAddExclusivePublication(Channel, StreamId1)
	require.NoError(t, err)

	require.NoError(t, tc.AsyncRemovePublication(regID))
	require.Equal(t, 1, tc.proxy.called("RemovePublication"))

	tc.OnNewExclusivePublication(StreamId1, SessionId1, 1, 2, "unused", regID, regID)
	pub, err := tc.GetExclusivePublication(regID)
	require.Nil(t, pub)
	require.True(t, errors.Is(err, ErrUnknownRegistration), "%v", err)
	require.Empty(t, tc.logBuffersByID, "late ready response must not map log buffers")
}

func TestAsyncRemoveAfterFailedAddSendsNothing(t *testing.T) {
	tc := newTestConductor(t)
	regID, err := tc.AsyncAddSubscription(Channel, StreamId1)
	require.NoError(t, err)
	tc.OnErrorResponse(regID, 3, "invalid channel")

	require.NoError(t, tc.AsyncRemoveSubscription(regID))
	require.Equal(t, 0, tc.proxy.called("RemoveSubscription"))
	_, err = tc.GetSubscription(regID)
	require.True(t, errors.Is(err, ErrUnknownRegistration), "uncollected error is discarded: %v", err)
}
