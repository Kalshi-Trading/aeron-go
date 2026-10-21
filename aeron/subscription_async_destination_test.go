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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSubscriptionAsyncDestinations(t *testing.T) {
	const destination = "aeron:udp?endpoint=localhost:0"
	cc := NewMockReceivingConductor(t)
	sub := NewSubscription(cc, "aeron:udp?control-mode=manual", 7, 1001, -1, nil, nil)
	cc.On("AsyncAddRcvDestination", int64(7), destination).Return(int64(101), nil).Once()
	cc.On("AsyncRemoveRcvDestination", int64(7), destination).Return(int64(102), nil).Once()

	id, err := sub.AsyncAddDestination(destination)
	require.NoError(t, err)
	assert.Equal(t, int64(101), id)
	id, err = sub.AsyncRemoveDestination(destination)
	require.NoError(t, err)
	assert.Equal(t, int64(102), id)

	sub.isClosed.Set(true)
	_, err = sub.AsyncAddDestination(destination)
	assert.Error(t, err)
	_, err = sub.AsyncRemoveDestination(destination)
	assert.Error(t, err)
}
