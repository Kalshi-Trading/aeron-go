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

package conductore2e

import (
	"sync"
	"testing"
	"time"

	"github.com/lirm/aeron-go/aeron"
	ab "github.com/lirm/aeron-go/aeron/atomic"
	ctr "github.com/lirm/aeron-go/aeron/counters"
	"github.com/stretchr/testify/require"
)

const publisherLimitTypeID = 1

func TestAsyncRemoveInFlightPublicationFreesDriverResource(t *testing.T) {
	var mu sync.Mutex
	var errs []error
	a := connect(t, func(ctx *aeron.Context) {
		ctx.ErrorHandler(func(err error) { mu.Lock(); errs = append(errs, err); mu.Unlock() })
	})
	defer a.Close()

	regID, err := a.AsyncAddPublication("aeron:ipc?alias=conductor-review", 4711)
	require.NoError(t, err)
	require.NoError(t, a.AsyncRemovePublication(regID), "remove while the add is still in flight")
	require.False(t, a.IsCommandActive(regID))
	awaitTrue(t, "remove acknowledged by the driver", func() bool { return !a.HasActiveCommands() })

	limitCounter := func() int32 {
		return a.CounterReader().FindCounter(publisherLimitTypeID, func(key *ab.Buffer) bool {
			return key.GetInt64(0) == regID
		})
	}
	awaitTrue(t, "driver frees the publication", func() bool { return limitCounter() == ctr.NullCounterId })
	time.Sleep(50 * time.Millisecond)
	_, err = a.GetPublication(regID)
	require.ErrorIs(t, err, aeron.ErrUnknownRegistration)
	mu.Lock()
	defer mu.Unlock()
	require.Empty(t, errs)
}

func TestCloseFromAvailableImageHandler(t *testing.T) {
	a := connect(t)
	closeErr := make(chan error, 1)
	sub, err := a.AddSubscriptionWithHandlers("aeron:ipc", 4712,
		func(aeron.Image) { closeErr <- a.Close() }, nil)
	require.NoError(t, err)
	_, err = a.AddPublication("aeron:ipc", 4712)
	require.NoError(t, err)

	select {
	case err := <-closeErr:
		require.ErrorIs(t, err, aeron.ErrCloseFromCallback)
	case <-time.After(5 * time.Second):
		t.Fatal("Close from an image handler deadlocked")
	}
	awaitTrue(t, "client closed", a.IsClosed)
	awaitTrue(t, "subscription closed", sub.IsClosed)
	require.NoError(t, a.Close(), "like Java, a later Close waits for the async close and succeeds")
}
