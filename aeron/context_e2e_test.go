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
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	aatomic "github.com/lirm/aeron-go/aeron/atomic"
	ctr "github.com/lirm/aeron-go/aeron/counters"
	"github.com/lirm/aeron-go/aeron/logbuffer"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// E2E tests against a real Java media driver, e.g.:
//
//	docker compose -f docker/docker-compose.yml up -d --build archiving-driver
//	AERON_E2E=1 AERON_DIR=/dev/shm/aeron-compose go test ./aeron -run E2E -v
func requireE2E(t *testing.T) {
	if os.Getenv("AERON_E2E") != "1" {
		t.Skip("set AERON_E2E=1 (and AERON_DIR) to run against a live media driver")
	}
	require.NotEmpty(t, os.Getenv(AeronDirEnv), "AERON_DIR must point at the driver directory")
}

func TestContextE2EIsDriverActive(t *testing.T) {
	requireE2E(t)
	ctx := NewContext()
	assert.Equal(t, os.Getenv(AeronDirEnv), ctx.GetAeronDirectoryName())

	active, err := IsDriverActive(ctx.GetAeronDirectoryName(), ctx.GetDriverTimeout(), func(s string) { t.Log(s) })
	require.NoError(t, err)
	assert.True(t, active, "driver in AERON_DIR should be active")

	active, err = IsDriverActive(t.TempDir(), time.Second, nil)
	require.NoError(t, err)
	assert.False(t, active, "empty directory has no driver")

	active, err = NewContext().AeronDir("/nonexistent/aeron-dir").IsDriverActive(nil)
	require.NoError(t, err)
	assert.False(t, active)
}

func TestContextE2EConnectWithEnvAndClientName(t *testing.T) {
	requireE2E(t)
	name := fmt.Sprintf("go-ctx-e2e-%d", os.Getpid())
	var closed atomic.Int32
	ctx := NewContext().
		ClientName(name).
		PreTouchMappedMemory(true).
		CloseHandler(func() { closed.Add(1) })

	a, err := Connect(ctx)
	require.NoError(t, err)
	t.Log(ctx.String())
	assert.NotEqual(t, int64(NullValue), ctx.ClientID())
	assert.Equal(t, a.ClientID(), ctx.ClientID())
	assert.Greater(t, ctx.GetInterServiceTimeout(), time.Duration(0), "client liveness timeout from CnC")
	assert.Greater(t, ctx.FilePageSize(), int32(0), "file page size from CnC")
	assert.Error(t, ctx.Conclude(), "context is concluded by Connect")

	// The driver allocates the client heartbeat counter on the client's first command.
	pubSubRoundTrip(t, a, "aeron:ipc", 7101)

	label := ""
	deadline := time.Now().Add(5 * time.Second)
	for label == "" && time.Now().Before(deadline) {
		a.CounterReader().Scan(func(c ctr.Counter) {
			if c.TypeId == heartbeatTypeId && strings.Contains(c.Label, "name="+name+" ") {
				label = c.Label
			}
		})
		time.Sleep(50 * time.Millisecond)
	}
	require.NotEmpty(t, label, "client name should appear in heartbeat counter label")
	t.Logf("heartbeat counter: %q", label)
	assert.Contains(t, label, fmt.Sprintf("%d", ctx.ClientID()))

	require.NoError(t, a.Close())
	assert.Equal(t, int32(1), closed.Load(), "close handler called once")
}

func TestContextE2EConductorAgentInvoker(t *testing.T) {
	requireE2E(t)
	var closed atomic.Int32
	drv := &nopInvoker{}
	ctx := NewContext().
		ClientName("go-ctx-e2e-invoker").
		UseConductorAgentInvoker(true).
		DriverAgentInvoker(drv).
		CloseHandler(func() { closed.Add(1) })
	a, err := Connect(ctx)
	require.NoError(t, err)
	require.NotNil(t, a.ConductorAgentInvoker())

	pubSubRoundTrip(t, a, "aeron:ipc", 7102)
	assert.Greater(t, drv.n, 0, "driver agent invoker invoked on duty cycle")

	require.NoError(t, a.Close())
	assert.Equal(t, int32(1), closed.Load())
	assert.Equal(t, 0, a.ConductorAgentInvoker().Invoke(), "no work after close")
}

func pubSubRoundTrip(t *testing.T, a *Aeron, channel string, streamID int32) {
	t.Helper()
	sub, err := a.AddSubscription(channel, streamID)
	require.NoError(t, err)
	pub, err := a.AddPublication(channel, streamID)
	require.NoError(t, err)

	invoke := func() {
		if inv := a.ConductorAgentInvoker(); inv != nil {
			inv.Invoke()
		}
	}
	deadline := time.Now().Add(5 * time.Second)
	for !(pub.IsConnected() && sub.IsConnected()) && time.Now().Before(deadline) {
		invoke()
		time.Sleep(time.Millisecond)
	}
	require.True(t, pub.IsConnected(), "publication connected")

	msg := []byte("hello from context e2e")
	src := aatomic.MakeBuffer(msg)
	for pub.Offer(src, 0, int32(len(msg)), nil) < 0 && time.Now().Before(deadline) {
		invoke()
		time.Sleep(time.Millisecond)
	}
	got := ""
	for got == "" && time.Now().Before(deadline) {
		invoke()
		sub.Poll(func(buf *aatomic.Buffer, offset, length int32, _ *logbuffer.Header) {
			got = string(buf.GetBytesArray(offset, length))
		}, 10)
	}
	assert.Equal(t, string(msg), got)
	require.NoError(t, pub.Close())
	require.NoError(t, sub.Close())
}
