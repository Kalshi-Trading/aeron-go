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
	"bytes"
	"encoding/binary"
	"errors"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"sync"
	syncatomic "sync/atomic"
	"testing"
	"time"

	"github.com/lirm/aeron-go/aeron/util"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type countingIdler struct{ n syncatomic.Int64 }

func (c *countingIdler) Idle(int) { c.n.Add(1); time.Sleep(time.Millisecond) }

type countingInvoker struct{ n syncatomic.Int64 }

func (c *countingInvoker) Invoke() int { c.n.Add(1); time.Sleep(time.Millisecond); return 0 }

func TestAwaitResponseUsesAwaitingIdleStrategy(t *testing.T) {
	tc := newTestConductor(t)
	idler := &countingIdler{}
	tc.awaitingIdleStrategy = idler
	err := tc.awaitResponse(4711)
	var timeoutErr *DriverTimeoutError
	require.ErrorAs(t, err, &timeoutErr)
	assert.Positive(t, idler.n.Load(), "awaitResponse must idle with the context's awaiting idle strategy")
}

func TestAwaitResponseInvokesDriverAgentInvoker(t *testing.T) {
	tc := newTestConductor(t)
	idler := &countingIdler{}
	invoker := &countingInvoker{}
	tc.awaitingIdleStrategy = idler
	tc.driverAgentInvoker = invoker
	err := tc.awaitResponse(4711)
	var timeoutErr *DriverTimeoutError
	require.ErrorAs(t, err, &timeoutErr)
	assert.Positive(t, invoker.n.Load(), "embedded driver agent must be invoked while awaiting a response")
	assert.Zero(t, idler.n.Load(), "idle strategy is not used when a driver agent invoker is set")
}

func setCncPageSize(t *testing.T, path string, pageSize int32) {
	b, err := os.ReadFile(path)
	require.NoError(t, err)
	binary.LittleEndian.PutUint32(b[filePageSizeFieldOffset:], uint32(pageSize))
	require.NoError(t, os.WriteFile(path, b, 0o644))
}

func TestConnectRejectsInterServiceTimeoutNotAboveKeepAlive(t *testing.T) {
	dir := t.TempDir()
	writeFakeCnc(t, dir, int32(CncVersion), time.Now().UnixMilli()) // client liveness timeout 7s
	ctx := NewContext().AeronDir(dir).DriverTimeout(time.Second).KeepAliveInterval(7 * time.Second)
	a, err := Connect(ctx)
	require.Nil(t, a)
	var cfgErr *ConfigurationError
	require.ErrorAs(t, err, &cfgErr)
	assert.Contains(t, err.Error(), "interServiceTimeoutNs=7000000000 <= keepAliveIntervalNs=7000000000")

	// a failed Connect must release the context for another attempt
	_, err = Connect(ctx)
	require.ErrorAs(t, err, &cfgErr)
}

func TestConnectNonTimeoutErrorsAreNotDriverTimeouts(t *testing.T) {
	dir := t.TempDir()
	writeFakeCnc(t, dir, int32(util.SemanticVersionCompose(1, 0, 0)), time.Now().UnixMilli())
	_, err := Connect(NewContext().AeronDir(dir).DriverTimeout(time.Second))
	require.Error(t, err)
	var timeoutErr *DriverTimeoutError
	assert.False(t, errors.As(err, &timeoutErr), "version mismatch is not a driver timeout: %v", err)
	assert.Contains(t, err.Error(), "CnC version not compatible")
}

func TestConnectDefaultsZeroFilePageSize(t *testing.T) {
	dir := t.TempDir()
	path := writeFakeCnc(t, dir, int32(CncVersion), time.Now().UnixMilli())
	setCncPageSize(t, path, 0)
	ctx := NewContext().AeronDir(dir).DriverTimeout(time.Second).UseConductorAgentInvoker(true)
	a, err := Connect(ctx)
	require.NoError(t, err)
	defer a.Close()
	assert.Equal(t, int32(4096), ctx.FilePageSize())
}

func TestConnectRejectsContextInUse(t *testing.T) {
	dir := t.TempDir()
	writeFakeCnc(t, dir, int32(CncVersion), time.Now().UnixMilli())
	ctx := NewContext().AeronDir(dir).DriverTimeout(time.Second).UseConductorAgentInvoker(true)
	a, err := Connect(ctx)
	require.NoError(t, err)

	_, err = Connect(ctx)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "context is already in use")

	require.NoError(t, a.Close())
	a, err = Connect(ctx)
	require.NoError(t, err, "context is reusable once its client is closed")
	require.NoError(t, a.Close())
}

func TestConcludeIsAtomic(t *testing.T) {
	ctx := NewContext()
	var wg sync.WaitGroup
	var ok syncatomic.Int32
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if ctx.Conclude() == nil {
				ok.Add(1)
			}
		}()
	}
	wg.Wait()
	assert.Equal(t, int32(1), ok.Load())

	bad := NewContext().AeronDir("")
	require.Error(t, bad.Conclude())
	assert.False(t, bad.IsConcluded())
	require.NoError(t, bad.AeronDir(t.TempDir()).Conclude(), "a failed conclude may be retried")
}

func TestDriverActiveAndTerminationCheckMajorVersionOnly(t *testing.T) {
	dir := t.TempDir()
	olderMinor := int32(util.SemanticVersionCompose(0, 1, 0))
	writeFakeCnc(t, dir, olderMinor, time.Now().UnixMilli())

	active, err := IsDriverActive(dir, time.Second, nil)
	require.NoError(t, err)
	assert.True(t, active)

	ok, err := RequestDriverTermination(dir, nil)
	require.NoError(t, err)
	assert.True(t, ok)

	_, err = awaitCncFile(CncFileName(dir), 100*time.Millisecond)
	assert.ErrorContains(t, err, "insufficient", "connecting still requires the client's minor version")
}

func TestAwaitCncFileRetriesWhileFileUnreadable(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root ignores file permissions")
	}
	dir := t.TempDir()
	path := writeFakeCnc(t, dir, int32(CncVersion), time.Now().UnixMilli())
	require.NoError(t, os.Chmod(path, 0))
	go func() {
		time.Sleep(100 * time.Millisecond)
		_ = os.Chmod(path, 0o644)
	}()
	m, err := awaitCncFile(path, 2*time.Second)
	require.NoError(t, err)
	m.Close()

	require.NoError(t, os.Chmod(path, 0))
	_, err = awaitCncFile(path, 50*time.Millisecond)
	assert.ErrorIs(t, err, ErrDriverTimeout)
	_ = os.Chmod(path, 0o644)
}

func TestDurationConfigSaturates(t *testing.T) {
	d, err := parseDurationConfig("9223372036854775807s")
	require.NoError(t, err)
	assert.Equal(t, time.Duration(math.MaxInt64), d)
	d, err = parseDurationConfig("9223372036855ms")
	require.NoError(t, err)
	assert.Equal(t, time.Duration(math.MaxInt64), d)

	t.Setenv(DriverTimeoutEnv, "9223372036854775807")
	assert.Equal(t, time.Duration(math.MaxInt64), NewContext().GetDriverTimeout())
}

func TestEnableExperimentalFeatures(t *testing.T) {
	t.Setenv(EnableExperimentalFeaturesEnv, "")
	assert.False(t, NewContext().GetEnableExperimentalFeatures())
	t.Setenv(EnableExperimentalFeaturesEnv, "true")
	assert.True(t, NewContext().GetEnableExperimentalFeatures())
	assert.False(t, NewContext().EnableExperimentalFeatures(false).GetEnableExperimentalFeatures())
}

func TestAeronDirectoryIsCanonical(t *testing.T) {
	real := t.TempDir()
	link := filepath.Join(t.TempDir(), "link")
	require.NoError(t, os.Symlink(real, link))
	ctx := NewContext().AeronDir(link + "/./")
	assert.Empty(t, ctx.AeronDirectory())
	require.NoError(t, ctx.Conclude())
	want, err := filepath.EvalSymlinks(real)
	require.NoError(t, err)
	assert.Equal(t, want, ctx.AeronDirectory())

	missing := filepath.Join(real, "missing")
	ctx = NewContext().AeronDir(missing)
	require.NoError(t, ctx.Conclude())
	assert.Equal(t, filepath.Join(want, "missing"), ctx.AeronDirectory())
}

func TestDeleteAeronDirectory(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "aeron")
	require.NoError(t, os.MkdirAll(dir, 0o755))
	writeFakeCnc(t, dir, int32(CncVersion), time.Now().UnixMilli())
	require.NoError(t, NewContext().AeronDir(dir).DeleteAeronDirectory())
	_, err := os.Stat(dir)
	assert.True(t, os.IsNotExist(err))
	assert.Error(t, NewContext().AeronDir("").DeleteAeronDirectory())
}

func TestSaveErrorLog(t *testing.T) {
	dir := t.TempDir()
	var buf bytes.Buffer
	n, err := NewContext().AeronDir(dir).SaveErrorLog(&buf)
	require.NoError(t, err)
	assert.Zero(t, n, "no cnc file")

	writeFakeCnc(t, dir, int32(CncVersion), time.Now().UnixMilli())
	n, err = SaveErrorLog(dir, &buf)
	require.NoError(t, err)
	assert.Zero(t, n)
}

func TestGenerateRandomDirName(t *testing.T) {
	a, b := GenerateRandomDirName(), GenerateRandomDirName()
	assert.NotEqual(t, a, b)
	prefix := regexp.QuoteMeta(defaultAeronDirBase() + "-" + userName() + "-")
	assert.Regexp(t, "^"+prefix+"[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$", a)
}
