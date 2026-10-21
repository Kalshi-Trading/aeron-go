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
	"encoding/binary"
	"errors"
	"github.com/lirm/aeron-go/aeron/command"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
	"unsafe"

	"github.com/lirm/aeron-go/aeron/atomic"
	ctr "github.com/lirm/aeron-go/aeron/counters"
	"github.com/lirm/aeron-go/aeron/util"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	fakeRingCapacity    = 1024
	fakeRingTrailer     = util.CacheLineLength * 12
	fakeToDriverLength  = fakeRingCapacity + fakeRingTrailer
	fakeToClientsLength = fakeRingCapacity + util.CacheLineLength*2
	fakeBufLength       = 1024
	fakeHeartbeatOffset = cncMetaDataLength + fakeRingCapacity + util.CacheLineLength*10
	fakeCorrelationOff  = cncMetaDataLength + fakeRingCapacity + util.CacheLineLength*8
)

// writeFakeCnc writes a CnC file in dir with the given version and to-driver consumer heartbeat (ms).
func writeFakeCnc(t *testing.T, dir string, version int32, heartbeatMs int64) string {
	t.Helper()
	size := cncMetaDataLength + fakeToDriverLength + fakeToClientsLength + 3*fakeBufLength
	b := make([]byte, size)
	le := binary.LittleEndian
	le.PutUint32(b[0:], uint32(version))
	le.PutUint32(b[4:], uint32(fakeToDriverLength))
	le.PutUint32(b[8:], uint32(fakeToClientsLength))
	le.PutUint32(b[12:], fakeBufLength)
	le.PutUint32(b[16:], fakeBufLength)
	le.PutUint32(b[20:], fakeBufLength)
	le.PutUint64(b[24:], uint64(7*time.Second))
	le.PutUint64(b[32:], uint64(time.Now().UnixMilli()))
	le.PutUint64(b[40:], 4242)
	le.PutUint32(b[filePageSizeFieldOffset:], 4096)
	le.PutUint64(b[fakeHeartbeatOffset:], uint64(heartbeatMs))
	le.PutUint64(b[fakeCorrelationOff:], 100)
	path := CncFileName(dir)
	require.NoError(t, os.WriteFile(path, b, 0o644))
	return path
}

func TestDefaultAeronDirectoryName(t *testing.T) {
	t.Setenv(AeronDirEnv, "/tmp/custom-aeron")
	assert.Equal(t, "/tmp/custom-aeron", DefaultAeronDirectoryName())
	assert.Equal(t, "/tmp/custom-aeron", NewContext().GetAeronDirectoryName())
	assert.Equal(t, "/tmp/custom-aeron/cnc.dat", NewContext().CncFileName())

	t.Setenv(AeronDirEnv, "")
	dir := DefaultAeronDirectoryName()
	assert.True(t, strings.HasSuffix(dir, "aeron-"+userName()), dir)
	if fi, err := os.Stat("/dev/shm"); err == nil && fi.IsDir() {
		assert.Equal(t, "/dev/shm/aeron-"+userName(), dir)
	}
}

func TestCheckCncVersion(t *testing.T) {
	assert.NoError(t, CheckCncVersion(int32(CncVersion)))
	assert.NoError(t, CheckCncVersion(int32(util.SemanticVersionCompose(0, 2, 7))))
	assert.NoError(t, CheckCncVersion(int32(util.SemanticVersionCompose(0, 3, 0))))
	assert.Error(t, CheckCncVersion(int32(util.SemanticVersionCompose(0, 1, 0))))
	assert.Error(t, CheckCncVersion(int32(util.SemanticVersionCompose(1, 2, 0))))
	assert.Equal(t, ctr.CurrentCncVersion, int32(CncVersion))
}

func TestParseDurationConfig(t *testing.T) {
	cases := map[string]time.Duration{
		"100":    100,
		"5ns":    5,
		"7us":    7 * time.Microsecond,
		"16ms":   16 * time.Millisecond,
		"3s":     3 * time.Second,
		" 2 MS ": 2 * time.Millisecond,
	}
	for in, want := range cases {
		got, err := parseDurationConfig(in)
		require.NoError(t, err, in)
		assert.Equal(t, want, got, in)
	}
	for _, bad := range []string{"", "abc", "-1ms", "1h"} {
		_, err := parseDurationConfig(bad)
		assert.Error(t, err, bad)
	}
}

func TestNewContextDefaults(t *testing.T) {
	for _, env := range []string{DriverTimeoutEnv, ClientNameEnv, IdleSleepDurationEnv, ResourceLingerDurationEnv,
		CloseLingerDurationEnv, PreTouchMappedMemoryEnv} {
		t.Setenv(env, "")
	}
	ctx := NewContext()
	assert.Equal(t, DefaultDriverTimeout, ctx.GetDriverTimeout())
	assert.Equal(t, DefaultKeepAliveInterval, ctx.GetKeepAliveInterval())
	assert.Equal(t, 16*time.Millisecond, ctx.GetIdleSleepDuration())
	assert.Equal(t, 3*time.Second, ctx.GetResourceLingerDuration())
	assert.Equal(t, time.Duration(0), ctx.GetCloseLingerDuration())
	assert.False(t, ctx.GetPreTouchMappedMemory())
	assert.False(t, ctx.GetUseConductorAgentInvoker())
	assert.Equal(t, "", ctx.GetClientName())
	assert.Equal(t, int64(NullValue), ctx.ClientID())
	assert.Equal(t, time.Duration(0), ctx.GetInterServiceTimeout())
}

func TestNewContextFromEnv(t *testing.T) {
	t.Setenv(DriverTimeoutEnv, "2500")
	t.Setenv(ClientNameEnv, "env-client")
	t.Setenv(IdleSleepDurationEnv, "2ms")
	t.Setenv(ResourceLingerDurationEnv, "1s")
	t.Setenv(CloseLingerDurationEnv, "50ms")
	t.Setenv(PreTouchMappedMemoryEnv, "true")
	ctx := NewContext()
	assert.Equal(t, 2500*time.Millisecond, ctx.GetDriverTimeout())
	assert.Equal(t, "env-client", ctx.GetClientName())
	assert.Equal(t, 2*time.Millisecond, ctx.GetIdleSleepDuration())
	assert.Equal(t, time.Second, ctx.GetResourceLingerDuration())
	assert.Equal(t, 50*time.Millisecond, ctx.GetCloseLingerDuration())
	assert.True(t, ctx.GetPreTouchMappedMemory())
}

type nopInvoker struct{ n int }

func (i *nopInvoker) Invoke() int { i.n++; return 0 }

func TestContextConclude(t *testing.T) {
	ctx := NewContext()
	require.NoError(t, ctx.Conclude())
	assert.True(t, ctx.IsConcluded())
	assert.NotNil(t, ctx.GetIdleStrategy())
	assert.NotNil(t, ctx.GetAwaitingIdleStrategy())
	assert.NotNil(t, ctx.GetSubscriberErrorHandler())
	assert.Error(t, ctx.Conclude(), "conclude twice")

	assert.Error(t, NewContext().ClientName(strings.Repeat("x", MaxClientNameLength+1)).Conclude())
	assert.NoError(t, NewContext().ClientName(strings.Repeat("x", MaxClientNameLength)).Conclude())
	assert.Error(t, NewContext().DriverAgentInvoker(&nopInvoker{}).Conclude())
	assert.NoError(t, NewContext().DriverAgentInvoker(&nopInvoker{}).UseConductorAgentInvoker(true).Conclude())
	assert.Error(t, NewContext().IdleSleepDuration(2*time.Second).Conclude())
	assert.Error(t, NewContext().IdleSleepDuration(-1).Conclude())
	assert.Error(t, NewContext().DriverTimeout(0).Conclude())
	assert.Error(t, NewContext().AeronDir("").Conclude())

	var got error
	ctx = NewContext().ErrorHandler(func(err error) { got = err })
	require.NoError(t, ctx.Conclude())
	ctx.GetSubscriberErrorHandler()(errors.New("boom"))
	assert.EqualError(t, got, "boom")
}

func TestIsDriverActive(t *testing.T) {
	dir := t.TempDir()
	active, err := IsDriverActive(dir, time.Second, nil)
	assert.NoError(t, err)
	assert.False(t, active, "no cnc file")

	writeFakeCnc(t, dir, int32(CncVersion), time.Now().UnixMilli())
	var logs []string
	active, err = IsDriverActive(dir, time.Second, func(s string) { logs = append(logs, s) })
	assert.NoError(t, err)
	assert.True(t, active)
	assert.Len(t, logs, 2)

	writeFakeCnc(t, dir, int32(CncVersion), time.Now().Add(-5*time.Second).UnixMilli())
	active, err = IsDriverActive(dir, time.Second, nil)
	assert.NoError(t, err)
	assert.False(t, active, "stale heartbeat")

	writeFakeCnc(t, dir, int32(util.SemanticVersionCompose(1, 0, 0)), time.Now().UnixMilli())
	_, err = IsDriverActive(dir, time.Second, nil)
	assert.Error(t, err, "incompatible version")

	active, err = NewContext().AeronDir(dir).DriverTimeout(time.Second).IsDriverActive(nil)
	assert.Error(t, err)
	assert.False(t, active)
}

func TestAwaitCncFile(t *testing.T) {
	dir := t.TempDir()
	_, err := awaitCncFile(CncFileName(dir), 50*time.Millisecond)
	assert.ErrorIs(t, err, ErrDriverTimeout)

	writeFakeCnc(t, dir, int32(CncVersion), time.Now().UnixMilli())
	m, err := awaitCncFile(CncFileName(dir), time.Second)
	require.NoError(t, err)
	defer m.Close()
	assert.Equal(t, int32(4096), m.filePageSize())
	assert.Equal(t, int64(7*time.Second), m.meta.ClientLivenessTo.Get())
	assert.Equal(t, int64(4242), m.meta.DriverPid.Get())
}

func TestRequestDriverTermination(t *testing.T) {
	dir := t.TempDir()
	ok, err := RequestDriverTermination(dir, nil)
	assert.NoError(t, err)
	assert.False(t, ok, "no driver")

	path := writeFakeCnc(t, dir, int32(CncVersion), time.Now().UnixMilli())
	token := []byte("secret")
	ok, err = RequestDriverTermination(dir, token)
	require.NoError(t, err)
	assert.True(t, ok)

	b, err := os.ReadFile(path)
	require.NoError(t, err)
	le := binary.LittleEndian
	rec := b[cncMetaDataLength:]
	payloadLen := 8 + 8 + 4 + len(token)
	assert.Equal(t, uint32(8+payloadLen), le.Uint32(rec[0:]), "record length")
	assert.Equal(t, uint32(terminateDriverMsgTypeID), le.Uint32(rec[4:]), "msg type")
	assert.Equal(t, uint64(100), le.Uint64(rec[8:]), "client id = next correlation id")
	assert.Equal(t, int64(-1), int64(le.Uint64(rec[16:])), "correlation id")
	assert.Equal(t, uint32(len(token)), le.Uint32(rec[24:]))
	assert.Equal(t, token, rec[28:28+len(token)])
}

func TestDecodePublicationErrorFrame(t *testing.T) {
	length := 256
	raw := make([]byte, length)
	buf := atomic.MakeBuffer(unsafe.Pointer(&raw[0]), length)
	var fly command.PublicationErrorFrame
	fly.Wrap(buf, 0)
	fly.SetRegistrationID(11)
	fly.SetDestinationRegistrationID(12)
	fly.SetSessionID(13)
	fly.SetStreamID(14)
	fly.SetReceiverID(15)
	fly.SetGroupTag(16)
	if err := fly.SetSourceAddress(&net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 4040}); err != nil {
		t.Fatal(err)
	}
	fly.SetErrorCode(13)
	fly.SetErrorMessage("rejected")
	length = int(fly.Length())

	var f PublicationErrorFrame
	require.NoError(t, decodePublicationErrorFrame(buf, 0, int32(length), &f))
	assert.Equal(t, PublicationErrorFrame{
		RegistrationID: 11, DestinationRegistrationID: 12, SessionID: 13, StreamID: 14, ReceiverID: 15,
		GroupTag: 16, ErrorCode: 13, ErrorMessage: "rejected",
		SourceAddress: &net.UDPAddr{IP: net.IP{127, 0, 0, 1}, Port: 4040},
	}, *f.Clone())
	assert.Error(t, decodePublicationErrorFrame(buf, 0, 10, &f))
}

func TestAppendToCounterLabel(t *testing.T) {
	raw := make([]byte, ctr.MetadataLength*2)
	buf := atomic.MakeBuffer(unsafe.Pointer(&raw[0]), len(raw))
	rec := int32(ctr.MetadataLength)
	buf.PutInt32(rec, ctr.RecordAllocated)
	label := []byte("client-heartbeat: 5")
	buf.PutInt32(rec+ctr.LabelOffset, int32(len(label)))
	buf.PutBytesArray(rec+ctr.LabelOffset+4, &label, 0, int32(len(label)))

	n, err := appendToCounterLabel(buf, 1, heartbeatLabelSuffix("my-client"))
	require.NoError(t, err)
	assert.Greater(t, n, int32(0))
	l := buf.GetInt32(rec + ctr.LabelOffset)
	got := string(buf.GetBytesArray(rec+ctr.LabelOffset+4, l))
	assert.True(t, strings.HasPrefix(got, "client-heartbeat: 5 name=my-client version="), got)

	_, err = appendToCounterLabel(buf, 0, "x")
	assert.Error(t, err, "unallocated")
	_, err = appendToCounterLabel(buf, 5, "x")
	assert.Error(t, err, "out of range")

	n, err = appendToCounterLabel(buf, 1, strings.Repeat("y", 1000))
	require.NoError(t, err)
	assert.Equal(t, int32(ctr.MaxLabelLength), buf.GetInt32(rec+ctr.LabelOffset))
	_ = n
}

func TestCncFileName(t *testing.T) {
	assert.Equal(t, filepath.Join("/a/b", "cnc.dat"), CncFileName("/a/b"))
}
