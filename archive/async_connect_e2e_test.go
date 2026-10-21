package archive

// End-to-end connect lifecycle tests against the Java 1.53.3 archives in docker/:
//
//	docker compose -f docker/docker-compose.yml up -d --build archiving-driver
//	docker compose -f docker/auth.compose.yml up -d --build auth-archiving-driver
//	AERON_E2E=1 go test ./archive -run E2E -v -prefix /dev/shm/aeron-compose
//
// AERON_DIR (default /dev/shm/aeron-compose) and AERON_AUTH_DIR (default /dev/shm/aeron-auth) override the
// driver directories. Auth tests are skipped if the auth driver is not running.

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/lirm/aeron-go/aeron"
	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/idlestrategy"
	"github.com/lirm/aeron-go/archive/codecs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func e2eDir(t *testing.T, env, def string) string {
	if os.Getenv("AERON_E2E") != "1" {
		t.Skip("set AERON_E2E=1 to run against the docker archive")
	}
	dir := os.Getenv(env)
	if dir == "" {
		dir = def
	}
	if _, err := os.Stat(dir + "/cnc.dat"); err != nil {
		t.Skipf("no media driver at %s", dir)
	}
	return dir
}

func e2eContext(dir string) *aeron.Context {
	return aeron.NewContext().AeronDir(dir).MediaDriverTimeout(10 * time.Second)
}

func e2eOptions() *Options {
	o := DefaultOptions()
	o.IdleStrategy = idlestrategy.Sleeping{SleepFor: time.Millisecond}
	o.ClientName = "aeron-go-e2e"
	return o
}

func TestE2EConnectRecordAndClose(t *testing.T) {
	dir := e2eDir(t, "AERON_DIR", "/dev/shm/aeron-compose")
	opts := e2eOptions()
	var signals int
	var mu sync.Mutex
	opts.RecordingSignalConsumer = func(*codecs.RecordingSignalEvent) { mu.Lock(); signals++; mu.Unlock() }

	a, err := Connect(opts, e2eContext(dir))
	require.NoError(t, err)
	assert.Equal(t, ArchiveStateConnected, a.State())
	assert.Equal(t, int64(1), a.ArchiveID(), "docker archive is configured with aeron.archive.id=1")
	assert.GreaterOrEqual(t, a.ControlSessionID(), int64(0))
	assert.True(t, a.Options.OwnsAeronClient)
	assert.False(t, opts.IsConcluded(), "caller options untouched")

	// Real data: record a stream and check the archive saw it.
	channel := "aeron:ipc"
	stream := int32(7000 + time.Now().Nanosecond()%1000)
	subID, err := a.StartRecording(channel, stream, true, false)
	require.NoError(t, err)
	pub, err := a.AddPublication(channel, stream)
	require.NoError(t, err)
	payload := []byte("hello archive")
	buf := atomic.MakeBuffer(payload, len(payload))
	sent := 0
	for deadline := time.Now().Add(5 * time.Second); sent < 100 && time.Now().Before(deadline); {
		if pub.Offer(buf, 0, int32(len(payload)), nil) > 0 {
			sent++
		} else {
			time.Sleep(time.Millisecond)
		}
	}
	require.Equal(t, 100, sent)
	require.NoError(t, a.KeepAlive())

	var pos int64
	for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); time.Sleep(10 * time.Millisecond) {
		rds, err := a.ListRecordingsForUri(0, 100, channel, stream)
		if err == nil && len(rds) > 0 {
			if pos, err = a.GetRecordingPosition(rds[len(rds)-1].RecordingId); err == nil && pos >= pub.Position() {
				break
			}
		}
	}
	assert.Equal(t, pub.Position(), pos)
	require.NoError(t, a.StopRecordingBySubscriptionId(subID))
	_, _ = a.PollForErrorResponse()
	mu.Lock()
	assert.Greater(t, signals, 0, "RecordingSignalConsumer receives signals")
	mu.Unlock()

	require.NoError(t, a.Close())
	assert.True(t, a.IsClosed())
	assert.NoError(t, a.Close(), "idempotent")
}

func TestE2EAsyncConnectSteps(t *testing.T) {
	dir := e2eDir(t, "AERON_DIR", "/dev/shm/aeron-compose")
	invoked := 0
	opts := e2eOptions()
	opts.AgentInvoker = func() int { invoked++; return 0 }
	ac, err := NewAsyncConnect(opts, e2eContext(dir))
	require.NoError(t, err)
	seen := map[AsyncConnectState]bool{}
	var a *Archive
	for a == nil {
		seen[ac.State()] = true
		a, err = ac.Poll()
		require.NoError(t, err)
		time.Sleep(100 * time.Microsecond)
	}
	assert.Equal(t, AsyncConnectDone, ac.State())
	assert.Equal(t, 10, ac.Step())
	for _, s := range []AsyncConnectState{AsyncConnectAwaitSubscription, AsyncConnectAddPublication,
		AsyncConnectSendConnectRequest, AsyncConnectAwaitConnectResponse, AsyncConnectAwaitArchiveIDResponse} {
		assert.True(t, seen[s], "visited %s", s)
	}
	assert.Greater(t, invoked, 0)
	assert.NoError(t, ac.Close(), "no-op once done")
	again, err := ac.Poll()
	assert.NoError(t, err)
	assert.Same(t, a, again)
	require.NoError(t, a.Close())
}

func TestE2ESharedAeronClientNotClosed(t *testing.T) {
	dir := e2eDir(t, "AERON_DIR", "/dev/shm/aeron-compose")
	client, err := aeron.Connect(e2eContext(dir))
	require.NoError(t, err)
	defer client.Close()

	for i := 0; i < 2; i++ {
		opts := e2eOptions()
		opts.Aeron = client
		a, err := Connect(opts, nil)
		require.NoError(t, err)
		assert.False(t, a.Options.OwnsAeronClient)
		require.NoError(t, a.Close())
	}
	assert.False(t, client.IsClosed(), "shared client must stay open")
	sub, err := client.AddSubscription("aeron:ipc", 7999)
	require.NoError(t, err)
	require.NoError(t, sub.Close())
}

func TestE2EResponseChannelControlMode(t *testing.T) {
	dir := e2eDir(t, "AERON_DIR", "/dev/shm/aeron-compose")
	opts := e2eOptions()
	opts.ResponseChannel = "aeron:udp?control-mode=response|control=localhost:10002"
	a, err := Connect(opts, e2eContext(dir))
	require.NoError(t, err)
	assert.Contains(t, a.Options.RequestChannel, "response-correlation-id=")
	_, err = a.ListRecordings(0, 10)
	assert.NoError(t, err)
	require.NoError(t, a.Close())
}

func TestE2EConnectTimeout(t *testing.T) {
	dir := e2eDir(t, "AERON_DIR", "/dev/shm/aeron-compose")
	opts := e2eOptions()
	opts.RequestChannel = "aeron:udp?endpoint=localhost:8999"
	opts.Timeout = time.Second
	start := time.Now()
	_, err := Connect(opts, e2eContext(dir))
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrConnectTimeout), "%v", err)
	assert.Contains(t, err.Error(), "AWAIT_PUBLICATION_CONNECTED")
	assert.Less(t, time.Since(start), 5*time.Second)
}

func TestE2EAuthenticatedArchive(t *testing.T) {
	dir := e2eDir(t, "AERON_AUTH_DIR", "/dev/shm/aeron-auth")
	connect := func(creds CredentialsSupplier) (*Archive, error) {
		opts := e2eOptions()
		opts.RequestChannel = "aeron:udp?endpoint=localhost:8110"
		opts.CredentialsSupplier = creds
		return Connect(opts, e2eContext(dir))
	}

	t.Run("accepted", func(t *testing.T) {
		a, err := connect(StaticCredentialsSupplier{Credentials: []byte("admin:admin")})
		require.NoError(t, err)
		assert.Equal(t, int64(2), a.ArchiveID())
		_, err = a.ListRecordings(0, 10)
		assert.NoError(t, err)
		require.NoError(t, a.Close())
	})

	t.Run("challenge", func(t *testing.T) {
		var got []byte
		a, err := connect(CredentialsSupplierFuncs{
			Credentials: func() []byte { return []byte("admin:challenge") },
			Challenge: func(c []byte) []byte {
				got = append([]byte{}, c...)
				return []byte("admin:response")
			},
		})
		require.NoError(t, err)
		assert.True(t, bytes.Equal([]byte("challenge!"), got), "challenge %q", got)
		assert.Equal(t, int64(2), a.ArchiveID())
		require.NoError(t, a.KeepAlive())
		require.NoError(t, a.Close())
	})

	for name, creds := range map[string]CredentialsSupplier{
		"rejected":           StaticCredentialsSupplier{Credentials: []byte("bad")},
		"challenge rejected": StaticCredentialsSupplier{Credentials: []byte("admin:challenge"), ChallengeResponse: []byte("bad")},
		"no credentials":     NullCredentialsSupplier{},
	} {
		t.Run(name, func(t *testing.T) {
			a, err := connect(creds)
			require.Error(t, err)
			assert.Nil(t, a)
			var ce *ConnectError
			require.True(t, errors.As(err, &ce), "%v", err)
			assert.Equal(t, codecs.ControlResponseCode.ERROR, ce.Code)
			fmt.Println(name, "->", err)
		})
	}
}
