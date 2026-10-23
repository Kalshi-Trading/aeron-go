package archive

import (
	"errors"
	"testing"
	"time"

	"github.com/lirm/aeron-go/aeron"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// requireRegistrationGone waits until the shared client no longer knows the registration, i.e. the pending
// resource was removed rather than completed and leaked once the driver responded.
func requireRegistrationGone(t *testing.T, get func() (bool, error)) {
	t.Helper()
	time.Sleep(500 * time.Millisecond) // let the driver respond to the add (and the remove)
	found, err := get()
	assert.False(t, found, "resource leaked after AsyncConnect.Close")
	assert.True(t, errors.Is(err, aeron.ErrUnknownRegistration), "%v", err)
}

func TestE2EAsyncConnectCloseRemovesPendingSubscription(t *testing.T) {
	dir := e2eDir(t, "AERON_DIR", "/dev/shm/aeron-compose")
	client, err := aeron.Connect(e2eContext(dir))
	require.NoError(t, err)
	defer client.Close()

	opts := e2eOptions()
	opts.Aeron = client
	ac, err := NewAsyncConnect(opts, nil)
	require.NoError(t, err)
	_, err = ac.Poll()
	require.NoError(t, err)
	require.Equal(t, AsyncConnectAwaitSubscription, ac.State(), "subscription should still be pending")
	regID := ac.subscriptionRegID
	require.NotEqual(t, aeron.NullValue, regID)
	require.NoError(t, ac.Close())

	requireRegistrationGone(t, func() (bool, error) {
		sub, err := client.GetSubscription(regID)
		return sub != nil, err
	})
	assert.False(t, client.IsClosed())
}

func TestE2EAsyncConnectCloseRemovesPendingPublication(t *testing.T) {
	dir := e2eDir(t, "AERON_DIR", "/dev/shm/aeron-compose")
	client, err := aeron.Connect(e2eContext(dir))
	require.NoError(t, err)
	defer client.Close()

	opts := e2eOptions()
	opts.Aeron = client
	ac, err := NewAsyncConnect(opts, nil)
	require.NoError(t, err)
	for ac.State() == AsyncConnectAwaitSubscription {
		_, err = ac.Poll()
		require.NoError(t, err)
	}
	_, err = ac.Poll()
	require.NoError(t, err)
	require.Equal(t, AsyncConnectAddPublication, ac.State(), "publication should still be pending")
	subRegID := ac.subscription.RegistrationID()
	pubRegID := ac.publicationRegID
	require.NotEqual(t, aeron.NullValue, pubRegID)
	require.NoError(t, ac.Close())

	requireRegistrationGone(t, func() (bool, error) {
		pub, err := client.GetExclusivePublication(pubRegID)
		return pub != nil, err
	})
	requireRegistrationGone(t, func() (bool, error) {
		sub, err := client.GetSubscription(subRegID)
		return sub != nil, err
	})
}

func TestE2EConnectRejectedIsArchiveException(t *testing.T) {
	dir := e2eDir(t, "AERON_AUTH_DIR", "/dev/shm/aeron-auth")
	opts := e2eOptions()
	opts.RequestChannel = "aeron:udp?endpoint=localhost:8110"
	opts.CredentialsSupplier = StaticCredentialsSupplier{Credentials: []byte("bad")}
	_, err := Connect(opts, e2eContext(dir))
	require.Error(t, err)
	code, ok := ArchiveErrorCodeOf(err)
	require.True(t, ok, "connect rejection must be an *ArchiveException: %v", err)
	assert.Equal(t, ErrorCodeAuthenticationRejected, code)
	var ce *ConnectError
	assert.True(t, errors.As(err, &ce))
}

func TestE2EArchiveUseAfterClose(t *testing.T) {
	dir := e2eDir(t, "AERON_DIR", "/dev/shm/aeron-compose")
	for _, owns := range []bool{true, false} {
		opts := e2eOptions()
		var client *aeron.Aeron
		if !owns {
			var err error
			client, err = aeron.Connect(e2eContext(dir))
			require.NoError(t, err)
			opts.Aeron = client
		}
		a, err := Connect(opts, e2eContext(dir))
		require.NoError(t, err)
		require.NoError(t, a.Close())

		start := time.Now()
		_, err = a.ListRecordings(0, 10)
		assert.ErrorContains(t, err, "client is closed")
		_, err = a.StartRecording("aeron:ipc", 7777, false, false)
		assert.ErrorContains(t, err, "client is closed")
		assert.ErrorContains(t, a.KeepAlive(), "client is closed")
		_, err = a.GetRecordingPosition(0)
		assert.ErrorContains(t, err, "client is closed")
		assert.Less(t, time.Since(start), time.Second, "closed client must fail fast")
		if client != nil {
			require.NoError(t, client.Close())
		}
	}
}

func TestE2EConnectSessionIDFromDriver(t *testing.T) {
	dir := e2eDir(t, "AERON_DIR", "/dev/shm/aeron-compose")
	opts := e2eOptions()
	// Java AeronArchive.Context.conclude() always replaces the control session-id with aeron.nextSessionId().
	opts.RequestChannel = "aeron:udp?endpoint=localhost:8010|session-id=7"
	a, err := Connect(opts, e2eContext(dir))
	require.NoError(t, err)
	defer a.Close()
	req, err := aeron.ParseChannelUri(a.Options.RequestChannel)
	require.NoError(t, err)
	resp, err := aeron.ParseChannelUri(a.Options.ResponseChannel)
	require.NoError(t, err)
	assert.NotEqual(t, "7", req.Get(aeron.SessionIdParamName))
	assert.NotEmpty(t, req.Get(aeron.SessionIdParamName))
	assert.Equal(t, req.Get(aeron.SessionIdParamName), resp.Get(aeron.SessionIdParamName))
	_, err = a.ListRecordings(0, 1)
	assert.NoError(t, err)
}
