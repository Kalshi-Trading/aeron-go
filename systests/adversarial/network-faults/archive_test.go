package networkfaults

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/lirm/aeron-go/aeron"
	"github.com/lirm/aeron-go/archive"
	"github.com/stretchr/testify/require"
)

const archiveTimeout = 2 * time.Second

// newArchive connects to the archive on driver a with a fixed response port so netem can target both directions.
func newArchive(dir string, responsePort int) (*archive.Archive, error) {
	options := archive.DefaultOptions()
	options.RequestChannel = archiveControl
	options.RequestStream = controlStream
	options.ResponseChannel = udp("localhost", responsePort, "")
	options.Timeout = archiveTimeout
	return archive.NewArchive(options, aeron.NewContext().AeronDir(dir).MediaDriverTimeout(10*time.Second))
}

// TestArchiveControlLoss: 25% loss on the archive control request and response channels. The control streams are
// reliable Aeron streams, so every request must still complete, in order, within the message timeout.
func TestArchiveControlLoss(t *testing.T) {
	requireStack(t)
	rp := port()
	arch, err := newArchive(dirA, rp)
	require.NoError(t, err)
	defer arch.Close()
	netem(t, toBoth("loss 25%", archiveCtlPort, rp))
	for i := 0; i < 200; i++ {
		start := time.Now()
		_, err := arch.ListRecordings(0, 10)
		require.NoError(t, err, "request %d", i)
		require.Less(t, time.Since(start), archiveTimeout+500*time.Millisecond)
		require.NoError(t, arch.KeepAlive())
	}
}

// TestArchiveControlPartition: an archive request during a full partition of the control channel must fail with a
// TimeoutError (Java TimeoutException) within the message timeout, not hang. If the partition heals before the
// response image times out, the next request on the same client succeeds, ignoring the late (retransmitted) response
// to the timed out one. If the partition outlives image liveness, the next request fails promptly with the Java
// "response channel from archive is not connected" ArchiveException, the client is DISCONNECTED, Close returns and
// a fresh client connects.
func TestArchiveControlPartition(t *testing.T) {
	requireStack(t)
	rp := port()
	arch, err := newArchive(dirA, rp)
	require.NoError(t, err)
	defer arch.Close()
	stream := streamID()
	recCh := udp("localhost", port(), "")
	_, err = arch.StartRecording(recCh, stream, false, true)
	require.NoError(t, err)
	t.Cleanup(func() { // the recording subscription belongs to the archive: don't leave it bound if the test fails
		if a, err := newArchive(dirA, port()); err == nil {
			_ = a.StopRecording(recCh, stream)
			_ = a.Close()
		}
	})

	netem(t, toBoth("loss 100%", archiveCtlPort, rp))
	start := time.Now()
	_, err = arch.ListRecordings(0, 10)
	elapsed := time.Since(start)
	var te *aeron.TimeoutError
	require.True(t, errors.As(err, &te), "want TimeoutError, got %T %v", err, err)
	require.GreaterOrEqual(t, elapsed, archiveTimeout-100*time.Millisecond)
	require.Less(t, elapsed, archiveTimeout+500*time.Millisecond, "request blocked past the message timeout")
	clearNetem(t)
	require.Less(t, time.Since(start), imageLiveness)
	// The timed out request may still reach the archive once the partition heals. As with Java 1.53.3, the archive
	// then rejects a new listing on the session with ACTIVE_LISTING until it has streamed the late listing, whose
	// descriptors the client must discard; a retry then succeeds.
	awaitTrue(t, "listing after heal", archiveTimeout+3*time.Second, func() bool {
		_, err = arch.ListRecordings(0, 10)
		var ae *archive.ArchiveException
		if errors.As(err, &ae) && ae.ErrorCode == archive.ErrorCodeActiveListing {
			time.Sleep(50 * time.Millisecond)
			return false
		}
		require.NoError(t, err)
		return true
	})
	require.NoError(t, arch.StopRecording(recCh, stream))

	netem(t, toBoth("loss 100%", archiveCtlPort, rp))
	time.Sleep(imageLiveness + 2*time.Second)
	start = time.Now()
	_, err = arch.ListRecordings(0, 10)
	var ae *archive.ArchiveException
	require.True(t, errors.As(err, &ae), "want ArchiveException, got %T %v", err, err)
	require.Less(t, time.Since(start), archiveTimeout+500*time.Millisecond)
	// Like Java 1.53.3, whichever side the driver declares dead first decides the error: ArchiveProxy.offer
	// reports a request publication that is no longer connected, AeronArchive.checkForDisconnect a response image
	// that has gone (and marks the client disconnected).
	switch msg := err.Error(); {
	case strings.Contains(msg, "connection to the archive is no longer available"):
	case strings.Contains(msg, "response channel from archive is not connected"):
		require.Equal(t, archive.ArchiveStateDisconnected, arch.State())
	default:
		t.Fatalf("unexpected archive error after partition: %v", err)
	}
	clearNetem(t)
	closed := make(chan error, 1)
	go func() { closed <- arch.Close() }()
	select {
	case <-closed:
	case <-time.After(archiveTimeout + 5*time.Second):
		t.Fatal("archive Close hung after partition")
	}
	arch2, err := newArchive(dirA, port())
	require.NoError(t, err)
	defer arch2.Close()
	_, err = arch2.ListRecordings(0, 10)
	require.NoError(t, err)
}
