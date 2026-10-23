package resourcelimits

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/lirm/aeron-go/aeron"
	"github.com/lirm/aeron-go/archive"
	"github.com/lirm/aeron-go/archive/codecs"
	"github.com/stretchr/testify/require"
)

// TestArchiveDiskFull records an IPC stream into rl-archive, whose archive dir is a 24 MiB tmpfs, until the disk is
// full. As in Java 1.53.3, RecordingWriter turns the ENOSPC write failure into ArchiveException(STORAGE_SPACE): the
// recording stops (STOP signal), the control session gets an async ERROR response with code STORAGE_SPACE, the
// publisher is back pressured until StopRecording removes the recording subscription, and a new
// StartRecording is rejected with STORAGE_SPACE by the low storage check (aeron.archive.low.storage.space.threshold).
// After the recording is purged, recording resumes.
func TestArchiveDiskFull(t *testing.T) {
	const (
		channel = "aeron:ipc?term-length=1m"
		stream  = 9960
		size    = 8 * 1024
	)
	requireStack(t, driverSvc, archiveSvc)
	base := baseline(t)
	eh := &errs{}
	var mu sync.Mutex
	var signals []*codecs.RecordingSignalEvent
	opts := archive.DefaultOptions()
	opts.RequestChannel = archiveControl
	opts.RequestStream = archiveControlStream
	opts.ResponseChannel = "aeron:udp?endpoint=localhost:27912"
	opts.RecordingSignalConsumer = func(e *codecs.RecordingSignalEvent) {
		mu.Lock()
		defer mu.Unlock()
		signals = append(signals, e)
	}
	var arch *archive.Archive
	awaitTrue(t, "archive connected", 30*time.Second, func() bool {
		var err error
		arch, err = archive.NewArchive(opts, newContext(dir, eh))
		return err == nil
	})
	t.Cleanup(func() { _ = arch.Close() })
	hasSignal := func(s codecs.RecordingSignalEnum) bool {
		mu.Lock()
		defer mu.Unlock()
		for _, e := range signals {
			if e.Signal == s {
				return true
			}
		}
		return false
	}

	_, err := arch.StartRecording(channel, stream, true, false)
	require.NoError(t, err)
	pub, err := arch.AddExclusivePublication(channel, stream)
	require.NoError(t, err)
	awaitTrue(t, "recording started", 10*time.Second, func() bool {
		_, _ = arch.PollForErrorResponse()
		return pub.IsConnected() && hasSignal(codecs.RecordingSignal.START)
	})

	buf := payload(size)
	var asyncErr error
	var written int64
	deadline := time.Now().Add(60 * time.Second)
	for asyncErr == nil {
		require.True(t, time.Now().Before(deadline), "no STORAGE_SPACE error after %d bytes", written)
		require.Less(t, written, int64(64<<20), "archive accepted more than its disk")
		switch r := pub.Offer(buf, 0, size, nil); {
		case r > 0:
			written = r
		case r == aeron.BackPressured || r == aeron.AdminAction || r == aeron.NotConnected:
			time.Sleep(time.Millisecond)
		default:
			t.Fatalf("unexpected %s", aeron.PublicationErrorString(r))
		}
		_, asyncErr = arch.PollForErrorResponse()
	}
	t.Logf("after %d bytes: %v", written, asyncErr)
	var ae *archive.ArchiveException
	require.True(t, errors.As(asyncErr, &ae), "%T %v", asyncErr, asyncErr)
	require.Equal(t, archive.ErrorCodeStorageSpace, ae.ErrorCode)
	awaitTrue(t, "STOP signal", 10*time.Second, func() bool {
		_, _ = arch.PollForErrorResponse()
		return hasSignal(codecs.RecordingSignal.STOP)
	})
	// Like Java, the failed recording session stops consuming but the recording subscription stays until
	// StopRecording, so the publisher is back pressured, then NOT_CONNECTED once the subscription is removed.
	awaitTrue(t, "BACK_PRESSURED by the stopped recording", 10*time.Second, func() bool {
		r := pub.Offer(buf, 0, size, nil)
		require.True(t, r > 0 || r == aeron.BackPressured || r == aeron.AdminAction, aeron.PublicationErrorString(r))
		return r == aeron.BackPressured
	})
	require.NoError(t, arch.StopRecording(channel, stream))
	awaitTrue(t, "NOT_CONNECTED after StopRecording", 10*time.Second, func() bool {
		return pub.Offer(buf, 0, size, nil) == aeron.NotConnected
	})

	_, err = arch.StartRecording(channel, stream+1, true, false)
	require.True(t, errors.As(err, &ae), "%T %v", err, err)
	require.Equal(t, archive.ErrorCodeStorageSpace, ae.ErrorCode, "%v", err)

	recs, err := arch.ListRecordingsForUri(0, 100, "aeron:ipc", stream)
	require.NoError(t, err)
	require.Len(t, recs, 1)
	err = arch.PurgeRecording(recs[0].RecordingId)
	require.NoError(t, err)

	// As in Java, the archive answers the purge once it has queued the segment files for deletion and deletes them
	// over later duty cycles, so the space comes back shortly after PurgeRecording returns.
	awaitTrue(t, "StartRecording once the purged segments are deleted", 10*time.Second, func() bool {
		_, err = arch.StartRecording(channel, stream+1, true, false)
		if errors.As(err, &ae) && ae.ErrorCode == archive.ErrorCodeStorageSpace {
			return false
		}
		require.NoError(t, err)
		return true
	})
	pub2, err := arch.AddExclusivePublication(channel, stream+1)
	require.NoError(t, err)
	awaitTrue(t, "recording resumed", 10*time.Second, func() bool { return pub2.IsConnected() })
	var pos int64
	for i := 0; i < 100; {
		if r := pub2.Offer(buf, 0, size, nil); r > 0 {
			pos = r
			i++
		}
	}
	recs, err = arch.ListRecordingsForUri(0, 100, "aeron:ipc", stream+1)
	require.NoError(t, err)
	require.Len(t, recs, 1)
	awaitTrue(t, "recorded", 10*time.Second, func() bool {
		p, err := arch.GetRecordingPosition(recs[0].RecordingId)
		return err == nil && p == pos
	})
	require.NoError(t, arch.StopRecording(channel, stream+1))
	require.NoError(t, pub.Close())
	require.NoError(t, pub2.Close())
	require.NoError(t, arch.Close())
	for _, err := range eh.all() {
		require.True(t, errors.As(err, &ae), "unexpected error %v", err)
	}
	base.check(t, dir)
}
