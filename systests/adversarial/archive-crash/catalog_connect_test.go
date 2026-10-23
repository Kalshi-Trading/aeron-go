package archivecrash

import (
	"errors"
	"fmt"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/lirm/aeron-go/aeron"
	"github.com/lirm/aeron-go/archive"
	"github.com/lirm/aeron-go/archive/codecs"
	"github.com/stretchr/testify/require"
)

// TestArchiveFaultDuringCatalogListing freezes (SIGSTOP) or kills (SIGKILL) the archive while the client is in
// the middle of a multi-term ListRecordings. The listing must fail within the message timeout of the last
// progress (Java: TimeoutException / ArchiveException), and after the restart the full catalog must be listed
// with every recording stopped at its recorded position.
func TestArchiveFaultDuringCatalogListing(t *testing.T) {
	for i, freeze := range []bool{true, false} {
		freeze, stream := freeze, int32(9340+i)
		name := map[bool]string{true: "sigstop", false: "sigkill"}[freeze]
		t.Run(name, func(t *testing.T) { faultDuringListing(t, freeze, stream) })
	}
}

func faultDuringListing(t *testing.T, freeze bool, stream int32) {
	freshStack(t)
	base := snapshot(t)
	arch := connectArchive(t, controlA, nil)
	// Long aliases make descriptors large so the listing spans several response terms and the archive is still
	// sending (flow controlled) when the fault hits.
	channel := "aeron:udp?endpoint=localhost:27370|alias=" + strings.Repeat("x", 400)
	_, err := arch.StartRecording(channel, stream, false, false)
	require.NoError(t, err)
	a := connect(t, nil)
	const recordings = 300
	stops := map[int64]int64{}
	for i := 0; i < recordings; i++ {
		pub, err := a.AddExclusivePublication(channel, stream)
		require.NoError(t, err)
		awaitTrue(t, "publication connected", 10*time.Second, pub.IsConnected)
		r := runPublisher(pub, 0, 3, 64, goFormat, false, 0)
		<-r.done
		require.NoError(t, r.err)
		stops[int64(pub.SessionID())] = pub.Position()
		awaitTrue(t, "recorded", 10*time.Second, func() bool {
			d, err := arch.ListRecordingsForUri(int64(i), 1, "27370", stream)
			return err == nil && len(d) == 1 && d[0].SessionId == pub.SessionID()
		})
		require.NoError(t, pub.Close())
	}
	require.NoError(t, arch.StopRecording(channel, stream))
	require.NoError(t, a.Close())

	var faultAt time.Time
	n := 0
	err = requireBounded(t, "ListRecordings across the fault", 30*time.Second, func() error {
		_, err := arch.ListRecordingsWithConsumer(0, recordings, func(*codecs.RecordingDescriptor) {
			n++
			if n == 5 {
				if freeze {
					pause(t, archiveA)
				} else {
					if out, err := exec.Command("docker", "kill", "-s", "KILL", containers[archiveA]).CombinedOutput(); err != nil {
						logs, _ := exec.Command("docker", "logs", "--tail", "30", containers[archiveA]).CombinedOutput()
						t.Fatalf("kill archive during listing: %v %s\narchive logs:\n%s", err, out, logs)
					}
				}
				faultAt = time.Now()
			}
		})
		return err
	})
	t.Logf("listing returned %v after %d descriptors, %v after the fault", err, n, time.Since(faultAt))
	requireArchiveFailure(t, err)
	require.Less(t, n, recordings, "listing completed although the archive was gone")
	require.LessOrEqual(t, time.Since(faultAt), msgTimeout+slack+500*time.Millisecond)
	if freeze {
		var te *aeron.TimeoutError
		require.True(t, errors.As(err, &te), "frozen archive: want TimeoutError, got %T: %v", err, err)
	}
	requireBounded(t, "stale Close", msgTimeout+slack, func() error { return arch.Close() })

	kill(t, archiveA)
	start(t, archiveA)
	arch2 := connectArchive(t, controlA, nil)
	descs, err := arch2.ListRecordings(0, recordings+10)
	require.NoError(t, err)
	require.Len(t, descs, recordings)
	for i, d := range descs {
		require.Equal(t, int64(i), d.RecordingId)
		require.Equal(t, stops[int64(d.SessionId)], d.StopPosition, "recording %d stop position", d.RecordingId)
	}
	require.NoError(t, arch2.Close())
	requireNoLeaks(t, base)
}

// asyncConnect polls an AsyncConnect to completion, returning the archive or the error and how long it took.
func asyncConnect(t *testing.T, control string, a *aeron.Aeron, onStep func(archive.AsyncConnectState)) (
	*archive.Archive, archive.AsyncConnectState, error) {
	t.Helper()
	o := archiveOptions(control)
	o.Aeron = a
	ac, err := archive.NewAsyncConnect(o, newContext(nil))
	require.NoError(t, err)
	last := ac.State()
	for {
		arch, err := ac.Poll()
		if arch != nil || err != nil {
			if err != nil {
				require.NoError(t, ac.Close())
			}
			return arch, last, err
		}
		if s := ac.State(); s != last {
			last = s
			if onStep != nil {
				onStep(s)
			}
		}
		time.Sleep(time.Millisecond)
	}
}

// TestArchiveFaultDuringAsyncConnect covers AsyncConnect against an archive that is down, frozen or killed at
// each connect step, and against a dying driver. Every attempt must end within the message timeout with
// ErrConnectTimeout (Java: TimeoutException "Archive connect timeout") or a disconnect error, release its
// publication and subscription, and a retry after the restart must connect.
func TestArchiveFaultDuringAsyncConnect(t *testing.T) {
	freshStack(t)
	base := snapshot(t)
	a := connect(t, nil)
	counterCount := func() int {
		n := 0
		for _, typeID := range []int32{1, 4, 6, 7, 12} {
			n += countCounters(a, typeID, "27300")
		}
		return n
	}
	idle := counterCount() // the archive's own control subscription

	t.Run("archive-down", func(t *testing.T) {
		kill(t, archiveA)
		_, step, err := requireBounded3(t, "AsyncConnect to a dead archive", msgTimeout+slack, func() (
			*archive.Archive, archive.AsyncConnectState, error) {
			return asyncConnect(t, controlA, a, nil)
		})
		require.ErrorIs(t, err, archive.ErrConnectTimeout)
		require.Equal(t, archive.AsyncConnectAwaitPublicationConnected, step)
		awaitTrue(t, "connect resources released", 10*time.Second, func() bool { return counterCount() == 0 })
		start(t, archiveA)
	})

	t.Run("archive-frozen", func(t *testing.T) {
		pause(t, archiveA)
		_, step, err := requireBounded3(t, "AsyncConnect to a frozen archive", msgTimeout+slack, func() (
			*archive.Archive, archive.AsyncConnectState, error) {
			return asyncConnect(t, controlA, a, nil)
		})
		require.ErrorIs(t, err, archive.ErrConnectTimeout)
		require.GreaterOrEqual(t, step, archive.AsyncConnectAwaitSubscriptionConnected)
		resume(t, archiveA)
		arch, _, err := asyncConnect(t, controlA, a, nil)
		require.NoError(t, err, "connect after the archive resumed")
		require.NoError(t, arch.Close())
	})

	for _, at := range []archive.AsyncConnectState{archive.AsyncConnectAwaitPublicationConnected,
		archive.AsyncConnectSendConnectRequest, archive.AsyncConnectAwaitSubscriptionConnected,
		archive.AsyncConnectAwaitConnectResponse} {
		at := at
		t.Run(fmt.Sprintf("kill-at-%s", at), func(t *testing.T) {
			pause(t, archiveA) // hold the state machine at the step until the kill lands
			killed := false
			arch, step, err := requireBounded3(t, "AsyncConnect across archive kill", msgTimeout+slack+3*time.Second,
				func() (*archive.Archive, archive.AsyncConnectState, error) {
					return asyncConnect(t, controlA, a, func(s archive.AsyncConnectState) {
						if s >= at && !killed {
							killed = true
							docker(t, "kill", "-s", "KILL", containers[archiveA])
						}
					})
				})
			t.Logf("killed at %s, attempt ended at %s: %v", at, step, err)
			if arch != nil {
				require.NoError(t, arch.Close())
			} else {
				require.ErrorIs(t, err, archive.ErrConnectTimeout)
			}
			kill(t, archiveA)
			start(t, archiveA)
			awaitTrue(t, "connect resources released", 10*time.Second, func() bool { return counterCount() <= idle })
			arch, _, err = asyncConnect(t, controlA, a, nil)
			require.NoError(t, err, "retry after restart")
			require.NoError(t, arch.Close())
		})
	}

	t.Run("driver-kill", func(t *testing.T) {
		sink := &errSink{}
		own := connect(t, sink)
		pause(t, archiveA)
		_, _, err := requireBounded3(t, "AsyncConnect across driver kill", msgTimeout+slack, func() (
			*archive.Archive, archive.AsyncConnectState, error) {
			return asyncConnect(t, controlA, own, func(s archive.AsyncConnectState) {
				if s == archive.AsyncConnectAwaitSubscriptionConnected {
					docker(t, "kill", "-s", "KILL", containers[driverSvc])
				}
			})
		})
		require.Error(t, err)
		require.NoError(t, own.Close())
		require.NoError(t, a.Close())
		docker(t, "kill", "-s", "KILL", containers[archiveA])
		start(t, driverSvc, archiveA, archiveB, archiveSml)
		a2 := connect(t, nil)
		arch, _, err := asyncConnect(t, controlA, a2, nil)
		require.NoError(t, err)
		require.NoError(t, arch.Close())
		require.NoError(t, a2.Close())
	})
	requireNoLeaks(t, base)
}

func requireBounded3[A, B any](t *testing.T, what string, max time.Duration, fn func() (A, B, error)) (A, B, error) {
	t.Helper()
	type res struct {
		a   A
		b   B
		err error
	}
	r := requireBounded(t, what, max, func() res { a, b, err := fn(); return res{a, b, err} })
	return r.a, r.b, r.err
}
