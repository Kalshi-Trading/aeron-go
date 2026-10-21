package resourcelimits

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/lirm/aeron-go/aeron"
	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/counters"
	"github.com/lirm/aeron-go/aeron/logbuffer"
	"github.com/stretchr/testify/require"
)

// TestShmFullWhenDriverCreatesLogBuffer runs rlhelper inside rl-shm-driver's IPC namespace (a private 64MiB
// /dev/shm) and adds 16MiB-term exclusive publications until the driver cannot create another log buffer. Java
// 1.53.3 checks usable space first (FileStoreLogFactory.checkStorage) and rejects the add with
// ErrorCode.STORAGE_SPACE; the client must get a RegistrationError with that code, map nothing for the rejected
// add, stay usable, and add a working publication once a log has been freed. As in Java, where
// StorageSpaceException.isStorageSpaceError only matches StorageSpaceException and "no space" IOExceptions, a driver
// RegistrationError with code STORAGE_SPACE is not an IsStorageSpaceError.
func TestShmFullWhenDriverCreatesLogBuffer(t *testing.T) {
	requireStack(t, shmSvc)
	h := startHelperInContainer(t, shmContainer, "-mode=fill", "-dir="+dirShm,
		"-channel=aeron:ipc?term-length=16m", "-stream=9900", "-count=20")
	rejected := h.expect(t, "rejected", 60*time.Second)
	t.Log(rejected)
	require.Equal(t, "1", field(rejected, "added"), "a 64MiB /dev/shm holds exactly one 48MiB log")
	require.Equal(t, fmt.Sprint(int32(aeron.ErrorCodeStorageSpace)), field(rejected, "code"))
	require.Equal(t, "false", field(rejected, "storage"))
	require.Equal(t, "0", field(rejected, "leakedMaps"))
	require.Contains(t, rejected, "insufficient usable storage")
	recovered := h.expect(t, "recovered", 30*time.Second)
	require.Equal(t, "1", field(recovered, "received"))
	require.Equal(t, "0", field(h.expect(t, "closed", 10*time.Second), "maps"))
	select {
	case <-h.exited:
	case <-time.After(10 * time.Second):
		t.Fatal("rlhelper did not exit")
	}
	require.True(t, h.cmd.ProcessState.Success(), "rlhelper: %v", h.cmd.ProcessState)
}

// startHelperInContainer runs rlhelper in a throwaway container that shares the IPC namespace (and so /dev/shm)
// of container.
func startHelperInContainer(t *testing.T, container string, args ...string) *helperProc {
	t.Helper()
	cmdArgs := append([]string{"run", "--rm", "--network", "host", "--ipc", "container:" + container,
		"-u", fmt.Sprintf("%d:%d", os.Getuid(), os.Getgid()),
		"-v", filepath.Dir(helperPath) + ":/h:ro", "--entrypoint", "/h/rlhelper", "docker-archiving-driver"},
		args...)
	return startProcess(t, "docker", cmdArgs...)
}

// TestCountersBufferExhausted fills rl-tiny-driver's 1MiB counters buffer (8192 counters) with client counters.
// Java rejects the next counter (CountersManager: "unable to allocate counter, buffer is full") and any publication
// that needs counters with a GENERIC_ERROR RegistrationError; the client must survive, and once counters are freed
// (after the driver's free-to-reuse timeout) publications work again and every counter is returned on Close.
func TestCountersBufferExhausted(t *testing.T) {
	requireStack(t, tinySvc)
	base := baseline(t)
	eh := &errs{}
	monitor := connect(t, newContext(dirTiny, nil))
	a, err := aeron.Connect(newContext(dirTiny, eh))
	require.NoError(t, err)
	t.Cleanup(func() { _ = a.Close() })
	before := countAllocated(monitor)

	var owned []*aeron.Counter
	var fullErr error
	start := time.Now()
	for i := 0; i < 9000; i++ {
		c, err := a.AddCounterByLabel(1001, fmt.Sprintf("rl-%d", i))
		if err != nil {
			fullErr = err
			break
		}
		owned = append(owned, c)
	}
	require.Error(t, fullErr, "8192 counters must not fit alongside the driver's own")
	var regErr *aeron.RegistrationError
	require.True(t, errors.As(fullErr, &regErr), "%T %v", fullErr, fullErr)
	require.Equal(t, aeron.ErrorCodeGenericError, regErr.Code)
	require.Contains(t, fullErr.Error(), "unable to allocate counter, buffer is full")
	elapsed := time.Since(start)
	t.Logf("%d counters allocated in %v before: %v", len(owned), elapsed, fullErr)
	// Regression: a synchronous AddCounter must await the response like Java's ClientConductor.awaitResponse (1ms
	// awaiting idle), not poll with the 16ms conductor idle strategy (~17ms per add, >2 minutes for this loop).
	require.Less(t, elapsed/time.Duration(len(owned)+1), 5*time.Millisecond, "per synchronous AddCounter")

	_, err = a.AddPublication("aeron:udp?endpoint=localhost:27903", 9900)
	require.Error(t, err, "a network publication needs counters")
	require.True(t, errors.As(err, &regErr))
	require.Equal(t, aeron.ErrorCodeGenericError, regErr.Code)
	require.False(t, a.IsClosed())

	for _, c := range owned[:64] {
		require.NoError(t, c.Close())
	}
	var pub *aeron.Publication
	awaitTrue(t, "publication after freeing counters", 15*time.Second, func() bool {
		pub, err = a.AddPublication("aeron:udp?endpoint=localhost:27903", 9900)
		return err == nil
	})
	sub, err := a.AddSubscription("aeron:udp?endpoint=localhost:27903", 9900)
	require.NoError(t, err)
	awaitTrue(t, "connected", 10*time.Second, pub.IsConnected)
	buf := payload(64)
	awaitTrue(t, "offer", 5*time.Second, func() bool { return pub.Offer(buf, 0, 64, nil) > 0 })
	awaitTrue(t, "receive", 5*time.Second, func() bool {
		return sub.Poll(func(*atomic.Buffer, int32, int32, *logbuffer.Header) {}, 1) == 1
	})

	require.NoError(t, a.Close())
	awaitTrue(t, "driver frees every client counter", 15*time.Second, func() bool {
		return countAllocated(monitor) == before
	})
	require.Empty(t, eh.all())
	require.NoError(t, monitor.Close())
	base.check(t, dirTiny)
}

// countAllocated counts every allocated counter in the driver.
func countAllocated(a *aeron.Aeron) int {
	n := 0
	a.CounterReader().Scan(func(counters.Counter) { n++ })
	return n
}

// TestThousandPublicationsOneClient opens 1000 exclusive IPC publications (1000 log buffers) plus 200 concurrent
// ones on one client, with 50 subscriptions that therefore see 1000 images, sends one message on each and checks
// every message arrives exactly once. Closing the client must unmap every log, stop every goroutine and return
// every driver counter.
func TestThousandPublicationsOneClient(t *testing.T) {
	requireStack(t, driverSvc)
	base := baseline(t)
	monitor := connect(t, newContext(dir, nil))
	before := clientCounters(monitor)
	eh := &errs{}
	a, err := aeron.Connect(newContext(dir, eh))
	require.NoError(t, err)
	t.Cleanup(func() { _ = a.Close() })

	const channel = "aeron:ipc?term-length=64k"
	const streams = 50
	subs := make([]*aeron.Subscription, streams)
	for i := range subs {
		subs[i], err = a.AddSubscription(channel, int32(9900+i))
		require.NoError(t, err)
	}
	var excl []*aeron.ExclusivePublication
	for i := 0; i < 1000; i++ {
		p, err := a.AddExclusivePublication(channel, int32(9900+i%streams))
		require.NoError(t, err, "exclusive publication %d", i)
		excl = append(excl, p)
	}
	var conc []*aeron.Publication
	for i := 0; i < 200; i++ {
		p, err := a.AddPublication(channel, int32(9900+i%streams))
		require.NoError(t, err)
		conc = append(conc, p)
	}
	require.Greater(t, mappedFiles(t, dir), 1000)
	buf := payload(64)
	for i, p := range excl {
		buf.PutInt64(0, int64(i))
		awaitTrue(t, "offer", 10*time.Second, func() bool { return p.Offer(buf, 0, 64, nil) > 0 })
	}
	seen := make(map[int64]int)
	awaitTrue(t, "all 1000 messages", 30*time.Second, func() bool {
		for _, s := range subs {
			s.Poll(func(b *atomic.Buffer, o, l int32, h *logbuffer.Header) {
				if l == 64 {
					seen[b.GetInt64(o)]++
				}
			}, 100)
		}
		return len(seen) == 1000
	})
	for i, n := range seen {
		require.Equal(t, 1, n, "message %d delivered %d times", i, n)
	}
	for _, s := range subs {
		// 20 exclusive logs per stream plus the one log the 4 concurrent publications per stream share.
		require.Equal(t, 1000/streams+1, s.ImageCount())
	}
	require.NoError(t, within(t, "Close with 1200 publications", 20*time.Second, a.Close))
	awaitTrue(t, "driver frees every publication counter", 20*time.Second, func() bool {
		return clientCounters(monitor) == before
	})
	require.Empty(t, eh.all())
	require.NoError(t, monitor.Close())
	base.check(t, dir)
}

// TestClientWithThousandPublicationsKilled SIGKILLs an rlhelper process holding 1000 exclusive publications: the
// Java driver must time the client out (aeron.client.liveness.timeout=3s) and free all 1000 logs and their
// counters, and a new client must be able to reuse the streams.
func TestClientWithThousandPublicationsKilled(t *testing.T) {
	requireStack(t, driverSvc)
	monitor := connect(t, newContext(dir, nil))
	before := clientCounters(monitor)
	h := startHelper(t, "-mode=pubs", "-dir="+dir, "-channel=aeron:ipc?term-length=64k", "-stream=9900",
		"-count=1000")
	h.expect(t, "ready", 60*time.Second)
	require.GreaterOrEqual(t, clientCounters(monitor), before+2000, "publisher limit + position per log")
	h.signal(t, syscall.SIGKILL)
	awaitTrue(t, "driver times out the killed client and frees its 1000 publications", 30*time.Second, func() bool {
		return clientCounters(monitor) == before
	})
	logs, _ := filepath.Glob(filepath.Join(dir, "publications", "*.logbuffer"))
	awaitTrue(t, "log buffer files deleted", 30*time.Second, func() bool {
		logs, _ = filepath.Glob(filepath.Join(dir, "publications", "*.logbuffer"))
		return len(logs) == 0
	})
	pub, err := monitor.AddExclusivePublication("aeron:ipc?term-length=64k", 9900)
	require.NoError(t, err)
	sub, err := monitor.AddSubscription("aeron:ipc?term-length=64k", 9900)
	require.NoError(t, err)
	awaitTrue(t, "connected", 10*time.Second, pub.IsConnected)
	awaitTrue(t, "image", 10*time.Second, func() bool { return sub.ImageCount() == 1 })
	require.Greater(t, pub.Offer(payload(64), 0, 64, nil), int64(0))
}
