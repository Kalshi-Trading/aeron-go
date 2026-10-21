package conductorfaults

import (
	"os/exec"
	"strconv"
	"testing"
	"time"

	"github.com/lirm/aeron-go/aeron"
	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/stretchr/testify/require"
)

// iptables runs iptables via passwordless sudo, skipping the test when that is unavailable.
func iptables(t *testing.T, args ...string) {
	t.Helper()
	out, err := exec.Command("sudo", append([]string{"-n", "iptables"}, args...)...).CombinedOutput()
	if err != nil {
		t.Skipf("iptables unavailable (needs passwordless sudo and NET_ADMIN): %v: %s", err, out)
	}
}

// TestUDPPartitionWithIptables drops all UDP datagrams to a unicast subscription endpoint with iptables while an
// exclusive publication streams to it. Like Java, the image must go unavailable after the image liveness timeout,
// the publication must report not connected (without errors or client termination), and once the partition heals
// a new image of the same session must become available and deliver ordered, uncorrupted data.
func TestUDPPartitionWithIptables(t *testing.T) {
	setup(t)
	before := snapshot(t)
	c := connect(t, 5*time.Second)
	port := nextPort()
	channel, stream := udp(port), nextStream()
	rule := []string{"INPUT", "-i", "lo", "-p", "udp", "--dport", strconv.Itoa(port), "-j", "DROP"}
	iptables(t, "-L", "INPUT", "-n") // probe: skips when iptables cannot be used
	t.Cleanup(func() { _ = exec.Command("sudo", append([]string{"-n", "iptables", "-D"}, rule...)...).Run() })

	log := newSessionLog()
	log.rejoin = true
	sub, err := c.AddSubscriptionWithHandlers(channel, stream, log.onAvailable, log.imageEvent(log.unavailable))
	require.NoError(t, err)
	pub, err := c.AddExclusivePublication(channel, stream)
	require.NoError(t, err)
	sid := pub.SessionID()
	handler := log.onFragment
	msg := make([]byte, 200)
	buf := atomic.MakeBuffer(msg)
	seq := int64(0)
	stepUntil := func(timeout time.Duration, what string, cond func() bool) {
		awaitTrue(t, timeout, what, func() bool {
			encode(msg, seq)
			r := pub.Offer(buf, 0, int32(len(msg)), nil)
			require.False(t, r == aeron.PublicationClosed || r == aeron.MaxPositionExceeded, "offer %d", r)
			if r > 0 {
				seq++
			}
			for sub.Poll(handler, 64) > 0 {
			}
			return cond()
		})
	}
	stepUntil(10*time.Second, "initial data", func() bool { return log.received(sid) >= 1000 })

	iptables(t, append([]string{"-I"}, rule...)...)
	stepUntil(10*time.Second, "image unavailable during partition", func() bool { return log.is(log.unavailable, sid) })
	stepUntil(10*time.Second, "publication not connected", func() bool { return !pub.IsConnected() })
	require.False(t, c.IsClosed())

	log.mu.Lock()
	log.available = map[int32]bool{}
	log.mu.Unlock()
	iptables(t, append([]string{"-D"}, rule...)...)
	resumeAt := seq
	stepUntil(15*time.Second, "new image after heal", func() bool { return log.is(log.available, sid) })
	stepUntil(15*time.Second, "data after heal", func() bool { return log.received(sid) >= resumeAt+1000 })
	log.mu.Lock()
	require.Empty(t, log.problems)
	log.mu.Unlock()
	require.Empty(t, c.errs.all())
	require.NoError(t, pub.Close())
	require.NoError(t, sub.Close())
	require.NoError(t, c.Close())
	assertNoLeaks(t, before)
}
