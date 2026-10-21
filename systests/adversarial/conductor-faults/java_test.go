package conductorfaults

import (
	"encoding/json"
	"strconv"
	"testing"
	"time"

	"github.com/lirm/aeron-go/aeron"
	"github.com/stretchr/testify/require"
)

func javaEvent(t *testing.T, p *proc, event string, timeout time.Duration) map[string]any {
	t.Helper()
	line := p.await(`{"event":"`+event+`"`, timeout)
	var m map[string]any
	require.NoError(t, json.Unmarshal([]byte(line[0]), &m))
	return m
}

// TestJavaPublisherContainerSIGKILL streams from a Java 1.53.3 publisher (InteropTool in its own container) to a
// Go subscription and SIGKILLs the Java client container mid stream (exclusive UDP and concurrent IPC). The Go
// side must see an ordered, uncorrupted prefix, fire the unavailable image handler, keep its client healthy, and
// receive a new Java publisher from sequence 0.
func TestJavaPublisherContainerSIGKILL(t *testing.T) {
	setup(t)
	for _, tc := range []struct {
		name      string
		udp       bool
		exclusive bool
	}{{"UDPExclusive", true, true}, {"IPCConcurrent", false, false}} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			before := snapshot(t)
			c := connect(t, 5*time.Second)
			channel, stream := "aeron:ipc", nextStream()
			if tc.udp {
				channel = udp(nextPort())
			}
			log := newSessionLog()
			sub, err := c.AddSubscriptionWithHandlers(channel, stream, log.onAvailable, log.imageEvent(log.unavailable))
			require.NoError(t, err)
			handler := aeron.NewFragmentAssembler(log.onFragment, 0).OnFragment
			args := []string{"dir=" + aeronDir, "channel=" + channel, "stream=" + strconv.Itoa(int(stream)),
				"exclusive=" + strconv.FormatBool(tc.exclusive), "count=100000000", "delayUs=20", "timeoutMs=120000"}

			j := startJava(t, "pub", args...)
			sid := int32(javaEvent(t, j, "ready", 60*time.Second)["sessionId"].(float64))
			pollUntil(t, sub, handler, 30*time.Second, "Java data", func() bool { return log.received(sid) >= 2000 })
			require.NoError(t, docker("kill", "-s", "KILL", j.name))
			j.awaitExit(t, 20*time.Second)
			pollUntil(t, sub, handler, 20*time.Second, "unavailable image", func() bool { return log.is(log.unavailable, sid) })

			j2 := startJava(t, "pub", args...)
			sid2 := int32(javaEvent(t, j2, "ready", 60*time.Second)["sessionId"].(float64))
			require.NotEqual(t, sid, sid2)
			pollUntil(t, sub, handler, 30*time.Second, "new Java publisher", func() bool { return log.received(sid2) >= 1000 })
			require.NoError(t, docker("kill", "-s", "KILL", j2.name))
			j2.awaitExit(t, 20*time.Second)

			log.mu.Lock()
			require.Empty(t, log.problems)
			log.mu.Unlock()
			require.Empty(t, c.errs.all())
			require.False(t, c.IsClosed())
			require.NoError(t, sub.Close())
			require.NoError(t, c.Close())
			assertNoLeaks(t, before)
		})
	}
}
