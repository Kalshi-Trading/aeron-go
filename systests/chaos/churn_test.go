package chaos

import (
	"fmt"
	"runtime"
	"testing"
	"time"

	"github.com/lirm/aeron-go/aeron"
	"github.com/lirm/aeron-go/e2e"
	"github.com/stretchr/testify/require"
)

// TestChaosResourceChurn repeatedly creates clients, publications, subscriptions and archive sessions and closes them.
// Log buffers must be unmapped after the linger timeout, and no goroutines may leak once everything is closed.
func TestChaosResourceChurn(t *testing.T) {
	requireChaos(t)
	restartStack(t)
	baseGoroutines := runtime.NumGoroutine()
	require.Zero(t, mappedFiles(t, chaosDir))

	for c := 0; c < 3; c++ {
		a, err := aeron.Connect(newContext(chaosDir).ResourceLingerTimeout(100 * time.Millisecond))
		require.NoError(t, err)
		baseMapped := mappedFiles(t, chaosDir)
		for i := 0; i < 40; i++ {
			channel := "aeron:ipc?term-length=65536"
			if i%2 == 1 {
				channel = fmt.Sprintf("aeron:udp?endpoint=localhost:%d|term-length=65536", 24560+i%5)
			}
			streamID := int32(5700 + i)
			sub, err := a.AddSubscription(channel, streamID)
			require.NoError(t, err)
			var pub aeron.Publisher
			if i%3 == 0 {
				pub, err = a.AddPublication(channel, streamID)
			} else {
				pub, err = a.AddExclusivePublication(channel, streamID)
			}
			require.NoError(t, err)
			require.NoError(t, e2e.WaitConnected(pub, 5*time.Second))
			go func() { _ = e2e.Publish(pub, e2e.PublishOptions{Count: 20, Timeout: 5 * time.Second}) }()
			require.NoError(t, e2e.Consume(e2e.SubscriptionPoller(sub), e2e.NewSequenceChecker(0), 20, 5*time.Second))
			require.NoError(t, pub.Close())
			require.NoError(t, sub.Close())
		}
		awaitTrue(t, "log buffers unmapped after linger", 10*time.Second, func() bool {
			return mappedFiles(t, chaosDir) <= baseMapped
		})
		require.NoError(t, a.Close())
		require.Zero(t, mappedFiles(t, chaosDir), "client %d left mappings after close", c)
	}

	for i := 0; i < 10; i++ {
		arch, err := connectArchiveErr(chaosDir, chaosControlChannel, 10*time.Second)
		require.NoError(t, err)
		_, err = arch.ListRecordings(0, 10)
		require.NoError(t, err)
		require.NoError(t, arch.Close())
	}
	awaitTrue(t, "archive client mappings released", 5*time.Second, func() bool { return mappedFiles(t, chaosDir) == 0 })
	awaitGoroutines(t, baseGoroutines, 5*time.Second)
}
