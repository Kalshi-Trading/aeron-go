package interop

import (
	"fmt"
	"testing"
	"time"

	"github.com/lirm/aeron-go/aeron"
	"github.com/lirm/aeron-go/archive/replaymerge"
	"github.com/stretchr/testify/require"
)

// A Go publication on an MDC (dynamic) channel is recorded remotely, then a Java ReplayMerge and a Go ReplayMerge
// consume the same recording + live stream concurrently while the publisher keeps publishing. Both must merge and
// yield identical bytes (the full stream from position 0).
func TestReplayMergeJavaVsGo(t *testing.T) {
	requireE2E(t)
	const groupTag = 99901
	arch := connectGoArchive(t)
	stream := nextStream()
	ctrl, recPort := nextPort(), nextPort()
	pubCh := fmt.Sprintf("aeron:udp?control=localhost:%d|control-mode=dynamic|term-length=65536|fc=tagged,g:%d/1,t:5s",
		ctrl, groupTag)
	pub, err := arch.AddPublication(pubCh, stream)
	require.NoError(t, err)
	defer pub.Close()
	sessionID := pub.SessionID()

	recCh := fmt.Sprintf("aeron:udp?endpoint=localhost:%d|control=localhost:%d|session-id=%d|gtag=%d",
		recPort, ctrl, sessionID, groupTag)
	_, err = arch.StartRecording(recCh, stream, false, true)
	require.NoError(t, err)
	reader := arch.Aeron().CounterReader()
	counterID, recordingID := awaitRecordingCounter(t, reader, sessionID)
	require.NoError(t, awaitConnected(pub, 10*time.Second))

	const initial, total = 2000, 4000
	o := msgOpts{count: total, sizes: Sizes{64, 2000, 200, 6000}, timeout: 60 * time.Second}
	initialOpts := o
	initialOpts.count = initial
	_, err = goPublish(pub, initialOpts)
	require.NoError(t, err)
	awaitCounterValue(t, reader, counterID, pub.Position())

	liveDest := fmt.Sprintf("aeron:udp?endpoint=localhost:0|control=localhost:%d", ctrl)
	j := startJava(t, "replaymerge", append([]string{fmt.Sprintf("recordingId=%d", recordingID),
		fmt.Sprintf("sessionId=%d", sessionID), fmt.Sprintf("stream=%d", stream), "liveDestination=" + liveDest},
		o.javaArgs()...)...)
	j.waitFor("ready", 20*time.Second)

	sub, err := arch.AddSubscription(fmt.Sprintf("aeron:udp?control-mode=manual|session-id=%d", sessionID), stream)
	require.NoError(t, err)
	rm, err := replaymerge.NewReplayMerge(sub, arch, fmt.Sprintf("aeron:udp?session-id=%d", sessionID),
		"aeron:udp?endpoint=localhost:0", liveDest, recordingID, 0, 5000)
	require.NoError(t, err)
	defer rm.Close()

	pubDone := make(chan error, 1)
	go func() {
		rest := o
		rest.from = initial
		rest.delay = 200 * time.Microsecond
		_, err := goPublish(pub, rest)
		pubDone <- err
	}()

	v := NewVerifier(o.sizes, false)
	assembler := aeron.NewFragmentAssembler(v.OnFragment, 4096)
	goMerged := false
	deadline := time.Now().Add(o.timeout)
	for v.Count() < total && time.Now().Before(deadline) {
		_, err := rm.Poll(assembler.OnFragment, 10)
		require.NoError(t, err)
		require.False(t, rm.HasFailed(), "go replay merge failed")
		if !goMerged && rm.IsMerged() {
			goMerged = true
			t.Logf("go replay merge merged after %d messages", v.Count())
		}
	}
	require.NoError(t, <-pubDone)
	position := pub.Position()
	goResult := v.Result(v.Count() < total)
	requireResult(t, "go replay merge", goResult, o, position)
	require.True(t, rm.IsMerged(), "go replay merge did not merge")

	javaMerged := j.waitFor("merged", o.timeout)
	t.Logf("java replay merge merged after %d messages", javaMerged.int("received"))
	state := j.waitFor("state", o.timeout)
	require.True(t, state.bool("merged"))
	javaResult := j.waitFor("result", o.timeout).result()
	requireResult(t, "java replay merge", javaResult, o, position)
	require.Equal(t, javaResult.SHA256, goResult.SHA256)
}
