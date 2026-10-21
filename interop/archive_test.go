package interop

import (
	"fmt"
	"testing"
	"time"

	"github.com/lirm/aeron-go/aeron"
	"github.com/lirm/aeron-go/aeron/counters"
	"github.com/lirm/aeron-go/archive"
	"github.com/stretchr/testify/require"
)

func connectGoArchive(t *testing.T) *archive.Archive {
	t.Helper()
	arch, err := archive.NewArchive(archive.DefaultOptions(), goContext())
	require.NoError(t, err)
	t.Cleanup(func() { _ = arch.Close() })
	return arch
}

// awaitRecordingCounter waits for the archive's recording position counter of a session.
func awaitRecordingCounter(t *testing.T, reader *counters.Reader, sessionID int32) (counterID int32, recordingID int64) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		counterID = archive.FindCounterIdBySession(reader, sessionID)
		if counterID != counters.NullCounterId {
			return counterID, archive.GetRecordingId(reader, counterID)
		}
		require.Truef(t, time.Now().Before(deadline), "no recording counter for session %d", sessionID)
		time.Sleep(time.Millisecond)
	}
}

func awaitCounterValue(t *testing.T, reader *counters.Reader, counterID int32, position int64) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for reader.GetCounterValue(counterID) < position {
		require.Truef(t, time.Now().Before(deadline), "recording position %d < %d",
			reader.GetCounterValue(counterID), position)
		time.Sleep(time.Millisecond)
	}
}

func awaitStopPosition(t *testing.T, arch *archive.Archive, recordingID, position int64) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		stop, err := arch.GetStopPosition(recordingID)
		require.NoError(t, err)
		if stop == position {
			return
		}
		require.Truef(t, time.Now().Before(deadline), "stop position %d != %d", stop, position)
		time.Sleep(5 * time.Millisecond)
	}
}

var archiveMsgs = msgOpts{count: 60, sizes: Sizes{64, 1 << 20, 4000, 300_001, 1376}, reserved: true}

func recordedChannels() map[string]func() string {
	return map[string]func() string{
		"ipc": func() string { return "aeron:ipc" },
		"udp": func() string { return udp(nextPort()) + "|ssc=true" },
	}
}

// Go publishes + records through the Go archive client; Java replays (AeronArchive.replay) and verifies.
func TestGoRecordJavaReplay(t *testing.T) {
	requireE2E(t)
	for name, channelFn := range recordedChannels() {
		channel := channelFn()
		t.Run(name, func(t *testing.T) {
			arch := connectGoArchive(t)
			stream := nextStream()
			pub, err := arch.AddPublication(channel, stream)
			require.NoError(t, err)
			defer pub.Close()
			recCh, err := archive.AddSessionIdToChannel(channel, pub.SessionID())
			require.NoError(t, err)
			subID, err := arch.StartRecording(recCh, stream, true, false)
			require.NoError(t, err)
			reader := arch.Aeron().CounterReader()
			counterID, recordingID := awaitRecordingCounter(t, reader, pub.SessionID())
			require.NoError(t, awaitConnected(pub, 10*time.Second))

			position, err := goPublish(pub, archiveMsgs)
			require.NoError(t, err)
			awaitCounterValue(t, reader, counterID, position)
			require.NoError(t, arch.StopRecordingBySubscriptionId(subID))
			awaitStopPosition(t, arch, recordingID, position)

			for _, replayCh := range []string{udp(nextPort()), "aeron:ipc"} {
				j := startJava(t, "replay", append([]string{fmt.Sprintf("recordingId=%d", recordingID),
					"channel=" + replayCh, fmt.Sprintf("stream=%d", nextStream())}, archiveMsgs.javaArgs()...)...)
				r := j.waitFor("result", defaultTimeout).result()
				requireResult(t, "java replay via "+replayCh, r, archiveMsgs, position)
			}
		})
	}
}

// Java publishes + records through the Java archive client; Go replays and verifies (descriptor and data).
func TestJavaRecordGoReplay(t *testing.T) {
	requireE2E(t)
	for name, channelFn := range recordedChannels() {
		channel := channelFn()
		for _, exclusive := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/exclusive=%t", name, exclusive), func(t *testing.T) {
				stream := nextStream()
				j := startJava(t, "record", append([]string{"channel=" + channel, fmt.Sprintf("stream=%d", stream),
					fmt.Sprintf("exclusive=%t", exclusive)}, archiveMsgs.javaArgs()...)...)
				rec := j.waitFor("recorded", defaultTimeout)
				recordingID, stopPosition := rec.int("recordingId"), rec.int("stopPosition")

				arch := connectGoArchive(t)
				awaitStopPosition(t, arch, recordingID, stopPosition)
				desc, err := arch.ListRecording(recordingID)
				require.NoError(t, err)
				require.Equal(t, recordingID, desc.RecordingId)
				require.Equal(t, stream, desc.StreamId)
				require.Equal(t, int32(rec.int("sessionId")), desc.SessionId)
				require.Equal(t, int64(0), desc.StartPosition)
				require.Equal(t, stopPosition, desc.StopPosition)

				for _, replayCh := range []string{udp(nextPort()), "aeron:ipc"} {
					replayStream := nextStream()
					sub, err := arch.AddSubscription(replayCh, replayStream)
					require.NoError(t, err)
					replaySessionID, err := arch.StartReplay(recordingID, 0, archive.RecordingLengthNull, replayCh, replayStream)
					require.NoError(t, err)
					require.NoError(t, awaitSubConnected(sub, 10*time.Second))
					r := goReceive(sub.Poll, archiveMsgs)
					require.Equal(t, int32(replaySessionID), r.SessionID)
					requireResult(t, "go replay via "+replayCh, r, archiveMsgs, stopPosition)
					require.NoError(t, sub.Close())
				}
			})
		}
	}
}

// A Java publication recorded remotely by the Go archive client, replayed by both Go and Java: identical bytes.
func TestGoRecordsJavaPublication(t *testing.T) {
	requireE2E(t)
	arch := connectGoArchive(t)
	stream := nextStream()
	channel := udp(nextPort())
	subID, err := arch.StartRecording(channel, stream, false, false)
	require.NoError(t, err)

	o := archiveMsgs
	j := startJava(t, "pub", append([]string{"channel=" + channel, fmt.Sprintf("stream=%d", stream)}, o.javaArgs()...)...)
	ready := j.waitFor("ready", 20*time.Second)
	reader := arch.Aeron().CounterReader()
	counterID, recordingID := awaitRecordingCounter(t, reader, int32(ready.int("sessionId")))
	position := j.waitFor("done", defaultTimeout).int("position")
	awaitCounterValue(t, reader, counterID, position)
	require.NoError(t, arch.StopRecordingBySubscriptionId(subID))
	j.send("exit")
	awaitStopPosition(t, arch, recordingID, position)

	jr := startJava(t, "replay", append([]string{fmt.Sprintf("recordingId=%d", recordingID),
		"channel=" + udp(nextPort()), fmt.Sprintf("stream=%d", nextStream())}, o.javaArgs()...)...)
	javaResult := jr.waitFor("result", defaultTimeout).result()
	requireResult(t, "java replay", javaResult, o, position)

	replayCh, replayStream := udp(nextPort()), nextStream()
	sub, err := arch.AddSubscription(replayCh, replayStream)
	require.NoError(t, err)
	_, err = arch.StartReplay(recordingID, 0, archive.RecordingLengthNull, replayCh, replayStream)
	require.NoError(t, err)
	require.NoError(t, awaitSubConnected(sub, 10*time.Second))
	goResult := goReceive(sub.Poll, o)
	requireResult(t, "go replay", goResult, o, position)
	require.Equal(t, javaResult.SHA256, goResult.SHA256)
}

var _ = aeron.NullValue
