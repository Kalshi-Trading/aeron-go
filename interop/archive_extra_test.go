package interop

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/lirm/aeron-go/aeron"
	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/logbuffer"
	"github.com/lirm/aeron-go/archive"
	"github.com/stretchr/testify/require"
)

type recorded struct {
	recordingID  int64
	stopPosition int64
	sessionID    int32
	stream       int32
}

// goRecord publishes o on channel from Go and records it locally through the Go archive client.
func goRecord(t *testing.T, arch *archive.Archive, channel string, exclusive bool, o msgOpts) recorded {
	t.Helper()
	stream := nextStream()
	var pub aeron.Publisher
	var err error
	if exclusive {
		pub, err = arch.AddExclusivePublication(channel, stream)
	} else {
		pub, err = arch.AddPublication(channel, stream)
	}
	require.NoError(t, err)
	defer pub.Close()
	recCh, err := archive.AddSessionIdToChannel(channel, pub.SessionID())
	require.NoError(t, err)
	subID, err := arch.StartRecording(recCh, stream, true, false)
	require.NoError(t, err)
	reader := arch.Aeron().CounterReader()
	counterID, recordingID := awaitRecordingCounter(t, reader, pub.SessionID())
	require.NoError(t, awaitConnected(pub, 10*time.Second))
	position, err := goPublish(pub, o)
	require.NoError(t, err)
	awaitCounterValue(t, reader, counterID, position)
	require.NoError(t, arch.StopRecordingBySubscriptionId(subID))
	awaitStopPosition(t, arch, recordingID, position)
	return recorded{recordingID: recordingID, stopPosition: position, sessionID: pub.SessionID(), stream: stream}
}

// javaRecord publishes o on channel from Java and records it through the Java archive client.
func javaRecord(t *testing.T, arch *archive.Archive, channel string, exclusive bool, o msgOpts) recorded {
	t.Helper()
	stream := nextStream()
	j := startJava(t, "record", append([]string{"channel=" + channel, fmt.Sprintf("stream=%d", stream),
		fmt.Sprintf("exclusive=%t", exclusive)}, o.javaArgs()...)...)
	rec := j.waitFor("recorded", defaultTimeout)
	r := recorded{recordingID: rec.int("recordingId"), stopPosition: rec.int("stopPosition"),
		sessionID: int32(rec.int("sessionId")), stream: stream}
	awaitStopPosition(t, arch, r.recordingID, r.stopPosition)
	return r
}

type recorderFn func(t *testing.T, arch *archive.Archive, channel string, exclusive bool, o msgOpts) recorded

func recorders() map[string]recorderFn {
	return map[string]recorderFn{"go-recorded": goRecord, "java-recorded": javaRecord}
}

// descriptorMsgs fit the small explicit term lengths of explicitRecordedChannels (max message = term-length/8).
var descriptorMsgs = msgOpts{count: 200, sizes: Sizes{64, 4000, 1376, 8000}, reserved: true}

func explicitRecordedChannels() map[string]func() string {
	return map[string]func() string{
		"ipc": func() string { return "aeron:ipc?term-length=131072|mtu=4096" },
		"udp": func() string { return udp(nextPort()) + "|ssc=true|term-length=65536|mtu=1408" },
	}
}

// Every RecordingDescriptor field decoded by the Go archive client equals what the Java client decodes.
func TestRecordingDescriptorGoVsJava(t *testing.T) {
	requireE2E(t)
	arch := connectGoArchive(t)
	for recName, record := range recorders() {
		for chName, channelFn := range explicitRecordedChannels() {
			for _, exclusive := range []bool{false, true} {
				record, channel := record, channelFn()
				t.Run(fmt.Sprintf("%s/%s/exclusive=%t", recName, chName, exclusive), func(t *testing.T) {
					rec := record(t, arch, channel, exclusive, descriptorMsgs)
					desc, err := arch.ListRecording(rec.recordingID)
					require.NoError(t, err)
					require.NotNil(t, desc)

					j := startJava(t, "describe", fmt.Sprintf("recordingId=%d", rec.recordingID))
					jd := j.waitFor("descriptor", defaultTimeout)
					require.Equal(t, int64(1), j.waitFor("described", defaultTimeout).int("found"))

					require.Equal(t, jd.int("recordingId"), desc.RecordingId)
					require.Equal(t, jd.int("startTimestamp"), desc.StartTimestamp)
					require.Equal(t, jd.int("stopTimestamp"), desc.StopTimestamp)
					require.Equal(t, jd.int("startPosition"), desc.StartPosition)
					require.Equal(t, jd.int("stopPosition"), desc.StopPosition)
					require.Equal(t, jd.int("initialTermId"), int64(desc.InitialTermId))
					require.Equal(t, jd.int("segmentFileLength"), int64(desc.SegmentFileLength))
					require.Equal(t, jd.int("termBufferLength"), int64(desc.TermBufferLength))
					require.Equal(t, jd.int("mtuLength"), int64(desc.MtuLength))
					require.Equal(t, jd.int("sessionId"), int64(desc.SessionId))
					require.Equal(t, jd.int("streamId"), int64(desc.StreamId))
					require.Equal(t, jd.str("strippedChannel"), string(desc.StrippedChannel))
					require.Equal(t, jd.str("originalChannel"), string(desc.OriginalChannel))
					require.Equal(t, jd.str("sourceIdentity"), string(desc.SourceIdentity))

					uri, err := aeron.ParseChannelUri(channel)
					require.NoError(t, err)
					require.Equal(t, rec.stopPosition, desc.StopPosition)
					require.Equal(t, rec.sessionID, desc.SessionId)
					require.Equal(t, rec.stream, desc.StreamId)
					require.Equal(t, uri.Get("term-length"), fmt.Sprint(desc.TermBufferLength))
					require.Equal(t, uri.Get("mtu"), fmt.Sprint(desc.MtuLength))
				})
			}
		}
	}
}

// messagePositions replays a whole recording through Go and returns the end position of every message.
func messagePositions(t *testing.T, arch *archive.Archive, recordingID int64, o msgOpts) []int64 {
	t.Helper()
	replayCh, replayStream := "aeron:ipc", nextStream()
	sub, err := arch.Replay(recordingID, 0, archive.RecordingLengthNull, replayCh, replayStream)
	require.NoError(t, err)
	defer sub.Close()
	require.NoError(t, awaitSubConnected(sub, 10*time.Second))
	v := NewVerifier(o.sizes, o.reserved)
	var positions []int64
	assembler := aeron.NewFragmentAssembler(func(b *atomic.Buffer, off, length int32, h *logbuffer.Header) {
		v.OnFragment(b, off, length, h)
		positions = append(positions, h.Position())
	}, 4096)
	deadline := time.Now().Add(o.timeoutOrDefault())
	for v.Count() < o.count {
		sub.Poll(assembler.OnFragment, 10)
		require.True(t, time.Now().Before(deadline), "full replay timed out at %d", v.Count())
	}
	requireResult(t, "go full replay", v.Result(false), o, -1)
	return positions
}

// Replays of a [position, position+length) slice of a recording starting mid-stream (frame aligned at a message
// boundary) deliver identical messages to Go and Java, over plain and response (control-mode=response) channels.
func TestReplayMidPositionGoVsJava(t *testing.T) {
	requireE2E(t)
	arch := connectGoArchive(t)
	ipcArch := connectGoArchiveIPC(t)
	for recName, record := range recorders() {
		record := record
		t.Run(recName, func(t *testing.T) {
			rec := record(t, arch, udp(nextPort())+"|ssc=true|term-length=8388608|mtu=1408", false, archiveMsgs)
			positions := messagePositions(t, arch, rec.recordingID, archiveMsgs)
			require.Len(t, positions, int(archiveMsgs.count))
			require.Equal(t, rec.stopPosition, positions[len(positions)-1])

			const first, n = 7, 31
			start := positions[first-1]
			length := positions[first+n-1] - start
			slice := archiveMsgs
			slice.first, slice.count = first, n

			for _, replayCh := range []string{udp(nextPort()), "aeron:ipc",
				fmt.Sprintf("aeron:udp?control-mode=response|control=localhost:%d", nextPort()),
				"aeron:ipc?control-mode=response"} {
				replayCh := replayCh
				t.Run(replayCh, func(t *testing.T) {
					stream := nextStream()
					params := archive.NewReplayParams()
					params.Position = start
					params.Length = length
					replayArch, javaControl := arch, []string(nil)
					if strings.HasPrefix(replayCh, "aeron:ipc?control-mode=response") {
						replayArch, javaControl = ipcArch, []string{"controlChannel=aeron:ipc"}
					}
					sub, err := replayArch.ReplayWithParams(rec.recordingID, replayCh, stream, params)
					require.NoError(t, err)
					defer sub.Close()
					require.NoError(t, awaitSubConnected(sub, 10*time.Second))
					joinPosition := int64(-1)
					goResult := goReceive(recordJoinPosition(sub, &joinPosition), slice)
					requireResult(t, "go replay", goResult, slice, start+length)
					require.Equal(t, start, joinPosition)

					j := startJava(t, "replay", append([]string{fmt.Sprintf("recordingId=%d", rec.recordingID),
						"channel=" + replayCh, fmt.Sprintf("stream=%d", nextStream()),
						fmt.Sprintf("position=%d", start), fmt.Sprintf("length=%d", length)},
						append(javaControl, slice.javaArgs()...)...)...)
					javaResult := j.waitFor("result", defaultTimeout).result()
					requireResult(t, "java replay", javaResult, slice, start+length)
					require.Equal(t, javaResult.SHA256, goResult.SHA256)
				})
			}
		})
	}
}
