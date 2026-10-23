// Command replayer replays a recording (by id, or the latest matching -channel/-stream) and verifies the
// sequenced messages.
package main

import (
	"flag"
	"log"
	"time"

	"github.com/lirm/aeron-go/archive"
	"github.com/lirm/aeron-go/archive/codecs"
	"github.com/lirm/aeron-go/e2e"
)

func main() {
	aeronDir := flag.String("aeron-dir", e2e.AeronDir(), "media driver directory (default $AERON_DIR)")
	recordingID := flag.Int64("recording-id", -1, "recording to replay, -1 for the latest matching -channel/-stream")
	channel := flag.String("channel", "aeron:udp?endpoint=localhost:20121", "recorded channel fragment (with -recording-id -1)")
	stream := flag.Int("stream", 1001, "recorded stream id (with -recording-id -1)")
	replayChannel := flag.String("replay-channel", "aeron:udp?endpoint=localhost:20151", "replay destination channel")
	replayStream := flag.Int("replay-stream", 1101, "replay stream id")
	count := flag.Int64("count", 10000, "number of messages expected")
	from := flag.Int64("from", 0, "first expected sequence number")
	controlChannel := flag.String("control-channel", e2e.DefaultControlChannel, "archive control request channel")
	controlStream := flag.Int("control-stream", e2e.DefaultControlStream, "archive control request stream")
	timeout := flag.Duration("timeout", 2*time.Minute, "give up after this long")
	flag.Parse()

	arch, err := e2e.ConnectArchive(*aeronDir, *controlChannel, int32(*controlStream))
	if err != nil {
		log.Fatalf("connect archive: %v", err)
	}
	defer arch.Close()

	rec, err := e2e.WaitForRecording(arch, *channel, int32(*stream), *timeout, func(d *codecs.RecordingDescriptor) bool {
		return *recordingID < 0 || d.RecordingId == *recordingID
	})
	if err != nil {
		log.Fatal(err)
	}
	sub, replaySession, err := e2e.Replay(arch, rec.RecordingId, rec.StartPosition, archive.RecordingPositionNull,
		*replayChannel, int32(*replayStream))
	if err != nil {
		log.Fatal(err)
	}
	defer sub.Close()
	log.Printf("replaying recording %d (replay session %d)", rec.RecordingId, replaySession)

	checker := e2e.NewSequenceChecker(*from)
	if err := e2e.Consume(e2e.ReassemblingPoller(sub), checker, *from+*count, *timeout); err != nil {
		log.Fatal(err)
	}
	log.Printf("OK recording %d: %s", rec.RecordingId, checker)
}
