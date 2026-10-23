// Command replay-merge replays the latest recording of a multi-destination-cast stream from the start and
// merges onto the live stream (archive/replaymerge), verifying that no message is lost or duplicated.
package main

import (
	"flag"
	"log"
	"time"

	"github.com/lirm/aeron-go/e2e"
)

func main() {
	aeronDir := flag.String("aeron-dir", e2e.AeronDir(), "media driver directory (default $AERON_DIR)")
	channel := flag.String("channel", "aeron:udp?endpoint=localhost:20142|control=localhost:20141", "recorded channel fragment")
	stream := flag.Int("stream", 1003, "recorded stream id")
	liveDestination := flag.String("live-destination", "aeron:udp?endpoint=localhost:20143|control=localhost:20141", "live MDC destination")
	replayDestination := flag.String("replay-destination", "aeron:udp?endpoint=localhost:0", "replay destination")
	count := flag.Int64("count", 10000, "total number of messages expected")
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

	rec, err := e2e.WaitForRecording(arch, *channel, int32(*stream), *timeout, nil)
	if err != nil {
		log.Fatal(err)
	}
	merge, sub, err := e2e.StartReplayMerge(arch, e2e.ReplayMergeOptions{
		Recording: rec, StartPosition: rec.StartPosition, ReplayDestination: *replayDestination, LiveDestination: *liveDestination,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer sub.Close()
	defer merge.Close()
	log.Printf("replay-merging recording %d session %d", rec.RecordingId, rec.SessionId)

	checker := e2e.NewSequenceChecker(*from)
	if err := e2e.Consume(e2e.MergePoller(merge), checker, *from+*count, *timeout); err != nil {
		log.Fatal(err)
	}
	if !merge.IsMerged() {
		log.Fatalf("consumed all messages but replay never merged with live: %s", checker)
	}
	log.Printf("OK merged recording %d: %s", rec.RecordingId, checker)
}
