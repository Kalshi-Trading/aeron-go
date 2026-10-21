// Command recorder asks the archive to record a channel (remote, auto-stop) and waits for the recorded
// publication to go away, then prints the recording descriptor.
package main

import (
	"flag"
	"log"
	"time"

	"github.com/lirm/aeron-go/archive/codecs"
	"github.com/lirm/aeron-go/e2e"
)

func main() {
	aeronDir := flag.String("aeron-dir", e2e.AeronDir(), "media driver directory (default $AERON_DIR)")
	channel := flag.String("channel", "aeron:udp?endpoint=localhost:20121", "channel to record")
	stream := flag.Int("stream", 1001, "stream id")
	controlChannel := flag.String("control-channel", e2e.DefaultControlChannel, "archive control request channel")
	controlStream := flag.Int("control-stream", e2e.DefaultControlStream, "archive control request stream")
	readyFile := flag.String("ready-file", "", "file touched once the recording has been requested")
	timeout := flag.Duration("timeout", 5*time.Minute, "give up after this long")
	minLength := flag.Int64("min-length", 0, "fail if the recording is shorter than this many bytes")
	flag.Parse()

	arch, err := e2e.ConnectArchive(*aeronDir, *controlChannel, int32(*controlStream))
	if err != nil {
		log.Fatalf("connect archive: %v", err)
	}
	defer arch.Close()

	baseline := e2e.MaxRecordingID(arch)
	subscriptionID, err := arch.StartRecording(*channel, int32(*stream), false, true)
	if err != nil {
		log.Fatalf("StartRecording: %v", err)
	}
	log.Printf("recording %s stream %d (subscription %d)", *channel, *stream, subscriptionID)
	if err := e2e.TouchFile(*readyFile); err != nil {
		log.Fatal(err)
	}

	rec, err := e2e.WaitForRecording(arch, *channel, int32(*stream), *timeout, func(d *codecs.RecordingDescriptor) bool {
		return d.RecordingId > baseline
	})
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("recording %d started for session %d", rec.RecordingId, rec.SessionId)
	stop, err := e2e.WaitRecordingStopped(arch, rec.RecordingId, *timeout)
	if err != nil {
		log.Fatal(err)
	}
	if length := stop - rec.StartPosition; length < *minLength {
		log.Fatalf("recording %d length %d < %d", rec.RecordingId, length, *minLength)
	}
	log.Printf("OK recording %d session %d [%d, %d)", rec.RecordingId, rec.SessionId, rec.StartPosition, stop)
}
