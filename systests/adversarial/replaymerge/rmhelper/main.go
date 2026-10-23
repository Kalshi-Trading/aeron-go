// Command rmhelper is the child process used by the adversarial replaymerge tests: a publisher ("pub"), a
// ReplayMerge consumer ("merge") or a PersistentSubscription consumer ("ps") that the tests kill with SIGKILL. It
// reports JSON events on stdout (see internal/rmrun).
package main

import (
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/lirm/aeron-go/aeron"
	"github.com/lirm/aeron-go/systests/adversarial/replaymerge/internal/rmrun"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: rmhelper pub|merge|ps [flags]")
		os.Exit(2)
	}
	mode := os.Args[1]
	fs := flag.NewFlagSet(mode, flag.ExitOnError)
	dir := fs.String("dir", "/dev/shm/aeron-adv-replaymerge", "aeron dir")
	channel := fs.String("channel", "", "publication / live channel")
	stream := fs.Int("stream", 9400, "stream id")
	kind := fs.String("kind", "excl", "publisher kind: pub, excl, tryclaim")
	from := fs.Int64("from", 0, "first sequence")
	count := fs.Int64("count", 0, "messages")
	sizes := fs.String("sizes", "64", "comma separated message sizes")
	interval := fs.Duration("interval", 0, "pause between messages")
	rate := fs.Int("rate", 0, "messages per second")
	every := fs.Int64("every", 1000, "progress/sent event every N messages")
	recording := fs.Int64("recording", -1, "recording id")
	session := fs.Int("session", 0, "session id")
	replayDest := fs.String("replayDest", "aeron:udp?endpoint=localhost:0", "replay destination")
	liveDest := fs.String("liveDest", "", "live destination")
	replayChannel := fs.String("replayChannel", "", "ps replay channel")
	replayStream := fs.Int("replayStream", 9401, "ps replay stream")
	position := fs.Int64("position", 0, "start position")
	timeout := fs.Duration("timeout", time.Minute, "overall timeout")
	retries := fs.Int("retries", 0, "merge retries")
	_ = fs.Parse(os.Args[2:])

	emit := rmrun.JSONEmitter(os.Stdout)
	ctx := aeron.NewContext().AeronDir(*dir).
		ErrorHandler(func(err error) { emit("clientError", "message", err.Error()) }).
		UnavailableImageHandler(func(img aeron.Image) {
			emit("unavailableImage", "sessionId", img.SessionID(), "position", img.Position())
		})
	a, err := aeron.Connect(ctx)
	if err != nil {
		emit("fatal", "message", err.Error())
		os.Exit(1)
	}
	defer a.Close()

	switch mode {
	case "pub":
		var sz []int
		for _, s := range strings.Split(*sizes, ",") {
			n, err := strconv.Atoi(s)
			if err != nil {
				emit("fatal", "message", err.Error())
				os.Exit(2)
			}
			sz = append(sz, n)
		}
		p, err := rmrun.NewPublisher(a, rmrun.PubConfig{Channel: *channel, StreamID: int32(*stream), Kind: *kind,
			From: *from, Count: *count, Sizes: sz, Interval: *interval, Rate: *rate, SentEvery: *every})
		if err != nil {
			emit("fatal", "message", err.Error())
			os.Exit(1)
		}
		emit("ready", "sessionId", p.Pub().SessionID())
		if err := p.Run(nil, emit, nil); err != nil {
			emit("fatal", "message", err.Error())
			os.Exit(1)
		}
		select {} // keep the publication open until killed
	case "merge":
		rmrun.RunReplayMerge(a, rmrun.MergeConfig{RecordingID: *recording, SessionID: int32(*session),
			StreamID: int32(*stream), ReplayDestination: *replayDest, LiveDestination: *liveDest, Position: *position,
			From: *from, Count: *count, Timeout: *timeout, Retries: *retries, ProgressEvery: *every}, emit)
	case "ps":
		if _, err := rmrun.RunPersistentSubscription(a, rmrun.PSConfig{AeronDir: *dir, RecordingID: *recording,
			Position: *position, LiveChannel: *channel, StreamID: int32(*stream), ReplayChannel: *replayChannel,
			ReplayStream: int32(*replayStream), From: *from, Count: *count, Timeout: *timeout,
			ProgressEvery: *every}, emit); err != nil {
			emit("fatal", "message", err.Error())
			os.Exit(1)
		}
	default:
		emit("fatal", "message", "unknown mode "+mode)
		os.Exit(2)
	}
}
