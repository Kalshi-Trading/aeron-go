// Copyright 2026 Kalshi Trading.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// persistent_subscriber is a port of Java's PersistentSubscriber: a PersistentSubscription to the latest recording
// of -samplechannel/-samplestream that replays from -start, joins the live stream once caught up and falls back to
// replay if the live stream is lost. Pair it with basic_recording_publisher.
package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/lirm/aeron-go/aeron"
	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/idlestrategy"
	"github.com/lirm/aeron-go/aeron/logbuffer"
	"github.com/lirm/aeron-go/aeron/logging"
	"github.com/lirm/aeron-go/archive"
	"github.com/lirm/aeron-go/archive/examples"
	ps "github.com/lirm/aeron-go/archive/persistentsubscription"
)

var (
	exitAfter     = flag.Int("exit", 0, "exit after receiving this many messages (0 = run until interrupted)")
	replayChannel = flag.String("replaychannel", "aeron:udp?endpoint=localhost:0", "channel to receive replays on")
	replayStream  = flag.Int("replaystream", 5000, "stream id to receive replays on")
	startPosition = flag.Int64("start", ps.FromStart,
		fmt.Sprintf("recording position to start from, %d = FROM_START, %d = FROM_LIVE", ps.FromStart, ps.FromLive))
)

var logID = "persistent_subscriber"
var logger = logging.MustGetLogger(logID)

func archiveOptions() *archive.Options {
	options := archive.DefaultOptions()
	options.RequestChannel = *examples.Config.RequestChannel
	options.RequestStream = int32(*examples.Config.RequestStream)
	options.ResponseChannel = *examples.Config.ResponseChannel
	options.ResponseStream = int32(*examples.Config.ResponseStream)
	if *examples.Config.Verbose {
		options.ArchiveLoglevel = logging.DEBUG
		options.AeronLoglevel = logging.DEBUG
	}
	return options
}

func main() {
	if !run() {
		os.Exit(1)
	}
}

// run returns false if the PersistentSubscription failed; deferred cleanup runs before main exits.
func run() bool {
	flag.Parse()

	liveChannel := *examples.Config.SampleChannel
	liveStream := int32(*examples.Config.SampleStream)
	fmt.Printf("Subscribing to live on channel %s and stream id %d, to replays on channel %s and stream id %d, "+
		"starting from position %d\n", liveChannel, liveStream, *replayChannel, *replayStream, *startPosition)

	timeout := time.Duration(time.Millisecond.Nanoseconds() * *examples.Config.DriverTimeout)
	aeronCtx := aeron.NewContext().AeronDir(*examples.Config.AeronPrefix).MediaDriverTimeout(timeout)
	arch, err := archive.NewArchive(archiveOptions(), aeronCtx)
	if err != nil {
		logger.Fatalf("Failed to connect to the archive: %s", err)
	}
	defer arch.Close()

	descriptor, err := examples.FindLatestRecording(arch, liveChannel, liveStream)
	if err != nil {
		logger.Fatal(err)
	}
	if descriptor == nil {
		logger.Fatalf("No recordings found for channel %s and stream id %d", liveChannel, liveStream)
	}

	isLive := false
	ctx := ps.NewContext()
	ctx.Aeron = arch.Aeron()
	ctx.AeronDir = *examples.Config.AeronPrefix
	// The PersistentSubscription runs its own (asynchronous) archive control session.
	ctx.ArchiveOptions = archiveOptions()
	ctx.RecordingID = descriptor.RecordingId
	ctx.StartPosition = *startPosition
	ctx.LiveChannel = liveChannel
	ctx.LiveStreamID = liveStream
	ctx.ReplayChannel = *replayChannel
	ctx.ReplayStreamID = int32(*replayStream)
	ctx.Listener = ps.ListenerFuncs{
		LiveJoined: func() {
			isLive = true
			fmt.Println("===========")
			fmt.Println("PersistentSubscription has joined live stream.")
			fmt.Println("===========")
		},
		LiveLeft: func() {
			isLive = false
			fmt.Println("===========")
			fmt.Println("PersistentSubscription has left live stream.")
			fmt.Println("===========")
		},
		Error: func(err error) {
			fmt.Printf("PersistentSubscription error: %v\n", err)
		},
	}

	subscription, err := ps.New(ctx)
	if err != nil {
		logger.Fatal(err)
	}
	defer subscription.Close()

	received := 0
	handler := func(buffer *atomic.Buffer, offset, length int32, header *logbuffer.Header) {
		streamState := "replay"
		if isLive {
			streamState = "live"
		}
		fmt.Printf("Message to %s stream %d from session %d of length %d at position %d <<%s>>\n", streamState,
			header.StreamID(), header.SessionID(), length, header.Position(), string(buffer.GetBytesArray(offset, length)))
		received++
	}

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	idleStrategy := idlestrategy.Yielding{}
loop:
	for *exitAfter == 0 || received < *exitAfter {
		select {
		case <-stop:
			break loop
		default:
		}
		workCount := subscription.Poll(handler, 10)
		if subscription.HasFailed() {
			fmt.Printf("PersistentSubscription has failed: %v\n", subscription.FailureReason())
			break
		}
		idleStrategy.Idle(workCount)
	}
	fmt.Println("Shutting down...")
	fmt.Printf("Received %d messages\n", received)
	return !subscription.HasFailed()
}
