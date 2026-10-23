// Copyright (C) 2021-2022 Talos, Inc.
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

// basic_replay_merge is a port of Java's ReplayMergeSubscriber: it replays the latest recording of
// -samplechannel/-samplestream from its start position and merges onto the live stream, which is added as a
// destination of a manual multi-destination subscription. Pair it with basic_recording_publisher.
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
	"github.com/lirm/aeron-go/archive/replaymerge"
)

var (
	exitAfter         = flag.Int("exit", 0, "exit after receiving this many messages (0 = run until interrupted)")
	replayDestination = flag.String("replaydestination", "aeron:udp?endpoint=localhost:0",
		"replay destination; port 0 is resolved before the replay is requested")
)

var logID = "basic_replay_merge"
var logger = logging.MustGetLogger(logID)

func main() {
	flag.Parse()

	// As per Java: the live destination is the recorded sample channel.
	liveDestination := *examples.Config.SampleChannel
	streamID := int32(*examples.Config.SampleStream)
	fmt.Printf("Subscribing to live %s, and replay %s on stream id %d\n", liveDestination, *replayDestination, streamID)

	timeout := time.Duration(time.Millisecond.Nanoseconds() * *examples.Config.DriverTimeout)
	context := aeron.NewContext().
		AeronDir(*examples.Config.AeronPrefix).
		MediaDriverTimeout(timeout).
		AvailableImageHandler(examples.PrintAvailableImage).
		UnavailableImageHandler(examples.PrintUnavailableImage)

	options := archive.DefaultOptions()
	options.RequestChannel = *examples.Config.RequestChannel
	options.RequestStream = int32(*examples.Config.RequestStream)
	options.ResponseChannel = *examples.Config.ResponseChannel
	options.ResponseStream = int32(*examples.Config.ResponseStream)
	if *examples.Config.Verbose {
		options.ArchiveLoglevel = logging.DEBUG
		options.AeronLoglevel = logging.DEBUG
		logging.SetLevel(logging.DEBUG, logID)
	}

	arch, err := archive.NewArchive(options, context)
	if err != nil {
		logger.Fatalf("Failed to connect to the archive: %s", err)
	}
	defer arch.Close()

	descriptor, err := examples.FindLatestRecording(arch, liveDestination, streamID)
	if err != nil {
		logger.Fatal(err)
	}
	if descriptor == nil {
		logger.Fatalf("No recordings found for channel %s and stream id %d", liveDestination, streamID)
	}

	replayChannel := fmt.Sprintf("aeron:udp?session-id=%d", descriptor.SessionId)
	subscriptionChannel := fmt.Sprintf("aeron:udp?control-mode=manual|session-id=%d", descriptor.SessionId)

	// The multi-destination subscription that ReplayMerge adds the replay and live destinations to.
	subscription, err := arch.AddSubscription(subscriptionChannel, streamID)
	if err != nil {
		logger.Fatal(err)
	}
	defer subscription.Close()

	merge, err := replaymerge.NewReplayMergeWithDefaults(subscription, arch, replayChannel, *replayDestination,
		liveDestination, descriptor.RecordingId, descriptor.StartPosition)
	if err != nil {
		logger.Fatal(err)
	}
	defer merge.Close()

	isLive := false
	received := 0
	handler := aeron.NewFragmentAssembler(func(buffer *atomic.Buffer, offset, length int32, header *logbuffer.Header) {
		streamState := "replay"
		if isLive {
			streamState = "live"
		}
		fmt.Printf("Message to %s stream %d from session %d (%d@%d) <<%s>>\n", streamState, streamID,
			header.SessionID(), length, offset, string(buffer.GetBytesArray(offset, length)))
		received++
	}, aeron.DefaultFragmentAssemblyBufferLength)

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
		if merge.HasFailed() {
			logger.Fatalf("ReplayMerge has failed, %s", merge)
		}
		if merge.IsMerged() && !isLive {
			fmt.Println("===========")
			fmt.Println("ReplayMerge has joined live stream.")
			fmt.Println("===========")
			isLive = true
		}
		fragments, err := merge.Poll(handler.OnFragment, 10)
		if err != nil {
			logger.Fatalf("ReplayMerge has failed: %s, %s", err, merge)
		}
		idleStrategy.Idle(fragments)
	}
	fmt.Println("Shutting down...")
	fmt.Printf("Received %d messages\n", received)
}
