/*
Copyright 2016 Stanislav Liberman

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// replay_throughput is the Go equivalent of io.aeron.samples.archive.EmbeddedReplayThroughput. Go
// has no embedded archive, so it uses an external ArchivingMediaDriver (e.g. docker/): it records
// -messages messages of -payload bytes on -samplechannel/-samplestream, then replays the recording
// to -replaychannel/-replaystream and reports the replay throughput.
package main

import (
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/lirm/aeron-go/aeron"
	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/counters"
	"github.com/lirm/aeron-go/aeron/idlestrategy"
	"github.com/lirm/aeron-go/aeron/logbuffer"
	"github.com/lirm/aeron-go/aeron/logging"
	"github.com/lirm/aeron-go/archive"
	"github.com/lirm/aeron-go/archive/examples"
	"github.com/lirm/aeron-go/archive/status"
)

var (
	replayChannel = flag.String("replaychannel", "aeron:ipc", "replay destination channel")
	replayStream  = flag.Int("replaystream", 101, "replay destination stream id")
)

const megabyte = 1024.0 * 1024.0

func main() {
	_ = flag.Set("samplechannel", "aeron:ipc?term-length=16m")
	_ = flag.Set("messages", "1000000")
	_ = flag.Set("payload", "32")
	flag.Parse()

	channel := *examples.Config.SampleChannel
	stream := int32(*examples.Config.SampleStream)
	messages := *examples.Config.Messages
	payload := int32(*examples.Config.Payload)
	if payload < 8 {
		log.Fatalf("payload must be at least 8 bytes")
	}

	arch := connect()
	defer arch.Close()

	fmt.Println("Making a recording for playback...")
	recordingID, recordingLength := makeRecording(arch, channel, stream, messages, payload)

	fmt.Printf("Replaying %d messages (%d bytes) from recording %d...\n", messages, recordingLength, recordingID)
	start := time.Now()
	received := replay(arch, recordingID, recordingLength, messages)
	elapsed := time.Since(start)
	fmt.Printf("Performance inc setup: recording %.2f MB, replayed %.2f MB/s %.4g msgs/sec for %d byte messages (%d messages in %v)\n",
		float64(recordingLength)/megabyte, float64(recordingLength)/megabyte/elapsed.Seconds(),
		float64(received)/elapsed.Seconds(), payload, received, elapsed)
	if received != messages {
		log.Fatalf("replayed %d messages, expected %d", received, messages)
	}
	if _, err := arch.PurgeRecording(recordingID); err != nil {
		log.Printf("PurgeRecording(%d) failed: %v", recordingID, err)
	}
}

func makeRecording(arch *archive.Archive, channel string, stream int32, messages int, payload int32) (int64, int64) {
	subscriptionID, err := arch.StartRecording(channel, stream, true, false)
	if err != nil {
		log.Fatalf("StartRecording failed: %v", err)
	}
	defer func() {
		if err := arch.StopRecordingBySubscriptionId(subscriptionID); err != nil {
			log.Printf("StopRecording failed: %v", err)
		}
	}()
	publication, err := arch.AddExclusivePublication(channel, stream)
	if err != nil {
		log.Fatalf("AddExclusivePublication failed: %v", err)
	}
	defer publication.Close()

	counterID, recordingID, err := awaitRecordingCounter(arch, publication.SessionID(), 10*time.Second)
	if err != nil {
		log.Fatal(err)
	}
	buffer := atomic.MakeBuffer(make([]byte, payload))
	idler := idlestrategy.NewDefaultBackoffIdleStrategy()
	start := time.Now()
	for i := 0; i < messages; i++ {
		buffer.PutInt64(0, int64(i))
		for {
			result := publication.Offer(buffer, 0, payload, nil)
			if result > 0 {
				break
			}
			if result == aeron.PublicationClosed || result == aeron.MaxPositionExceeded {
				log.Fatalf("offer failed: %d", result)
			}
			idler.Idle(0)
		}
	}
	position := publication.Position()
	if err := awaitRecordingPosition(arch, counterID, recordingID, position, time.Minute); err != nil {
		log.Fatal(err)
	}
	elapsed := time.Since(start)
	fmt.Printf("Recorded %.2f MB @ %.2f MB/s - %.4g msg/sec - %d byte payload\n", float64(position)/megabyte,
		float64(position)/megabyte/elapsed.Seconds(), float64(messages)/elapsed.Seconds(), payload)
	return recordingID, position
}

func replay(arch *archive.Archive, recordingID, length int64, messages int) int {
	replaySessionID, err := arch.StartReplay(recordingID, 0, length, *replayChannel, int32(*replayStream))
	if err != nil {
		log.Fatalf("StartReplay failed: %v", err)
	}
	subChannel, err := archive.AddSessionIdToChannel(*replayChannel, archive.ReplaySessionIdToSessionId(replaySessionID))
	if err != nil {
		log.Fatal(err)
	}
	subscription, err := arch.AddSubscription(subChannel, int32(*replayStream))
	if err != nil {
		log.Fatalf("AddSubscription failed: %v", err)
	}
	defer subscription.Close()

	received := 0
	next := int64(0)
	handler := func(buffer *atomic.Buffer, offset int32, _ int32, _ *logbuffer.Header) {
		if value := buffer.GetInt64(offset); value != next {
			log.Fatalf("unexpected message: got %d, expected %d", value, next)
		}
		next++
		received++
	}
	idler := idlestrategy.NewDefaultBackoffIdleStrategy()
	deadline := time.Now().Add(time.Minute)
	for received < messages && time.Now().Before(deadline) {
		idler.Idle(subscription.Poll(handler, 100))
	}
	return received
}

func connect() *archive.Archive {
	context := aeron.NewContext().
		AeronDir(*examples.Config.AeronPrefix).
		MediaDriverTimeout(time.Duration(*examples.Config.DriverTimeout) * time.Millisecond)
	options := archive.DefaultOptions()
	options.RequestChannel = *examples.Config.RequestChannel
	options.RequestStream = int32(*examples.Config.RequestStream)
	options.ResponseChannel = *examples.Config.ResponseChannel
	options.ResponseStream = int32(*examples.Config.ResponseStream)
	if *examples.Config.Verbose {
		options.ArchiveLoglevel = logging.DEBUG
		options.AeronLoglevel = logging.DEBUG
	}
	arch, err := archive.NewArchive(options, context)
	if err != nil {
		log.Fatalf("Failed to connect to archive: %v", err)
	}
	return arch
}

// awaitRecordingCounter waits for the archive's RecordingPos counter for sessionID and returns
// (counterID, recordingID), as RecordingPos.findCounterIdBySession does in Java.
func awaitRecordingCounter(arch *archive.Archive, sessionID int32, timeout time.Duration) (int32, int64, error) {
	reader := arch.Aeron().CounterReader()
	deadline := time.Now().Add(timeout)
	for {
		if counterID := status.FindCounterIdBySession(reader, sessionID, status.NullValue); counterID != counters.NullCounterId {
			return counterID, status.GetRecordingId(reader, counterID), nil
		}
		if _, err := arch.PollForErrorResponse(); err != nil {
			return 0, 0, err
		}
		if time.Now().After(deadline) {
			return 0, 0, fmt.Errorf("timed out waiting for recording of session %d", sessionID)
		}
		time.Sleep(time.Millisecond)
	}
}

// awaitRecordingPosition waits until the recording has caught up with position.
func awaitRecordingPosition(arch *archive.Archive, counterID int32, recordingID, position int64, timeout time.Duration) error {
	reader := arch.Aeron().CounterReader()
	deadline := time.Now().Add(timeout)
	for reader.GetCounterValue(counterID) < position {
		if !status.IsActive(reader, counterID, recordingID) {
			return fmt.Errorf("recording has stopped unexpectedly: %d", recordingID)
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("timed out waiting for recording %d to reach %d", recordingID, position)
		}
		time.Sleep(time.Millisecond)
	}
	return nil
}
