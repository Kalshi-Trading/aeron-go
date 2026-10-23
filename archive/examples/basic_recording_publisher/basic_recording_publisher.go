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

// basic_recording_publisher is a port of io.aeron.samples.archive.RecordedBasicPublisher: it asks
// the archive to record -samplechannel/-samplestream, publishes -messages "Hello World! <n>"
// messages (one every -interval), waits for the recording to catch up and stops the recording.
package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/lirm/aeron-go/aeron"
	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/counters"
	"github.com/lirm/aeron-go/aeron/logging"
	"github.com/lirm/aeron-go/archive"
	"github.com/lirm/aeron-go/archive/examples"
	"github.com/lirm/aeron-go/archive/status"
)

var interval = flag.Duration("interval", time.Second, "delay between messages")

func main() {
	flag.Parse()
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	// ssc=true (spies-simulate-connection) lets the archive's local spy count as a subscriber, so
	// offers succeed without a live network subscriber.
	channel := *examples.Config.SampleChannel
	if !strings.Contains(channel, "ssc=") {
		channel += "|ssc=true"
	}
	stream := int32(*examples.Config.SampleStream)
	fmt.Printf("Publishing to %s on stream id %d\n", channel, stream)

	arch := connect()
	defer arch.Close()

	if _, err := arch.StartRecording(channel, stream, true, false); err != nil {
		log.Fatalf("StartRecording failed: %v", err)
	}
	defer func() {
		if err := arch.StopRecording(channel, stream); err != nil {
			log.Printf("StopRecording failed: %v", err)
		}
	}()

	publication, err := arch.AddPublication(channel, stream)
	if err != nil {
		log.Fatalf("AddPublication failed: %v", err)
	}
	defer publication.Close()

	counterID, recordingID, err := awaitRecordingCounter(arch, publication.SessionID(), 10*time.Second)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Recording started: recordingId = %d\n", recordingID)

	buffer := atomic.MakeBuffer(make([]byte, 256))
	messages := *examples.Config.Messages
loop:
	for i := 0; i < messages; i++ {
		msg := []byte(fmt.Sprintf("Hello World! %d", i))
		buffer.PutBytesSlice(0, msg)
		fmt.Printf("Offering %d/%d - ", i, messages)
		if result := publication.Offer(buffer, 0, int32(len(msg)), nil); result > 0 {
			fmt.Println("yay!")
		} else {
			fmt.Println(offerFailure(result))
		}
		if _, err := arch.PollForErrorResponse(); err != nil {
			log.Fatal(err)
		}
		select {
		case <-stop:
			break loop
		case <-time.After(*interval):
		}
	}

	if err := awaitRecordingPosition(arch, counterID, recordingID, publication.Position(), 10*time.Second); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Done sending. recordingId=%d position=%d\n", recordingID, publication.Position())
}

func offerFailure(result int64) string {
	switch result {
	case aeron.BackPressured:
		return "Offer failed due to back pressure"
	case aeron.NotConnected:
		return "Offer failed because publisher is not connected to a subscriber"
	case aeron.AdminAction:
		return "Offer failed because of an administration action in the system"
	case aeron.PublicationClosed:
		return "Offer failed because publication is closed"
	default:
		return fmt.Sprintf("Offer failed due to unknown reason: %d", result)
	}
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
