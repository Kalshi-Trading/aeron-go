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

// basic_publisher is a port of io.aeron.samples.BasicPublisher: it offers "Hello World! <n>"
// once a second. Add -exclusive to use an exclusive publication.
package main

import (
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/lirm/aeron-go/aeron"
	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/examples"
)

func main() {
	flag.Parse()
	examples.SetupLogging()
	ctx, cancel := examples.SignalContext()
	defer cancel()

	channel, streamID := *examples.ExamplesConfig.Channel, int32(*examples.ExamplesConfig.StreamID)
	fmt.Printf("Publishing to %s on stream id %d\n", channel, streamID)

	a, err := aeron.Connect(examples.NewContext())
	if err != nil {
		log.Fatalf("Failed to connect to media driver: %v", err)
	}
	defer a.Close()

	var publication *aeron.Publication
	if *examples.ExamplesConfig.Exclusive {
		publication, err = a.AddExclusivePublication(channel, streamID)
	} else {
		publication, err = a.AddPublication(channel, streamID)
	}
	if err != nil {
		log.Fatalf("Failed to add publication: %v", err)
	}
	defer publication.Close()

	buffer := atomic.MakeBuffer(make([]byte, 256))
	messages := *examples.ExamplesConfig.Messages
	for i := 0; i < messages && ctx.Err() == nil; i++ {
		fmt.Printf("Offering %d/%d - ", i, messages)
		msg := []byte(fmt.Sprintf("Hello World! %d", i))
		buffer.PutBytesArray(0, &msg, 0, int32(len(msg)))
		result := publication.Offer(buffer, 0, int32(len(msg)), nil)
		if result > 0 {
			fmt.Println("yay!")
		} else {
			fmt.Println(examples.OfferResultString(result))
			if examples.IsTerminalOfferResult(result) {
				break
			}
		}
		if !publication.IsConnected() {
			fmt.Println("No active subscribers detected")
		}
		select {
		case <-ctx.Done():
		case <-time.After(time.Second):
		}
	}
	fmt.Println("Done sending.")
	examples.Linger()
}
