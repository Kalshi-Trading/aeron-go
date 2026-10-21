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

// simple_publisher is a port of io.aeron.samples.SimplePublisher: it waits up to 5s for a
// subscriber on aeron:udp?endpoint=localhost:40123 stream 10 and sends one message.
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
	_ = flag.Set("chan", "aeron:udp?endpoint=localhost:40123")
	_ = flag.Set("sid", "10")
	flag.Parse()
	examples.SetupLogging()

	channel, streamID := *examples.ExamplesConfig.Channel, int32(*examples.ExamplesConfig.StreamID)
	fmt.Printf("Publishing to %s on stream id %d\n", channel, streamID)

	a, err := aeron.Connect(examples.NewContext())
	if err != nil {
		log.Fatalf("Failed to connect to media driver: %v", err)
	}
	defer a.Close()
	publication, err := a.AddPublication(channel, streamID)
	if err != nil {
		log.Fatalf("Failed to add publication: %v", err)
	}
	defer publication.Close()

	deadline := time.Now().Add(5 * time.Second)
	for !publication.IsConnected() {
		if time.Now().After(deadline) {
			fmt.Println("Failed to connect to subscriber")
			return
		}
		time.Sleep(time.Millisecond)
	}

	msg := []byte("Hello World! ")
	buffer := atomic.MakeBuffer(msg)
	if result := publication.Offer(buffer, 0, buffer.Capacity(), nil); result > 0 {
		fmt.Println("yay!")
	} else {
		fmt.Println(examples.OfferResultString(result))
	}
}
