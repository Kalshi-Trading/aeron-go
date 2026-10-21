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

// simple_subscriber is a port of io.aeron.samples.SimpleSubscriber: it prints messages received
// on aeron:udp?endpoint=localhost:40123 stream 10 until interrupted (or -exit N messages).
package main

import (
	"flag"
	"fmt"
	"log"

	"github.com/lirm/aeron-go/aeron"
	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/idlestrategy"
	"github.com/lirm/aeron-go/aeron/logbuffer"
	"github.com/lirm/aeron-go/examples"
)

func main() {
	_ = flag.Set("chan", "aeron:udp?endpoint=localhost:40123")
	_ = flag.Set("sid", "10")
	flag.Parse()
	examples.SetupLogging()
	ctx, cancel := examples.SignalContext()
	defer cancel()

	channel, streamID := *examples.ExamplesConfig.Channel, int32(*examples.ExamplesConfig.StreamID)
	fmt.Printf("Subscribing to %s on stream id %d\n", channel, streamID)

	a, err := aeron.Connect(examples.NewContext())
	if err != nil {
		log.Fatalf("Failed to connect to media driver: %v", err)
	}
	defer a.Close()
	subscription, err := a.AddSubscription(channel, streamID)
	if err != nil {
		log.Fatalf("Failed to add subscription: %v", err)
	}
	defer subscription.Close()

	received := 0
	handler := func(buffer *atomic.Buffer, offset int32, length int32, header *logbuffer.Header) {
		fmt.Printf("Message to stream %d from session %d (%d@%d) <<%s>>\n",
			streamID, header.SessionID(), length, offset, string(buffer.GetBytesArray(offset, length)))
		received++
	}
	exitAfter := *examples.ExamplesConfig.ExitAfterMessages
	idler := idlestrategy.NewDefaultBackoffIdleStrategy()
	for ctx.Err() == nil && (exitAfter == 0 || received < exitAfter) {
		idler.Idle(subscription.Poll(handler, *examples.ExamplesConfig.FragmentLimit))
	}
	fmt.Println("Shutting down...")
}
