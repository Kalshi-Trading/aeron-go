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

// basic_subscriber is a port of io.aeron.samples.BasicSubscriber: it prints every message received
// on the channel/stream. With -exit N it exits after N messages.
package main

import (
	"flag"
	"fmt"
	"log"

	"github.com/lirm/aeron-go/aeron"
	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/logbuffer"
	"github.com/lirm/aeron-go/examples"
)

func main() {
	flag.Parse()
	examples.SetupLogging()
	ctx, cancel := examples.SignalContext()
	defer cancel()

	channel, streamID := *examples.ExamplesConfig.Channel, int32(*examples.ExamplesConfig.StreamID)
	fmt.Printf("Subscribing to %s on stream id %d\n", channel, streamID)

	aeronCtx := examples.NewContext().
		AvailableImageHandler(examples.PrintAvailableImage).
		UnavailableImageHandler(examples.PrintUnavailableImage)
	a, err := aeron.Connect(aeronCtx)
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
	print := examples.PrintStringMessage(streamID)
	handler := func(buffer *atomic.Buffer, offset int32, length int32, header *logbuffer.Header) {
		print(buffer, offset, length, header)
		received++
	}
	exitAfter := *examples.ExamplesConfig.ExitAfterMessages
	examples.SubscriberLoop(ctx, subscription, handler, *examples.ExamplesConfig.FragmentLimit,
		examples.NewIdleStrategy(), func() bool { return exitAfter > 0 && received >= exitAfter })
	fmt.Printf("Shutting down... received %d messages\n", received)
}
