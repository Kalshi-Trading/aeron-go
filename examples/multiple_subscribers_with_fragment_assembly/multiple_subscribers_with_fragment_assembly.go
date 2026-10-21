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

// multiple_subscribers_with_fragment_assembly is a port of io.aeron.samples.MultipleSubscribersWithFragmentAssembly:
// it subscribes to stream -sid and -sid+1 and reassembles fragmented messages with a FragmentAssembler.
package main

import (
	"flag"
	"fmt"
	"log"

	"github.com/lirm/aeron-go/aeron"
	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/logbuffer"
	"github.com/lirm/aeron-go/aeron/logbuffer/term"
	"github.com/lirm/aeron-go/examples"
)

func main() {
	flag.Parse()
	examples.SetupLogging()
	ctx, cancel := examples.SignalContext()
	defer cancel()

	channel := *examples.ExamplesConfig.Channel
	stream1 := int32(*examples.ExamplesConfig.StreamID)
	stream2 := stream1 + 1

	aeronCtx := examples.NewContext().
		AvailableImageHandler(examples.PrintAvailableImage).
		UnavailableImageHandler(examples.PrintUnavailableImage)
	a, err := aeron.Connect(aeronCtx)
	if err != nil {
		log.Fatalf("Failed to connect to media driver: %v", err)
	}
	defer a.Close()
	sub1, err := a.AddSubscription(channel, stream1)
	if err != nil {
		log.Fatalf("Failed to add subscription: %v", err)
	}
	defer sub1.Close()
	sub2, err := a.AddSubscription(channel, stream2)
	if err != nil {
		log.Fatalf("Failed to add subscription: %v", err)
	}
	defer sub2.Close()
	// Printed once registered so a publisher started after this line is not NotConnected.
	fmt.Printf("Subscribing to %s on stream ids %d and %d\n", channel, stream1, stream2)

	received := 0
	reassembled := func(stream int32, expectedLength int32) term.FragmentHandler {
		return func(buffer *atomic.Buffer, offset int32, length int32, header *logbuffer.Header) {
			fmt.Printf("message to stream %d from session %x term id %x term offset %d (%d@%d)\n",
				stream, header.SessionId(), header.TermId(), header.Offset(), length, offset)
			if length != expectedLength {
				fmt.Printf("Received message was not assembled properly; received length was %d, but was expecting %d\n",
					length, expectedLength)
			}
			received++
		}
	}
	asm1 := aeron.NewFragmentAssembler(reassembled(stream1, 10000), aeron.DefaultFragmentAssemblyBufferLength)
	asm2 := aeron.NewFragmentAssembler(reassembled(stream2, 9000), aeron.DefaultFragmentAssemblyBufferLength)
	idler := examples.NewIdleStrategy()
	limit := *examples.ExamplesConfig.FragmentLimit
	exitAfter := *examples.ExamplesConfig.ExitAfterMessages
	for ctx.Err() == nil && (exitAfter == 0 || received < exitAfter) {
		idler.Idle(sub1.Poll(asm1.OnFragment, limit) + sub2.Poll(asm2.OnFragment, limit))
	}
	fmt.Printf("Shutting down... received %d messages\n", received)
}
