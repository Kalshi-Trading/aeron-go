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

// pong is a port of io.aeron.samples.Pong: it echoes every ping message back on the pong channel.
package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"runtime/pprof"
	"time"

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

	if *examples.ExamplesConfig.ProfilerEnabled {
		f, err := os.Create(fmt.Sprintf("pong-%d.pprof", time.Now().Unix()))
		if err != nil {
			log.Fatal(err)
		}
		if err := pprof.StartCPUProfile(f); err == nil {
			defer pprof.StopCPUProfile()
		}
	}

	pingChannel, pingStream := *examples.PingPongConfig.PingChannel, int32(*examples.PingPongConfig.PingStreamID)
	pongChannel, pongStream := *examples.PingPongConfig.PongChannel, int32(*examples.PingPongConfig.PongStreamID)
	fmt.Printf("Subscribing Ping at %s on stream id %d\n", pingChannel, pingStream)
	fmt.Printf("Publishing Pong at %s on stream id %d\n", pongChannel, pongStream)

	a, err := aeron.Connect(examples.NewContext())
	if err != nil {
		log.Fatalf("Failed to connect to media driver: %v", err)
	}
	defer a.Close()

	sub, err := a.AddSubscription(pingChannel, pingStream)
	if err != nil {
		log.Fatalf("Failed to add ping subscription: %v", err)
	}
	defer sub.Close()
	var pub aeron.Publisher
	if *examples.ExamplesConfig.Exclusive {
		pub, err = a.AddExclusivePublication(pongChannel, pongStream)
	} else {
		pub, err = a.AddPublication(pongChannel, pongStream)
	}
	if err != nil {
		log.Fatalf("Failed to add pong publication: %v", err)
	}
	defer pub.Close()

	idler := examples.NewIdleStrategy()
	echoed := 0
	handler := func(buffer *atomic.Buffer, offset int32, length int32, _ *logbuffer.Header) {
		for ctx.Err() == nil {
			result := pub.Offer(buffer, offset, length, nil)
			if result > 0 {
				echoed++
				return
			}
			if examples.IsTerminalOfferResult(result) {
				log.Fatal(examples.OfferResultString(result))
			}
			idler.Idle(0)
		}
	}
	exitAfter := *examples.ExamplesConfig.ExitAfterMessages
	for ctx.Err() == nil && (exitAfter == 0 || echoed < exitAfter) {
		idler.Idle(sub.Poll(handler, *examples.ExamplesConfig.FragmentLimit))
	}
	fmt.Printf("Shutting down... echoed %d messages\n", echoed)
}
