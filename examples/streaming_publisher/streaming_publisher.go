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

// streaming_publisher is a port of io.aeron.samples.StreamingPublisher: it publishes -m messages
// of -len bytes (or random lengths with -random) as fast as possible and reports the rate.
package main

import (
	"context"
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
	size := *examples.ExamplesConfig.Size
	if size < 8 {
		log.Fatalf("Message length must be at least 8 bytes: %d", size)
	}
	fmt.Printf("Publishing to %s on stream id %d\n", channel, streamID)

	a, err := aeron.Connect(examples.NewContext())
	if err != nil {
		log.Fatalf("Failed to connect to media driver: %v", err)
	}
	defer a.Close()
	var pub *aeron.Publication
	if *examples.ExamplesConfig.Exclusive {
		pub, err = a.AddExclusivePublication(channel, streamID)
	} else {
		pub, err = a.AddPublication(channel, streamID)
	}
	if err != nil {
		log.Fatalf("Failed to add publication: %v", err)
	}
	defer pub.Close()

	reporter := examples.NewRateReporter(time.Second, nil)
	reportCtx, stopReporter := context.WithCancel(ctx)
	go reporter.Run(reportCtx)
	defer stopReporter()

	for !pub.IsConnected() && ctx.Err() == nil {
		time.Sleep(time.Millisecond)
	}

	buffer := atomic.MakeBuffer(make([]byte, size))
	nextLength := examples.LengthGenerator(*examples.ExamplesConfig.RandomLength, size)
	idler := examples.NewIdleStrategy()
	messages := *examples.ExamplesConfig.Messages
	fmt.Printf("Streaming %d messages of%s size %d bytes to %s on stream id %d\n", messages,
		map[bool]string{true: " random", false: ""}[*examples.ExamplesConfig.RandomLength], size, channel, streamID)

	backPressureCount := int64(0)
	sent := 0
	for ; sent < messages && ctx.Err() == nil; sent++ {
		length := nextLength()
		buffer.PutInt64(0, int64(sent))
		for {
			result := pub.Offer(buffer, 0, length, nil)
			if result > 0 {
				break
			}
			if examples.IsTerminalOfferResult(result) {
				log.Fatal(examples.OfferResultString(result))
			}
			backPressureCount++
			if ctx.Err() != nil {
				break
			}
			idler.Idle(0)
		}
		reporter.OnMessage(int64(length))
	}
	fmt.Printf("Done streaming. sent=%d backPressureRatio=%g\n", sent, float64(backPressureCount)/float64(max1(sent)))
	examples.Linger()
}

func max1(n int) int {
	if n < 1 {
		return 1
	}
	return n
}
