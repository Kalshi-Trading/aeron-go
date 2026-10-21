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

// exclusive_throughput is the Go equivalent of io.aeron.samples.EmbeddedExclusiveThroughput. Go has
// no embedded media driver, so it connects to an external driver (e.g. the docker Java driver) and
// runs an exclusive publisher and a subscriber in the same process, reporting throughput, until -m
// messages have been received.
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
	messages := *examples.ExamplesConfig.Messages
	fmt.Printf("Streaming %d messages of size %d bytes to %s on stream id %d\n", messages, size, channel, streamID)

	a, err := aeron.Connect(examples.NewContext())
	if err != nil {
		log.Fatalf("Failed to connect to media driver: %v", err)
	}
	defer a.Close()
	sub, err := a.AddSubscription(channel, streamID)
	if err != nil {
		log.Fatalf("Failed to add subscription: %v", err)
	}
	defer sub.Close()
	pub, err := a.AddExclusivePublication(channel, streamID)
	if err != nil {
		log.Fatalf("Failed to add exclusive publication: %v", err)
	}
	defer pub.Close()

	reporter := examples.NewRateReporter(time.Second, nil)
	runCtx, stop := context.WithCancel(ctx)
	defer stop()
	go reporter.Run(runCtx)

	done := make(chan struct{})
	go func() {
		defer close(done)
		examples.SubscriberLoop(runCtx, sub, reporter.Handler(), *examples.ExamplesConfig.FragmentLimit,
			examples.NewIdleStrategy(), func() bool { return reporter.TotalMessages() >= int64(messages) })
	}()

	for !pub.IsConnected() && ctx.Err() == nil {
		time.Sleep(time.Millisecond)
	}
	start := time.Now()
	buffer := atomic.MakeBuffer(make([]byte, size))
	idler := examples.NewIdleStrategy()
	backPressureCount := int64(0)
	for i := 0; i < messages && ctx.Err() == nil; i++ {
		buffer.PutInt64(0, int64(i))
		for pub.Offer(buffer, 0, int32(size), nil) < 0 {
			backPressureCount++
			if ctx.Err() != nil {
				break
			}
			idler.Idle(0)
		}
	}
	<-done
	elapsed := time.Since(start)
	fmt.Printf("Done streaming. received=%d in %v (%.4g msgs/sec) backPressureRatio=%g\n", reporter.TotalMessages(),
		elapsed, float64(reporter.TotalMessages())/elapsed.Seconds(), float64(backPressureCount)/float64(messages))
}
