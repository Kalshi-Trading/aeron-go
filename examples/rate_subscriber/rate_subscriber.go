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

// rate_subscriber is a port of io.aeron.samples.RateSubscriber: it consumes messages and reports
// the receive rate once a second. With -exit N it stops after N messages.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/lirm/aeron-go/aeron"
	"github.com/lirm/aeron-go/examples"
)

func main() {
	flag.Parse()
	examples.SetupLogging()
	ctx, cancel := examples.SignalContext()
	defer cancel()

	channel, streamID := *examples.ExamplesConfig.Channel, int32(*examples.ExamplesConfig.StreamID)

	aeronCtx := examples.NewContext().
		AvailableImageHandler(examples.PrintAvailableImage).
		UnavailableImageHandler(examples.PrintUnavailableImage)
	a, err := aeron.Connect(aeronCtx)
	if err != nil {
		log.Fatalf("Failed to connect to media driver: %v", err)
	}
	defer a.Close()
	sub, err := a.AddSubscription(channel, streamID)
	if err != nil {
		log.Fatalf("Failed to add subscription: %v", err)
	}
	defer sub.Close()
	// Printed once registered so a publisher started after this line is not NotConnected.
	fmt.Printf("Subscribing to %s on stream id %d\n", channel, streamID)

	reporter := examples.NewRateReporter(time.Second, nil)
	reportCtx, stopReporter := context.WithCancel(ctx)
	go reporter.Run(reportCtx)
	defer stopReporter()

	exitAfter := int64(*examples.ExamplesConfig.ExitAfterMessages)
	examples.SubscriberLoop(ctx, sub, reporter.Handler(), *examples.ExamplesConfig.FragmentLimit,
		examples.NewIdleStrategy(), func() bool { return exitAfter > 0 && reporter.TotalMessages() >= exitAfter })
	fmt.Printf("Shutting down... received %d messages, %d bytes\n", reporter.TotalMessages(), reporter.TotalBytes())
}
