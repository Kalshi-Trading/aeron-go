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

// multiple_publishers_with_fragmentation is a port of io.aeron.samples.MultiplePublishersWithFragmentation:
// it publishes 10000 and 9000 byte messages (larger than the MTU, so they are fragmented) on
// stream -sid and -sid+1 of the same channel. Run multiple_subscribers_with_fragment_assembly to receive.
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
	_ = flag.Set("m", "10")
	flag.Parse()
	examples.SetupLogging()
	ctx, cancel := examples.SignalContext()
	defer cancel()

	channel := *examples.ExamplesConfig.Channel
	stream1 := int32(*examples.ExamplesConfig.StreamID)
	stream2 := stream1 + 1
	fmt.Printf("Publishing to %s on stream id %d and stream id %d\n", channel, stream1, stream2)

	a, err := aeron.Connect(examples.NewContext())
	if err != nil {
		log.Fatalf("Failed to connect to media driver: %v", err)
	}
	defer a.Close()
	pub1, err := a.AddPublication(channel, stream1)
	if err != nil {
		log.Fatalf("Failed to add publication: %v", err)
	}
	defer pub1.Close()
	pub2, err := a.AddPublication(channel, stream2)
	if err != nil {
		log.Fatalf("Failed to add publication: %v", err)
	}
	defer pub2.Close()

	buf1 := atomic.MakeBuffer(make([]byte, 10000))
	buf2 := atomic.MakeBuffer(make([]byte, 9000))
	sent1, sent2 := 0, 0
	messages := *examples.ExamplesConfig.Messages
	offer := func(pub *aeron.Publication, buf *atomic.Buffer, stream int32, n int) bool {
		msg := []byte(fmt.Sprintf("Hello World! %d", n))
		buf.PutBytesArray(0, &msg, 0, int32(len(msg)))
		result := pub.Offer(buf, 0, buf.Capacity(), nil)
		if result > 0 {
			fmt.Printf("Successfully sent data on stream id %d and data length %d at offset %d\n",
				stream, buf.Capacity(), result)
			return true
		}
		fmt.Printf("%s for stream id %d\n", examples.OfferResultString(result), stream)
		if examples.IsTerminalOfferResult(result) {
			log.Fatal("publication is closed")
		}
		return false
	}
	for (sent1 < messages || sent2 < messages) && ctx.Err() == nil {
		if sent1 < messages && offer(pub1, buf1, stream1, sent1+1) {
			sent1++
		}
		if sent2 < messages && offer(pub2, buf2, stream2, sent2+1) {
			sent2++
		}
		time.Sleep(100 * time.Millisecond)
	}
	fmt.Printf("Done sending total messages for stream id %d = %d and stream id %d = %d\n",
		stream1, sent1, stream2, sent2)
	examples.Linger()
}
