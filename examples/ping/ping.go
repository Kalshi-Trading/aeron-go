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

// ping is a port of io.aeron.samples.Ping: it measures round-trip latency against pong.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"runtime/pprof"
	"time"

	"github.com/HdrHistogram/hdrhistogram-go"
	"github.com/lirm/aeron-go/aeron"
	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/idlestrategy"
	"github.com/lirm/aeron-go/aeron/logbuffer"
	"github.com/lirm/aeron-go/examples"
)

func main() {
	flag.Parse()
	examples.SetupLogging()
	ctx, cancel := examples.SignalContext()
	defer cancel()

	if *examples.ExamplesConfig.ProfilerEnabled {
		f, err := os.Create(fmt.Sprintf("ping-%d.pprof", time.Now().Unix()))
		if err != nil {
			log.Fatal(err)
		}
		if err := pprof.StartCPUProfile(f); err == nil {
			defer pprof.StopCPUProfile()
		}
	}

	pingChannel, pingStream := *examples.PingPongConfig.PingChannel, int32(*examples.PingPongConfig.PingStreamID)
	pongChannel, pongStream := *examples.PingPongConfig.PongChannel, int32(*examples.PingPongConfig.PongStreamID)
	fmt.Printf("Publishing Ping at %s on stream id %d\n", pingChannel, pingStream)
	fmt.Printf("Subscribing Pong at %s on stream id %d\n", pongChannel, pongStream)
	fmt.Printf("Message length of %d bytes\n", *examples.ExamplesConfig.Size)

	a, err := aeron.Connect(examples.NewContext())
	if err != nil {
		log.Fatalf("Failed to connect to media driver: %v", err)
	}
	defer a.Close()

	sub, err := a.AddSubscription(pongChannel, pongStream)
	if err != nil {
		log.Fatalf("Failed to add pong subscription: %v", err)
	}
	defer sub.Close()
	var pub *aeron.Publication
	if *examples.ExamplesConfig.Exclusive {
		pub, err = a.AddExclusivePublication(pingChannel, pingStream)
	} else {
		pub, err = a.AddPublication(pingChannel, pingStream)
	}
	if err != nil {
		log.Fatalf("Failed to add ping publication: %v", err)
	}
	defer pub.Close()

	fmt.Println("Waiting for new image from Pong...")
	for !sub.IsConnected() || !pub.IsConnected() {
		if ctx.Err() != nil {
			return
		}
		time.Sleep(time.Millisecond)
	}

	size := *examples.ExamplesConfig.Size
	if size < 8 {
		size = 8
	}
	hist := hdrhistogram.New(1, 10_000_000_000, 3)
	p := &pinger{pub: pub, sub: sub, hist: hist, idler: examples.NewIdleStrategy(),
		buffer: atomic.MakeBuffer(make([]byte, size))}

	for i := 0; i < *examples.ExamplesConfig.WarmupIterations && ctx.Err() == nil; i++ {
		fmt.Printf("Warming up the media driver with %d messages of length %d bytes\n",
			*examples.ExamplesConfig.WarmupMessages, *examples.ExamplesConfig.Size)
		p.roundTrips(ctx, *examples.ExamplesConfig.WarmupMessages)
	}
	hist.Reset()
	fmt.Printf("Pinging %d messages\n", *examples.ExamplesConfig.Messages)
	start := time.Now()
	p.roundTrips(ctx, *examples.ExamplesConfig.Messages)
	fmt.Printf("Histogram of RTT latencies in microseconds (%d round trips in %v).\n", hist.TotalCount(), time.Since(start))
	OutputPercentileDistribution(os.Stdout, hist, 1000.0)
}

type pinger struct {
	pub    *aeron.Publication
	sub    *aeron.Subscription
	hist   *hdrhistogram.Histogram
	idler  idlestrategy.Idler
	buffer *atomic.Buffer
}

func (p *pinger) roundTrips(ctx context.Context, count int) {
	length := p.buffer.Capacity()
	handler := func(buffer *atomic.Buffer, offset int32, _ int32, _ *logbuffer.Header) {
		sent := buffer.GetInt64(offset)
		_ = p.hist.RecordValue(time.Now().UnixNano() - sent)
	}
	for i := 0; i < count && ctx.Err() == nil; i++ {
		p.buffer.PutInt64(0, time.Now().UnixNano())
		for {
			result := p.pub.Offer(p.buffer, 0, length, nil)
			if result > 0 {
				break
			}
			if examples.IsTerminalOfferResult(result) {
				log.Fatal(examples.OfferResultString(result))
			}
			if ctx.Err() != nil {
				return
			}
			p.idler.Idle(0)
		}
		for ctx.Err() == nil {
			fragments := p.sub.Poll(handler, 1)
			if fragments > 0 {
				break
			}
			p.idler.Idle(fragments)
		}
	}
}

// OutputPercentileDistribution prints the histogram like HdrHistogram's outputPercentileDistribution.
func OutputPercentileDistribution(out io.Writer, h *hdrhistogram.Histogram, scalingFactor float64) {
	fmt.Fprint(out, "Value     Percentile TotalCount 1/(1-Percentile)\n\n")
	for _, b := range h.CumulativeDistribution() {
		pct := b.Quantile / 100.0
		fmt.Fprintf(out, "%12.3f %2.12f  %10d  %14.2f\n", float64(b.ValueAt)/scalingFactor, pct, b.Count, 1/(1-pct))
	}
	fmt.Fprintf(out, "#[Mean    = %12.3f, StdDeviation   = %12.3f]\n", h.Mean()/scalingFactor, h.StdDev()/scalingFactor)
	fmt.Fprintf(out, "#[Max     = %12.3f, Total count    = %12d]\n", float64(h.Max())/scalingFactor, h.TotalCount())
}
