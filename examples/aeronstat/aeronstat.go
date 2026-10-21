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

// aeronstat is a port of io.aeron.samples.AeronStat: it prints the driver counters, optionally
// filtered by Java-style name=regex arguments:
//
//	aeronstat -p /dev/shm/aeron-compose [delay=<seconds>] [watch=<true|false>] [type=<p>] [identity=<p>]
//	          [session=<p>] [stream=<p>] [channel=<p>]
package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/lirm/aeron-go/examples"
	"github.com/lirm/aeron-go/examples/internal/cnc"
)

func main() {
	flag.Parse()
	filter, delay, watch, err := cnc.ParseFilterArgs(flag.Args())
	if err != nil {
		log.Fatal(err)
	}
	ctx, cancel := examples.SignalContext()
	defer cancel()

	file, err := cnc.Open(*examples.ExamplesConfig.AeronPrefix)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	for {
		if watch {
			fmt.Print("\u001b[2J\u001b[H")
		}
		printOutput(file, filter)
		if !watch {
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Duration(delay) * time.Second):
		}
	}
}

func printOutput(file *cnc.File, filter cnc.Filter) {
	now := time.Now()
	fmt.Printf("%s - Aeron Stat (CnC v%s), pid %d, heartbeat age %dms\n", now.Format("15:04:05"),
		file.SemanticVersion(), file.DriverPid(), file.DriverHeartbeatAgeMs(now))
	fmt.Println("======================================================================")
	infos := cnc.Snapshot(file.Counters)
	width := 1
	for _, c := range infos {
		if w := len(strconv.Itoa(int(c.ID))); w > width {
			width = w
		}
	}
	for _, c := range infos {
		if filter.Match(c) {
			fmt.Fprintf(os.Stdout, "%*d: %26s - %s\n", width, c.ID, groupThousands(c.Value), c.Label)
		}
	}
	fmt.Println("--")
}

func groupThousands(v int64) string {
	s := strconv.FormatInt(v, 10)
	neg := s[0] == '-'
	if neg {
		s = s[1:]
	}
	out := make([]byte, 0, len(s)+len(s)/3+1)
	for i, ch := range []byte(s) {
		if i > 0 && (len(s)-i)%3 == 0 {
			out = append(out, ',')
		}
		out = append(out, ch)
	}
	if neg {
		return "-" + string(out)
	}
	return string(out)
}
