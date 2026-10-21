/*
Copyright 2026 Kalshi Trading

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

// aeronstat dumps the media driver's CnC metadata and counters, like io.aeron.samples.AeronStat.
//
//	go run ./examples/aeronstat -dir /dev/shm/aeron-compose [-watch 1s] [-type 14] [-filter regex]
package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"regexp"
	"time"

	"github.com/lirm/aeron-go/aeron"
	"github.com/lirm/aeron-go/aeron/cnc"
	"github.com/lirm/aeron-go/aeron/counters"
	"github.com/lirm/aeron-go/aeron/util"
)

func main() {
	dir := flag.String("dir", aeron.DefaultAeronDir+"/aeron-"+aeron.UserName, "aeron directory")
	watch := flag.Duration("watch", 0, "refresh interval (0 prints once)")
	typeID := flag.Int("type", -1, "only show counters of this type id")
	filter := flag.String("filter", "", "only show counters whose label matches this regex")
	flag.Parse()

	var re *regexp.Regexp
	if *filter != "" {
		re = regexp.MustCompile(*filter)
	}

	f, err := cnc.Open(*dir)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()
	reader := f.CountersReader()

	for {
		if *watch > 0 {
			fmt.Print("\u001b[2J\u001b[H")
		}
		fmt.Printf("%s - Aeron Stat (CnC v%s), pid %d, uptime %s\n",
			time.Now().Format("15:04:05"), util.SemanticVersionToString(uint32(f.Version())), f.Pid(),
			time.Since(time.UnixMilli(f.StartTimestampMs())).Truncate(time.Millisecond))
		fmt.Println("======================================================================")
		reader.Scan(func(c counters.Counter) {
			if (*typeID >= 0 && c.TypeId != int32(*typeID)) || (re != nil && !re.MatchString(c.Label)) {
				return
			}
			fmt.Printf("%3d: %20d - %s\n", c.Id, c.Value, c.Label)
		})
		fmt.Println("--")
		if *watch <= 0 {
			os.Exit(0)
		}
		time.Sleep(*watch)
	}
}
