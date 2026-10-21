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

// lossstat is a port of io.aeron.samples.LossStat: it prints the driver's loss report
// (<aeron dir>/loss-report.dat) as CSV. -lossfile overrides the report location.
package main

import (
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/lirm/aeron-go/examples"
	"github.com/lirm/aeron-go/examples/internal/cnc"
)

const dateFormat = "2006-01-02 15:04:05.000-0700"

var lossFile = flag.String("lossfile", "", "loss report file (default <aeron dir>/loss-report.dat)")

func main() {
	flag.Parse()
	path := *lossFile
	if path == "" {
		path = *examples.ExamplesConfig.AeronPrefix
	}
	report, err := cnc.OpenLossReport(path)
	if err != nil {
		log.Fatal(err)
	}
	defer report.Close()

	fmt.Println(cnc.LossReportCSVHeader)
	count := cnc.ReadLossReport(report.Buffer, func(e cnc.LossEntry) {
		fmt.Printf("%d,%d,%s,%s,%d,%d,%s,%s\n", e.ObservationCount, e.TotalBytesLost,
			time.UnixMilli(e.FirstObservationMs).Format(dateFormat), time.UnixMilli(e.LastObservationMs).Format(dateFormat),
			e.SessionID, e.StreamID, e.Channel, e.Source)
	})
	fmt.Printf("%d loss entries\n", count)
}
