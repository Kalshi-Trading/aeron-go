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
	"os"

	"github.com/lirm/aeron-go/aeron/lossreport"
	"github.com/lirm/aeron-go/examples"
)

var lossFile = flag.String("lossfile", "", "loss report file (default <aeron dir>/loss-report.dat)")

func main() {
	flag.Parse()
	var report *lossreport.Report
	var err error
	if *lossFile != "" {
		report, err = lossreport.MapFile(*lossFile)
	} else {
		report, err = lossreport.Open(*examples.ExamplesConfig.AeronPrefix)
	}
	if err != nil {
		log.Fatal(err)
	}
	defer report.Close()

	fmt.Println(lossreport.CSVHeader)
	count := report.Read(lossreport.CSVEntryConsumer(os.Stdout))
	fmt.Printf("%d loss entries\n", count)
}
