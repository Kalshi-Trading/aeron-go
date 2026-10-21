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

// errorstat is a port of io.aeron.samples.ErrorStat: it prints the distinct errors recorded in the
// driver's CnC error log.
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

func main() {
	flag.Parse()
	file, err := cnc.Open(*examples.ExamplesConfig.AeronPrefix)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()
	fmt.Println("Command `n Control file " + cnc.FileName(*examples.ExamplesConfig.AeronPrefix))

	count := cnc.ReadErrors(file.Meta.ErrorBuf.Get(), 0, func(o cnc.ErrorObservation) {
		fmt.Println()
		fmt.Printf("%d observations from %s to %s for:\n", o.ObservationCount,
			time.UnixMilli(o.FirstObservationMs).Format(dateFormat), time.UnixMilli(o.LastObservationMs).Format(dateFormat))
		fmt.Println(o.EncodedError)
	})
	fmt.Println()
	fmt.Printf("%d distinct errors observed.\n", count)
}
