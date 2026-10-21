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

// backlogstat is a port of io.aeron.samples.BacklogStat: it prints, per stream, the publisher,
// sender, receiver and subscriber positions and the resulting backlogs.
package main

import (
	"flag"
	"log"
	"os"

	"github.com/lirm/aeron-go/examples"
	"github.com/lirm/aeron-go/examples/internal/cnc"
)

func main() {
	flag.Parse()
	file, err := cnc.Open(*examples.ExamplesConfig.AeronPrefix)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()
	cnc.PrintBacklog(os.Stdout, cnc.Backlog(cnc.Snapshot(file.Counters)))
}
