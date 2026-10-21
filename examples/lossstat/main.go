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

// lossstat prints the media driver loss report as CSV, like io.aeron.samples.LossStat.
//
//	go run ./examples/lossstat -dir /dev/shm/aeron-compose
package main

import (
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/lirm/aeron-go/aeron"
	"github.com/lirm/aeron-go/aeron/lossreport"
)

func main() {
	dir := flag.String("dir", aeron.DefaultAeronDir+"/aeron-"+aeron.UserName, "aeron directory")
	flag.Parse()

	r, err := lossreport.Open(*dir)
	if err != nil {
		log.Fatal(err)
	}
	defer r.Close()
	fmt.Println(lossreport.CSVHeader)
	n := r.Read(lossreport.CSVEntryConsumer(os.Stdout))
	fmt.Printf("%d loss entries\n", n)
}
