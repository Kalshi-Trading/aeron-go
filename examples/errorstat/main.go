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

// errorstat prints the distinct error log of a media driver, like io.aeron.samples.ErrorStat.
//
//	go run ./examples/errorstat -dir /dev/shm/aeron-compose
package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/lirm/aeron-go/aeron"
	"github.com/lirm/aeron-go/aeron/cnc"
	"github.com/lirm/aeron-go/aeron/errorlog"
)

func main() {
	dir := flag.String("dir", aeron.DefaultAeronDir+"/aeron-"+aeron.UserName, "aeron directory")
	flag.Parse()

	f, err := cnc.Open(*dir)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	fmt.Printf("Media driver pid=%d started %s, client liveness timeout %s, file page size %d\n",
		f.Pid(), time.UnixMilli(f.StartTimestampMs()).Format(errorlog.TimestampFormat),
		time.Duration(f.ClientLivenessTimeoutNs()), f.FilePageSize())
	errorlog.PrintErrorLog(f.ErrorLogBuffer(), os.Stdout)
}
