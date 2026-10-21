// Copyright 2026 Kalshi Trading
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Command loginspector dumps an Aeron log buffer file in the same format as io.aeron.samples.LogInspector.
//
//	go run ./examples/loginspector [-limit N] [-ascii] [-skip-default-header] [-scan-over-zeroes] <logFileName>
package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/lirm/aeron-go/aeron/logbuffer"
)

func main() {
	limit := flag.Int("limit", 0, "dump limit in bytes per message (0 = unlimited)")
	ascii := flag.Bool("ascii", false, "dump payloads as ascii instead of hex")
	skip := flag.Bool("skip-default-header", false, "do not print the default frame header")
	scan := flag.Bool("scan-over-zeroes", false, "continue scanning over zeroed frames")
	flag.Parse()
	if flag.NArg() < 1 {
		fmt.Fprintln(os.Stderr, "Usage: loginspector [flags] <logFileName>")
		os.Exit(1)
	}
	lb, err := logbuffer.NewLogBuffers(flag.Arg(0))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer lb.Close()
	opts := logbuffer.InspectOptions{MessageDumpLimit: int32(*limit), ASCII: *ascii, SkipDefaultHeader: *skip,
		ScanOverZeroes: *scan}
	if err := logbuffer.Inspect(os.Stdout, lb, opts); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
