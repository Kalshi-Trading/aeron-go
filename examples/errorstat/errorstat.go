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
	"os"

	"github.com/lirm/aeron-go/aeron/cnc"
	"github.com/lirm/aeron-go/aeron/errorlog"
	"github.com/lirm/aeron-go/examples"
)

func main() {
	flag.Parse()
	dir := *examples.ExamplesConfig.AeronPrefix
	file, err := cnc.Open(dir)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()
	fmt.Println("Command `n Control file " + cnc.Path(dir))
	errorlog.PrintErrorLog(file.ErrorLogBuffer(), os.Stdout)
}
