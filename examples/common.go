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

// Package examples holds configuration and helpers shared by the sample programs. Defaults mirror
// io.aeron.samples.SampleConfiguration from the Java client so Go and Java samples interoperate.
package examples

import (
	"flag"
	"os"

	"github.com/lirm/aeron-go/aeron"
)

// DefaultAeronDir returns $AERON_DIR if set, otherwise the aeron.Context default (<tmp>/aeron-<user>).
func DefaultAeronDir() string {
	if dir := os.Getenv("AERON_DIR"); dir != "" {
		return dir
	}
	return aeron.DefaultAeronDir + "/aeron-" + aeron.UserName
}

// ExamplesConfig holds the flags shared by the samples (see SampleConfiguration.java).
var ExamplesConfig = struct {
	AeronPrefix       *string
	ProfilerEnabled   *bool
	DriverTo          *int64
	StreamID          *int
	Channel           *string
	Messages          *int
	Size              *int
	LoggingOn         *bool
	FragmentLimit     *int
	LingerMs          *int64
	Exclusive         *bool
	RandomLength      *bool
	IdleStrategy      *string
	WarmupMessages    *int
	WarmupIterations  *int
	ExitAfterMessages *int
}{
	flag.String("p", DefaultAeronDir(), "aeron directory containing cnc.dat (default $AERON_DIR or <tmp>/aeron-<user>)"),
	flag.Bool("prof", false, "enable CPU profiling"),
	flag.Int64("to", 10000, "driver liveliness timeout in ms"),
	flag.Int("sid", 1001, "stream id"),
	flag.String("chan", "aeron:udp?endpoint=localhost:20121", "channel"),
	flag.Int("m", 10_000_000, "number of messages to send"),
	flag.Int("len", 32, "message length in bytes"),
	flag.Bool("l", false, "enable debug logging"),
	flag.Int("frags", 10, "fragment count limit per poll"),
	flag.Int64("linger", 0, "linger timeout in ms after sending"),
	flag.Bool("exclusive", false, "use exclusive publications"),
	flag.Bool("random", false, "use random message lengths between 8 and -len"),
	flag.String("idle", "busy", "idle strategy: busy|yield|sleep|backoff"),
	flag.Int("wm", 10_000, "warmup messages per iteration (ping)"),
	flag.Int("wi", 10, "warmup iterations (ping)"),
	flag.Int("exit", 0, "subscribers exit after receiving this many messages (0 = run until interrupted)"),
}

// PingPongConfig holds the ping/pong channels (see SampleConfiguration.java).
var PingPongConfig = struct {
	PongStreamID *int
	PingStreamID *int
	PongChannel  *string
	PingChannel  *string
}{
	flag.Int("S", 1003, "streamId to use for pong"),
	flag.Int("s", 1002, "streamId to use for ping"),
	flag.String("C", "aeron:udp?endpoint=localhost:20124", "pong channel"),
	flag.String("c", "aeron:udp?endpoint=localhost:20123", "ping channel"),
}
