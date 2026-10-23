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

package systests

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"testing"
	"time"

	"github.com/lirm/aeron-go/aeron"
	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/logbuffer"
	"github.com/lirm/aeron-go/aeron/logbuffer/term"
	"github.com/stretchr/testify/require"
)

// TestReassemblyE2E has the Java 1.53.3 LargeMessagePublisher (docker/reassembly) send large fragmented messages
// through the dockerised Java media driver and checks that each Go assembler variant delivers them byte for byte,
// with the header of the first fragment (reserved value, term offset), unfragmented flags, the complete frame
// length and the same position Java's Publication.offer returned.
//
// Requires: AERON_E2E=1 and `docker compose -f docker/docker-compose.yml up -d --build archiving-driver`.
func TestReassemblyE2E(t *testing.T) {
	if os.Getenv("AERON_E2E") != "1" {
		t.Skip("set AERON_E2E=1 and start the docker archiving-driver to run")
	}
	aeronDir := os.Getenv("AERON_DIR")
	if aeronDir == "" {
		aeronDir = "/dev/shm/aeron-compose"
	}

	cases := []struct {
		name     string
		kind     string
		channel  string
		streamID int32
		count    int
	}{
		{"FragmentAssembler", "fragment", "aeron:udp?endpoint=localhost:20131", 1031, 120},
		{"ControlledFragmentAssembler", "controlled", "aeron:udp?endpoint=localhost:20132", 1032, 120},
		{"ImageFragmentAssembler", "image", "aeron:udp?endpoint=localhost:20133", 1033, 120},
		{"ImageControlledFragmentAssembler", "imageControlled", "aeron:udp?endpoint=localhost:20134", 1034, 120},
		// small terms and a larger MTU: many term rotations, padding frames and different fragment sizes
		{"FragmentAssemblerSmallTerm", "fragment", "aeron:udp?endpoint=localhost:20135|term-length=65536|mtu=4096", 1035, 400},
		{"ImageControlledSmallTerm", "imageControlled", "aeron:udp?endpoint=localhost:20136|term-length=65536|mtu=8192", 1036, 400},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			runReassemblyCase(t, aeronDir, tc.kind, tc.channel, tc.streamID, tc.count)
		})
	}
}

type reassemblyChecker struct {
	t         *testing.T
	count     int
	received  int
	sessionID int32
	pending   *pendingMessage
	errs      []string
}

type pendingMessage struct {
	index         int
	data          []byte
	reserved      int64
	termOffset    int32
	flags         uint8
	frameLength   int32
	position      int64
	fragmentedLen int32
}

func e2ePattern(i, n int) []byte {
	b := make([]byte, n)
	for j := range b {
		b[j] = byte(i*31 + j*7 + (j >> 8))
	}
	return b
}

func (c *reassemblyChecker) failf(format string, args ...interface{}) {
	if len(c.errs) < 20 {
		c.errs = append(c.errs, fmt.Sprintf(format, args...))
	}
}

func (c *reassemblyChecker) onMessage(buffer *atomic.Buffer, offset, length int32, header *logbuffer.Header) {
	c.sessionID = header.SessionID()
	if c.pending == nil {
		c.pending = &pendingMessage{
			index:         c.received,
			data:          buffer.GetBytesArray(offset, length),
			reserved:      header.ReservedValue(),
			termOffset:    header.TermOffset(),
			flags:         header.Flags(),
			frameLength:   header.FrameLength(),
			position:      header.Position(),
			fragmentedLen: header.FragmentedFrameLength(),
		}
		return
	}

	m := c.pending
	c.pending = nil
	c.received++
	if length != 16 {
		c.failf("message %d: expected 16 byte trailer, got %d bytes", m.index, length)
		return
	}
	wantPosition := buffer.GetInt64(offset)
	wantIndex := int(buffer.GetInt32(offset + 8))
	wantLength := int(buffer.GetInt32(offset + 12))
	if wantIndex != m.index {
		c.failf("message %d: trailer index %d", m.index, wantIndex)
	}
	if len(m.data) != wantLength {
		c.failf("message %d: length %d, want %d", m.index, len(m.data), wantLength)
	} else if !bytes.Equal(m.data, e2ePattern(wantIndex, wantLength)) {
		c.failf("message %d: payload mismatch (length %d)", m.index, wantLength)
	}
	if m.position != wantPosition {
		c.failf("message %d: header position %d, Java offer position %d (len %d)", m.index, m.position, wantPosition, wantLength)
	}
	if int(m.reserved>>32) != wantIndex || int32(m.reserved) != m.termOffset {
		c.failf("message %d: reserved value %#x not from first fragment (termOffset %d)", m.index, m.reserved, m.termOffset)
	}
	if m.flags != 0xC0 {
		c.failf("message %d: flags %#x", m.index, m.flags)
	}
	if m.frameLength != logbuffer.DataFrameHeader.Length+int32(wantLength) {
		c.failf("message %d: frame length %d", m.index, m.frameLength)
	}
}

func runReassemblyCase(t *testing.T, aeronDir, kind, channel string, streamID int32, count int) {
	checker := &reassemblyChecker{t: t, count: count}
	images := make(chan aeron.Image, 4)

	var fragment *aeron.FragmentAssembler
	var controlled *aeron.ControlledFragmentAssembler
	onUnavailable := func(image aeron.Image) {}
	controlledHandler := func(buffer *atomic.Buffer, offset, length int32, header *logbuffer.Header) term.ControlledPollAction {
		checker.onMessage(buffer, offset, length, header)
		return term.ControlledPollActionCommit
	}
	switch kind {
	case "fragment":
		fragment = aeron.NewFragmentAssembler(checker.onMessage, 0)
		onUnavailable = fragment.OnUnavailableImage
	case "controlled":
		controlled = aeron.NewControlledFragmentAssembler(controlledHandler, 0)
		onUnavailable = controlled.OnUnavailableImage
	}

	ctx := aeron.NewContext().AeronDir(aeronDir).MediaDriverTimeout(10 * time.Second)
	client, err := aeron.Connect(ctx)
	require.NoError(t, err)
	defer client.Close()

	sub, err := client.AddSubscriptionWithHandlers(channel, streamID,
		func(image aeron.Image) { images <- image }, onUnavailable)
	require.NoError(t, err)
	defer sub.Close()

	runCtx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(runCtx, "docker", "compose", "-f", "../docker/reassembly.compose.yml",
		"run", "--rm", "reassembly-publisher", channel, strconv.Itoa(int(streamID)), strconv.Itoa(count))
	cmd.Env = append(os.Environ(), "UID="+strconv.Itoa(os.Getuid()), "GID="+strconv.Itoa(os.Getgid()),
		"AERON_PROPS=-Daeron.dir="+aeronDir)
	var out bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &out
	require.NoError(t, cmd.Start())
	publisherDone := make(chan error, 1)
	go func() { publisherDone <- cmd.Wait() }()

	var image aeron.Image
	var imageFragment *aeron.ImageFragmentAssembler
	var imageControlled *aeron.ImageControlledFragmentAssembler
	if kind == "image" || kind == "imageControlled" {
		select {
		case image = <-images:
		case err := <-publisherDone:
			t.Fatalf("publisher exited before image available: %v\n%s", err, out.String())
		case <-runCtx.Done():
			t.Fatalf("no image\n%s", out.String())
		}
		imageFragment = aeron.NewImageFragmentAssembler(checker.onMessage, 0)
		imageControlled = aeron.NewImageControlledFragmentAssembler(controlledHandler, 0)
	}

	var publisherErr error
	publisherExited := false
	deadline := time.Now().Add(3 * time.Minute)
	for checker.received < count && time.Now().Before(deadline) && len(checker.errs) == 0 {
		var n int
		switch kind {
		case "fragment":
			n = sub.Poll(fragment.OnFragment, 64)
		case "controlled":
			n = sub.ControlledPoll(controlled.OnFragment, 64)
		case "image":
			n = image.Poll(imageFragment.OnFragment, 64)
		case "imageControlled":
			n = image.ControlledPoll(imageControlled.OnFragment, 64)
		}
		if n == 0 {
			if !publisherExited {
				select {
				case publisherErr = <-publisherDone:
					publisherExited = true
					// keep draining what has already been published
					deadline = time.Now().Add(10 * time.Second)
				default:
				}
			}
			time.Sleep(100 * time.Microsecond)
		}
	}
	if !publisherExited {
		publisherErr = <-publisherDone
	}

	t.Logf("publisher output:\n%s", out.String())
	require.NoError(t, publisherErr)
	require.Empty(t, checker.errs)
	require.Equal(t, count, checker.received, "messages received")
	switch kind {
	case "fragment":
		require.False(t, fragment.IsSessionAssembling(checker.sessionID))
	case "controlled":
		require.False(t, controlled.IsSessionAssembling(checker.sessionID))
	case "image":
		require.False(t, imageFragment.IsAssembling())
	case "imageControlled":
		require.False(t, imageControlled.IsAssembling())
	}
}
