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

package logbuffer_test

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lirm/aeron-go/aeron"
	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/logbuffer"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestE2EMetadataMatchesJava runs against the docker Java 1.53.3 driver (docker/docker-compose.yml) and compares
// the metadata and Inspect output for real publication and image log buffers with Java's LogInspector and a
// LogMetaDump that prints every LogBufferDescriptor field (docker/logbuffer.compose.yml).
//
//	AERON_E2E=1 AERON_DIR=/dev/shm/aeron-compose go test ./aeron/logbuffer/ -run E2E -v
func TestE2EMetadataMatchesJava(t *testing.T) {
	if os.Getenv("AERON_E2E") != "1" {
		t.Skip("set AERON_E2E=1 to run against the docker Java driver")
	}
	dir := os.Getenv("AERON_DIR")
	if dir == "" {
		dir = "/dev/shm/aeron-compose"
	}
	a, err := aeron.Connect(aeron.NewContext().AeronDir(dir).MediaDriverTimeout(10 * time.Second))
	require.NoError(t, err)
	defer a.Close()

	endpoint := "localhost:20199"
	pubChannel := "aeron:udp?endpoint=" + endpoint + "|term-length=128k|mtu=4096|linger=0|sparse=true|ssc=true" +
		"|so-sndbuf=262144|max-resend=7"
	subChannel := "aeron:udp?endpoint=" + endpoint + "|rcv-wnd=65536|so-rcvbuf=262144|rejoin=false|tether=false" +
		"|group=false|reliable=true|sparse=false"
	const streamID = 4242
	sub, err := a.AddSubscription(subChannel, streamID)
	require.NoError(t, err)
	defer sub.Close()
	pub, err := a.AddExclusivePublication(pubChannel, streamID)
	require.NoError(t, err)
	defer pub.Close()

	deadline := time.Now().Add(10 * time.Second)
	for !pub.IsConnected() || sub.ImageCount() == 0 {
		require.True(t, time.Now().Before(deadline), "publication did not connect")
		time.Sleep(10 * time.Millisecond)
	}

	payload := make([]byte, 10000)
	for i := range payload {
		payload[i] = byte(i)
	}
	sizes := []int32{5, 100, 1000, 10000}
	for i := 0; i < 12; i++ {
		length := sizes[i%len(sizes)]
		buf := atomic.MakeBuffer(payload[:length])
		for pub.Offer(buf, 0, length, nil) < 0 {
			require.True(t, time.Now().Before(deadline), "offer failed")
			time.Sleep(time.Millisecond)
		}
	}
	image := sub.ImageBySessionID(pub.SessionID())
	require.NotNil(t, image)
	var lastHeaderPosition int64
	for image.Position() < pub.Position() {
		require.True(t, time.Now().Before(deadline), "subscriber did not catch up")
		sub.Poll(func(_ *atomic.Buffer, _ int32, _ int32, h *logbuffer.Header) {
			lastHeaderPosition = h.Position()
		}, 100)
	}
	assert.Equal(t, pub.Position(), lastHeaderPosition)
	time.Sleep(200 * time.Millisecond) // let the driver settle status messages

	files := map[string]string{
		"publication": filepath.Join(dir, "publications", fmt.Sprintf("%d.logbuffer", pub.RegistrationID())),
		"image":       filepath.Join(dir, "images", fmt.Sprintf("%d.logbuffer", image.CorrelationID())),
	}
	for kind, file := range files {
		t.Run(kind, func(t *testing.T) {
			lb, err := logbuffer.NewLogBuffers(file)
			require.NoError(t, err)
			defer lb.Close()
			m := lb.Meta()

			assert.Equal(t, int32(128*1024), m.TermLength())
			assert.Equal(t, int32(4096), m.MtuLength())
			assert.Equal(t, pub.InitialTermID(), m.InitialTermID())
			if kind == "publication" {
				assert.Equal(t, logbuffer.LogBufferTypeExclusivePublication, m.LogType())
				assert.True(t, m.Sparse())
				assert.True(t, m.SpiesSimulateConnection())
				assert.Equal(t, int32(7), m.MaxResend())
				assert.Equal(t, int64(0), m.LingerTimeoutNs())
				assert.Equal(t, pub.RegistrationID(), m.CorrelationID())
				shift := logbuffer.PositionBitsToShift(m.TermLength())
				rawTail := m.ActiveRawTailVolatile()
				assert.Equal(t, pub.Position(), logbuffer.ComputePosition(logbuffer.TermID(rawTail),
					logbuffer.TermOffset(rawTail, int64(m.TermLength())), shift, m.InitialTermID()))
			} else {
				assert.Equal(t, logbuffer.LogBufferTypePublicationImage, m.LogType())
				assert.Equal(t, int32(65536), m.ReceiverWindowLength())
				assert.False(t, m.Rejoin())
				assert.False(t, m.Tether())
				assert.True(t, m.Reliable())
			}

			javaMeta := runJava(t, "/opt/aeron/LogMetaDump.java", file)
			assert.Equal(t, strings.TrimSpace(javaMeta), strings.TrimSpace(goMetaDump(m)))

			javaDump := runJava(t, "io.aeron.samples.LogInspector", file, "64")
			var goDump bytes.Buffer
			require.NoError(t, logbuffer.Inspect(&goDump, lb, logbuffer.InspectOptions{MessageDumpLimit: 64}))
			assert.Equal(t, dropDateLine(javaDump), dropDateLine(goDump.String()))
			if t.Failed() {
				t.Logf("java LogInspector:\n%s", javaDump)
			} else {
				t.Logf("%s: %d bytes of LogInspector output identical; java LogMetaDump:\n%s", kind,
					len(javaDump), javaMeta)
			}
		})
	}
}

func dropDateLine(s string) []string {
	lines := strings.Split(strings.TrimRight(s, "\n"), "\n")
	if len(lines) > 1 {
		lines = append(lines[:1:1], lines[2:]...)
	}
	return lines
}

var buildLogInspector sync.Once

func runJava(t *testing.T, args ...string) string {
	files := []string{"compose", "-f", "../../docker/docker-compose.yml", "-f", "../../docker/logbuffer.compose.yml"}
	// Build up front so image build progress never lands on the run's stdout.
	buildLogInspector.Do(func() {
		build := exec.Command("docker", append(append([]string{}, files...), "build", "-q", "log-inspector")...)
		out, err := build.CombinedOutput()
		require.NoErrorf(t, err, "docker compose build log-inspector: %s", out)
	})
	cmdArgs := append(append(append([]string{}, files...), "run", "--rm", "-T", "log-inspector"), args...)
	cmd := exec.Command("docker", cmdArgs...)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	require.NoErrorf(t, err, "docker %v: %s", cmdArgs, stderr.String())
	return string(out)
}

func goMetaDump(m *logbuffer.LogBufferMetaData) string {
	var b strings.Builder
	p := func(k string, v interface{}) { fmt.Fprintf(&b, "%s=%v\n", k, v) }
	p("termLength", m.TermLength())
	p("initialTermId", m.InitialTermID())
	p("mtuLength", m.MtuLength())
	p("pageSize", m.PageSize())
	p("correlationId", m.CorrelationID())
	p("isConnected", m.IsConnected())
	p("activeTransportCount", m.ActiveTransportCount())
	p("endOfStreamPosition", m.EndOfStreamPosition())
	p("activeTermCount", m.ActiveTermCount())
	for i := int32(0); i < logbuffer.PartitionCount; i++ {
		p(fmt.Sprintf("rawTail%d", i), m.RawTailVolatile(i))
	}
	p("sparse", m.Sparse())
	p("tether", m.Tether())
	p("isPublicationRevoked", m.IsPublicationRevoked())
	p("type", m.LogType())
	p("group", m.Group())
	p("isResponse", m.IsResponse())
	p("rejoin", m.Rejoin())
	p("reliable", m.Reliable())
	p("socketRcvbufLength", m.SocketRcvbufLength())
	p("osDefaultSocketRcvbufLength", m.OsDefaultSocketRcvbufLength())
	p("osMaxSocketRcvbufLength", m.OsMaxSocketRcvbufLength())
	p("socketSndbufLength", m.SocketSndbufLength())
	p("osDefaultSocketSndbufLength", m.OsDefaultSocketSndbufLength())
	p("osMaxSocketSndbufLength", m.OsMaxSocketSndbufLength())
	p("receiverWindowLength", m.ReceiverWindowLength())
	p("publicationWindowLength", m.PublicationWindowLength())
	p("untetheredWindowLimitTimeoutNs", m.UntetheredWindowLimitTimeoutNs())
	p("untetheredLingerTimeoutNs", m.UntetheredLingerTimeoutNs())
	p("untetheredRestingTimeoutNs", m.UntetheredRestingTimeoutNs())
	p("maxResend", m.MaxResend())
	p("lingerTimeoutNs", m.LingerTimeoutNs())
	p("entityTag", m.EntityTag())
	p("responseCorrelationId", m.ResponseCorrelationID())
	p("signalEos", m.SignalEos())
	p("spiesSimulateConnection", m.SpiesSimulateConnection())
	return b.String()
}
