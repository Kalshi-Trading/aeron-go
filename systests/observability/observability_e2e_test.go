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

// Package observability contains end-to-end tests of the errors & observability APIs against a real Java media
// driver. Run with:
//
//	docker compose -f docker/docker-compose.yml up -d --build archiving-driver
//	AERON_E2E=1 AERON_DIR=/dev/shm/aeron-compose go test ./systests/observability -v
package observability

import (
	"encoding/binary"
	"errors"
	"net"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lirm/aeron-go/aeron"
	"github.com/lirm/aeron-go/aeron/cnc"
	"github.com/lirm/aeron-go/aeron/counters"
	"github.com/lirm/aeron-go/aeron/errorlog"
	"github.com/lirm/aeron-go/aeron/lossreport"
	"github.com/lirm/aeron-go/aeron/status"
)

const (
	hdrTypeData  = 0x01
	hdrTypeErr   = 0x04
	hdrTypeSM    = 0x03
	hdrTypeSetup = 0x05
	termLength   = 64 * 1024
)

func aeronDir(t *testing.T) string {
	if os.Getenv("AERON_E2E") != "1" {
		t.Skip("set AERON_E2E=1 to run against a live Java media driver")
	}
	if d := os.Getenv("AERON_DIR"); d != "" {
		return d
	}
	return "/dev/shm/aeron-compose"
}

func waitFor(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

type recorder struct {
	mu     sync.Mutex
	frames []aeron.PublicationErrorFrame
	errs   []error
}

func (r *recorder) frame(f *aeron.PublicationErrorFrame) {
	r.mu.Lock()
	r.frames = append(r.frames, *f)
	r.mu.Unlock()
}

func (r *recorder) err(e error) {
	r.mu.Lock()
	r.errs = append(r.errs, e)
	r.mu.Unlock()
}

func (r *recorder) snapshot() []aeron.PublicationErrorFrame {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]aeron.PublicationErrorFrame(nil), r.frames...)
}

func connect(t *testing.T, dir string, rec *recorder) *aeron.Aeron {
	ctx := aeron.NewContext().AeronDir(dir).MediaDriverTimeout(10 * time.Second).
		ErrorHandler(rec.err).PublicationErrorFrameHandler(rec.frame)
	a, err := aeron.Connect(ctx)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { _ = a.Close() })
	return a
}

func TestCncAndCounters(t *testing.T) {
	dir := aeronDir(t)
	f, err := cnc.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	t.Logf("cnc version=%#x pid=%d start=%s liveness=%s pageSize=%d errorLog=%d",
		f.Version(), f.Pid(), time.UnixMilli(f.StartTimestampMs()), time.Duration(f.ClientLivenessTimeoutNs()),
		f.FilePageSize(), f.ErrorLogBufferLength())
	if f.Pid() <= 0 || f.StartTimestampMs() <= 0 || f.ClientLivenessTimeoutNs() <= 0 || f.FilePageSize() <= 0 ||
		f.ErrorLogBufferLength() <= 0 {
		t.Error("unexpected CnC metadata")
	}
	if f.StartTimestampMs() > time.Now().UnixMilli() {
		t.Error("start timestamp in the future")
	}
	var labels []string
	f.CountersReader().Scan(func(c counters.Counter) {
		labels = append(labels, c.Label)
	})
	t.Logf("%d counters", len(labels))
	for _, want := range []string{"Bytes sent", "Errors: version=1.53.3"} {
		found := false
		for _, l := range labels {
			found = found || strings.HasPrefix(l, want)
		}
		if !found {
			t.Errorf("missing system counter %q", want)
		}
	}
}

func TestInvalidChannelErrorAndErrorLog(t *testing.T) {
	dir := aeronDir(t)
	rec := &recorder{}
	a := connect(t, dir, rec)

	f, err := cnc.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	marker := "e2e" + time.Now().Format("150405.000000")
	channel := "aeron:udp?endpoint=localhost:notaport&" + marker + "=1"
	_, err = a.AddPublication(channel, 1)
	var reg *aeron.RegistrationError
	if !errors.As(err, &reg) {
		t.Fatalf("expected RegistrationError, got %T %v", err, err)
	}
	t.Logf("registration error: %v (code=%s)", reg, reg.ErrorCode())
	if reg.ErrorCode() != aeron.ErrorCodeInvalidChannel || reg.Category != aeron.CategoryError {
		t.Errorf("unexpected code/category %s/%s", reg.ErrorCode(), reg.Category)
	}

	var found errorlog.Observation
	waitFor(t, 5*time.Second, "error in distinct error log", func() bool {
		for _, o := range errorlog.ReadAll(f.ErrorLogBuffer()) {
			if strings.Contains(o.EncodedError, marker) {
				found = o
				return true
			}
		}
		return false
	})
	t.Logf("error log: %d observations, first line: %s", found.ObservationCount,
		strings.SplitN(found.EncodedError, "\n", 2)[0])
	if found.ObservationCount < 1 || found.FirstObservationTimestampMs > found.LastObservationTimestampMs {
		t.Error("bad observation")
	}
}

func TestLocalSocketAddressesAndLossReport(t *testing.T) {
	dir := aeronDir(t)
	rec := &recorder{}
	a := connect(t, dir, rec)

	const streamID = 4242
	sub, err := a.AddSubscription("aeron:udp?endpoint=127.0.0.1:0", streamID)
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()

	var addrs []string
	waitFor(t, 5*time.Second, "local socket address", func() bool {
		addrs = sub.LocalSocketAddresses()
		return len(addrs) > 0
	})
	direct := status.FindAddresses(a.CounterReader(), int64(sub.ChannelStatus()), sub.ChannelStatusID())
	t.Logf("subscription bound to %v (status=%s, via status pkg %v), resolved channel %s", addrs,
		status.String(int64(sub.ChannelStatus())), direct, sub.TryResolveChannelEndpointPort())
	if len(direct) != 1 || direct[0] != addrs[0] || strings.HasSuffix(addrs[0], ":0") {
		t.Fatalf("unexpected addresses %v / %v", addrs, direct)
	}

	// Act as a raw Aeron sender: SETUP, then two DATA frames with a gap so the driver records loss.
	target, err := net.ResolveUDPAddr("udp", addrs[0])
	if err != nil {
		t.Fatal(err)
	}
	conn, err := net.DialUDP("udp", nil, target)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	sessionID := int32(time.Now().UnixNano() & 0x7fffffff)

	waitFor(t, 5*time.Second, "image from raw setup", func() bool {
		_, _ = conn.Write(setupFrame(sessionID, streamID))
		return sub.ImageCount() > 0
	})
	_, _ = conn.Write(dataFrame(sessionID, streamID, 0))
	_, _ = conn.Write(dataFrame(sessionID, streamID, 4096))

	report, err := lossreport.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer report.Close()
	var entry lossreport.Entry
	waitFor(t, 10*time.Second, "loss report entry", func() bool {
		for _, e := range report.Entries() {
			if e.SessionID == sessionID && e.StreamID == streamID {
				entry = e
				return true
			}
		}
		_, _ = conn.Write(dataFrame(sessionID, streamID, 4096))
		return false
	})
	t.Logf("loss entry: %+v", entry)
	if entry.TotalBytesLost <= 0 || entry.ObservationCount < 1 || !strings.Contains(entry.Channel, "127.0.0.1:0") ||
		entry.Source == "" {
		t.Errorf("unexpected entry %+v", entry)
	}
}

func TestPublicationErrorFrame(t *testing.T) {
	dir := aeronDir(t)
	rec := &recorder{}
	a := connect(t, dir, rec)

	// Raw UDP "receiver" that the driver's publication sends to.
	recv, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer recv.Close()
	const streamID = 4343
	pub, err := a.AddPublication("aeron:udp?endpoint="+recv.LocalAddr().String(), streamID)
	if err != nil {
		t.Fatal(err)
	}
	defer pub.Close()

	buf := make([]byte, 2048)
	_ = recv.SetReadDeadline(time.Now().Add(5 * time.Second))
	n, src, err := recv.ReadFromUDP(buf)
	if err != nil || n < 24 {
		t.Fatalf("no frame from driver: %v", err)
	}
	sessionID := int32(binary.LittleEndian.Uint32(buf[12:]))
	termID := int32(binary.LittleEndian.Uint32(buf[24:])) // active term id (SETUP) or term id (DATA)
	const receiverID = int64(0x5eed)
	if _, err := recv.WriteToUDP(statusMessage(sessionID, streamID, termID, receiverID), src); err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)

	var got aeron.PublicationErrorFrame
	waitFor(t, 5*time.Second, "publication error frame", func() bool {
		for _, f := range rec.snapshot() {
			if f.RegistrationID == pub.OriginalRegistrationID() {
				got = f
				return true
			}
		}
		_, _ = recv.WriteToUDP(statusMessage(sessionID, streamID, termID, receiverID), src)
		_, _ = recv.WriteToUDP(errorFrame(sessionID, streamID, receiverID, int32(aeron.ErrorCodeImageRejected),
			"e2e rejected"), src)
		return false
	})
	t.Logf("publication error frame: %v", got)
	if got.SessionID != sessionID || got.StreamID != streamID || got.ReceiverID != receiverID ||
		got.ErrorCode != int32(aeron.ErrorCodeImageRejected) || got.ErrorMessage != "e2e rejected" ||
		got.SourceAddress == nil || got.SourceAddress.Port != recv.LocalAddr().(*net.UDPAddr).Port {
		t.Errorf("unexpected frame %v", got)
	}
}

func header(b []byte, frameLength int, flags byte, typ uint16) {
	binary.LittleEndian.PutUint32(b[0:], uint32(frameLength))
	b[4] = 0
	b[5] = flags
	binary.LittleEndian.PutUint16(b[6:], typ)
}

func setupFrame(sessionID, streamID int32) []byte {
	b := make([]byte, 40)
	header(b, 40, 0, hdrTypeSetup)
	binary.LittleEndian.PutUint32(b[12:], uint32(sessionID))
	binary.LittleEndian.PutUint32(b[16:], uint32(streamID))
	binary.LittleEndian.PutUint32(b[28:], termLength)
	binary.LittleEndian.PutUint32(b[32:], 1408)
	return b
}

func dataFrame(sessionID, streamID int32, termOffset int32) []byte {
	b := make([]byte, 64)
	header(b, 64, 0xC0, hdrTypeData)
	binary.LittleEndian.PutUint32(b[8:], uint32(termOffset))
	binary.LittleEndian.PutUint32(b[12:], uint32(sessionID))
	binary.LittleEndian.PutUint32(b[16:], uint32(streamID))
	return b
}

func statusMessage(sessionID, streamID, termID int32, receiverID int64) []byte {
	b := make([]byte, 36)
	header(b, 36, 0, hdrTypeSM)
	binary.LittleEndian.PutUint32(b[8:], uint32(sessionID))
	binary.LittleEndian.PutUint32(b[12:], uint32(streamID))
	binary.LittleEndian.PutUint32(b[16:], uint32(termID))
	binary.LittleEndian.PutUint32(b[24:], termLength)
	binary.LittleEndian.PutUint64(b[28:], uint64(receiverID))
	return b
}

func errorFrame(sessionID, streamID int32, receiverID int64, code int32, msg string) []byte {
	b := make([]byte, 40+len(msg))
	header(b, len(b), 0, hdrTypeErr)
	binary.LittleEndian.PutUint32(b[8:], uint32(sessionID))
	binary.LittleEndian.PutUint32(b[12:], uint32(streamID))
	binary.LittleEndian.PutUint64(b[16:], uint64(receiverID))
	binary.LittleEndian.PutUint32(b[32:], uint32(code))
	binary.LittleEndian.PutUint32(b[36:], uint32(len(msg)))
	copy(b[40:], msg)
	return b
}
