package errorlog

import (
	"bytes"
	"errors"
	"strings"
	"testing"
	"unsafe"

	"github.com/lirm/aeron-go/aeron/atomic"
)

func newBuffer(n int) *atomic.Buffer {
	mem := make([]byte, n)
	return atomic.MakeBuffer(unsafe.Pointer(&mem[0]), int32(n))
}

func TestRecordAndRead(t *testing.T) {
	buf := newBuffer(1024)
	now := int64(1000)
	log := NewDistinctErrorLog(buf, func() int64 { return now })
	if HasErrors(buf) {
		t.Fatal("empty log has errors")
	}
	if !log.Record(errors.New("boom")) {
		t.Fatal("record failed")
	}
	now = 2000
	log.Record(errors.New("boom"))
	log.Record(errors.New("other"))

	obs := ReadAll(buf)
	if len(obs) != 2 {
		t.Fatalf("expected 2 distinct errors, got %d", len(obs))
	}
	if obs[0].EncodedError != "boom" || obs[0].ObservationCount != 2 || obs[0].FirstObservationTimestampMs != 1000 ||
		obs[0].LastObservationTimestampMs != 2000 {
		t.Errorf("unexpected first %+v", obs[0])
	}
	if obs[1].EncodedError != "other" || obs[1].ObservationCount != 1 {
		t.Errorf("unexpected second %+v", obs[1])
	}
	if n := ReadSince(buf, func(int32, int64, int64, string) {}, 1500); n != 2 {
		t.Errorf("ReadSince=%d", n)
	}
	if n := ReadSince(buf, func(int32, int64, int64, string) {}, 2500); n != 0 {
		t.Errorf("ReadSince=%d", n)
	}

	var out bytes.Buffer
	if PrintErrorLog(buf, &out) != 2 || !strings.Contains(out.String(), "2 observations from") ||
		!strings.Contains(out.String(), "2 distinct errors observed.") {
		t.Errorf("print output %q", out.String())
	}

	// Reopening picks up existing records.
	log2 := NewDistinctErrorLog(buf, func() int64 { return 3000 })
	log2.Record(errors.New("other"))
	log2.Record(errors.New("third"))
	obs = ReadAll(buf)
	if len(obs) != 3 || obs[1].ObservationCount != 2 || obs[2].EncodedError != "third" {
		t.Errorf("reopen failed %+v", obs)
	}
}

func TestRecordFull(t *testing.T) {
	buf := newBuffer(64)
	log := NewDistinctErrorLog(buf, nil)
	if !log.Record(errors.New("short")) {
		t.Fatal("first record should fit")
	}
	if log.Record(errors.New(strings.Repeat("x", 64))) {
		t.Error("record should not fit")
	}
}

// Layout check against agrona's DistinctErrorLog record layout.
func TestRecordLayout(t *testing.T) {
	buf := newBuffer(256)
	log := NewDistinctErrorLog(buf, func() int64 { return 5 })
	log.Record(errors.New("abc"))
	if buf.GetInt32(0) != 24+3 || buf.GetInt32(4) != 1 || buf.GetInt64(8) != 5 || buf.GetInt64(16) != 5 ||
		string(buf.GetBytesArray(24, 3)) != "abc" {
		t.Error("layout mismatch")
	}
	log.Record(errors.New("d"))
	if buf.GetInt32(32) != 25 {
		t.Error("second record should be 8-byte aligned at 32")
	}
}
