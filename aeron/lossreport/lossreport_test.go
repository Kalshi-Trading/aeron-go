package lossreport

import (
	"bytes"
	"strings"
	"testing"
	"unsafe"

	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/util"
)

// writeEntry mirrors io.aeron.driver.reports.LossReport.createEntry.
func writeEntry(buf *atomic.Buffer, offset int32, e Entry) int32 {
	buf.PutInt64(offset+TotalBytesLostOffset, e.TotalBytesLost)
	buf.PutInt64(offset+FirstObservationOffset, e.FirstObservationTimestampMs)
	buf.PutInt64(offset+LastObservationOffset, e.LastObservationTimestampMs)
	buf.PutInt32(offset+SessionIDOffset, e.SessionID)
	buf.PutInt32(offset+StreamIDOffset, e.StreamID)
	ch := []byte(e.Channel)
	buf.PutInt32(offset+ChannelOffset, int32(len(ch)))
	buf.PutBytesSlice(offset+ChannelOffset+4, ch)
	srcOff := offset + ChannelOffset + util.AlignInt32(4+int32(len(ch)), 4)
	src := []byte(e.Source)
	buf.PutInt32(srcOff, int32(len(src)))
	buf.PutBytesSlice(srcOff+4, src)
	buf.PutInt64Ordered(offset+ObservationCountOffset, e.ObservationCount)
	return util.AlignInt32(srcOff+4+int32(len(src))-offset, EntryAlignment)
}

func TestRead(t *testing.T) {
	mem := make([]byte, 4096)
	buf := atomic.MakeBuffer(unsafe.Pointer(&mem[0]), int32(len(mem)))
	if Read(buf, func(*Entry) {}) != 0 {
		t.Fatal("empty report")
	}
	e1 := Entry{3, 300, 10, 20, 7, 1001, "aeron:udp?endpoint=localhost:20121", "127.0.0.1:5555"}
	e2 := Entry{1, 32, 30, 30, -5, 9, "aeron:udp?endpoint=x:1", "src"}
	off := writeEntry(buf, 0, e1)
	if off != 128 {
		t.Errorf("entry should be cache-line aligned, got %d", off)
	}
	writeEntry(buf, off, e2)
	got := ReadAll(buf)
	if len(got) != 2 || got[0] != e1 || got[1] != e2 {
		t.Fatalf("got %+v", got)
	}
	var out bytes.Buffer
	Read(buf, CSVEntryConsumer(&out))
	if !strings.HasPrefix(out.String(), "3,300,") || !strings.Contains(out.String(), ",7,1001,aeron:udp?endpoint=localhost:20121,127.0.0.1:5555\n") {
		t.Errorf("csv %q", out.String())
	}
}

func TestOffsets(t *testing.T) {
	if TotalBytesLostOffset != 8 || FirstObservationOffset != 16 || LastObservationOffset != 24 ||
		SessionIDOffset != 32 || StreamIDOffset != 36 || ChannelOffset != 40 || EntryAlignment != 64 {
		t.Error("offsets must match Java LossReport")
	}
}
