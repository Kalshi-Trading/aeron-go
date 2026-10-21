package cnc

import (
	"errors"
	"testing"
	"unsafe"

	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/util"
)

func TestOffsetsMatchJava(t *testing.T) {
	want := []int32{0, 4, 8, 12, 16, 20, 24, 32, 40, 48}
	got := []int32{VersionFieldOffset, ToDriverBufferLengthFieldOffset, ToClientsBufferLengthFieldOffset,
		CountersMetaDataBufferLengthFieldOffset, CountersValuesBufferLengthFieldOffset,
		ErrorLogBufferLengthFieldOffset, ClientLivenessTimeoutFieldOffset, StartTimestampFieldOffset,
		PidFieldOffset, FilePageSizeFieldOffset}
	for i := range want {
		if want[i] != got[i] {
			t.Errorf("offset %d: %d != %d", i, got[i], want[i])
		}
	}
	if MetaDataLength != 128 || Version != int32(util.SemanticVersionCompose(0, 2, 0)) {
		t.Errorf("MetaDataLength=%d Version=%d", MetaDataLength, Version)
	}
	if ComputeFileLength(1000, 4096) != 4096 {
		t.Error("ComputeFileLength")
	}
}

func makeCnc(version int32, extra int32) (*atomic.Buffer, []byte) {
	lengths := []int32{64, 128, 256, 512, 1024}
	total := MetaDataLength
	for _, l := range lengths {
		total += l
	}
	mem := make([]byte, total+extra)
	buf := atomic.MakeBuffer(unsafe.Pointer(&mem[0]), int32(len(mem)))
	buf.PutInt32(ToDriverBufferLengthFieldOffset, lengths[0])
	buf.PutInt32(ToClientsBufferLengthFieldOffset, lengths[1])
	buf.PutInt32(CountersMetaDataBufferLengthFieldOffset, lengths[2])
	buf.PutInt32(CountersValuesBufferLengthFieldOffset, lengths[3])
	buf.PutInt32(ErrorLogBufferLengthFieldOffset, lengths[4])
	buf.PutInt64(ClientLivenessTimeoutFieldOffset, 10_000_000_000)
	buf.PutInt64(StartTimestampFieldOffset, 1234)
	buf.PutInt64(PidFieldOffset, 77)
	buf.PutInt32(FilePageSizeFieldOffset, 4096)
	buf.PutInt32(VersionFieldOffset, version)
	return buf, mem
}

func TestWrap(t *testing.T) {
	buf, _ := makeCnc(Version, 0)
	f, err := Wrap(buf)
	if err != nil {
		t.Fatal(err)
	}
	if f.ClientLivenessTimeoutNs() != 10_000_000_000 || f.StartTimestampMs() != 1234 || f.Pid() != 77 ||
		f.FilePageSize() != 4096 {
		t.Error("metadata mismatch")
	}
	if f.ToDriverBuffer().Ptr() != unsafe.Add(buf.Ptr(), 128) || f.ToDriverBuffer().Capacity() != 64 {
		t.Error("to-driver region")
	}
	if f.ToClientsBuffer().Ptr() != unsafe.Add(buf.Ptr(), 128+64) {
		t.Error("to-clients region")
	}
	if f.CountersMetaDataBuffer().Ptr() != unsafe.Add(buf.Ptr(), 128+64+128) {
		t.Error("counters metadata region")
	}
	if f.CountersValuesBuffer().Ptr() != unsafe.Add(buf.Ptr(), 128+64+128+256) {
		t.Error("counters values region")
	}
	if f.ErrorLogBuffer().Ptr() != unsafe.Add(buf.Ptr(), 128+64+128+256+512) || f.ErrorLogBuffer().Capacity() != 1024 {
		t.Error("error log region")
	}
	if f.CountersReader() == nil {
		t.Error("reader")
	}
}

func TestWrapRejects(t *testing.T) {
	buf, _ := makeCnc(int32(util.SemanticVersionCompose(1, 0, 0)), 0)
	if _, err := Wrap(buf); !errors.Is(err, ErrIncompatibleVersion) {
		t.Errorf("expected version error, got %v", err)
	}
	buf, _ = makeCnc(0, 0)
	if _, err := Wrap(buf); !errors.Is(err, ErrNotReady) {
		t.Errorf("expected not ready, got %v", err)
	}
	buf, mem := makeCnc(Version, 0)
	short := atomic.MakeBuffer(buf.Ptr(), int32(len(mem)-1))
	if _, err := Wrap(short); err == nil {
		t.Error("expected length error")
	}
	if CheckVersion(int32(util.SemanticVersionCompose(0, 3, 1))) != nil {
		t.Error("minor version differences are compatible")
	}
}
