package atomic

import (
	"sync"
	"testing"
)

func expectPanic(t *testing.T, f func()) {
	t.Helper()
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic")
		}
	}()
	f()
}

func TestBoundsCheckOverflow(t *testing.T) {
	b := MakeBuffer(make([]byte, 16))
	expectPanic(t, func() { b.GetInt64(9) })
	expectPanic(t, func() { b.GetInt32(-1) })
	expectPanic(t, func() { b.PutBytesSlice(0x7fffffff, []byte{1}) })
	BoundsCheck(8, 8, 16)
}

func TestPrimitives(t *testing.T) {
	b := MakeBuffer(make([]byte, 64))
	b.PutInt8(0, -3)
	b.PutInt16(2, -300)
	b.PutUInt32(4, 0xdeadbeef)
	b.PutUInt64(8, 1<<63)
	b.PutFloat32(16, 1.5)
	b.PutFloat64(24, -2.25)
	if b.GetInt8(0) != -3 || b.GetInt16(2) != -300 || b.GetUInt32(4) != 0xdeadbeef || b.GetUInt64(8) != 1<<63 ||
		b.GetFloat32(16) != 1.5 || b.GetFloat64(24) != -2.25 {
		t.Fatal("primitive round trip")
	}
}

func TestAtomics(t *testing.T) {
	b := MakeBuffer(make([]byte, 64))
	b.PutInt64Release(0, 5)
	if b.GetInt64Acquire(0) != 5 || b.GetAndAddInt64(0, 3) != 5 || b.GetInt64Volatile(0) != 8 {
		t.Fatal("int64")
	}
	if b.GetAndSetInt64(0, 1) != 8 || b.CompareAndExchangeInt64(0, 1, 2) != 1 || b.GetInt64Opaque(0) != 2 {
		t.Fatal("int64 xchg")
	}
	b.PutInt32Opaque(8, 7)
	if b.GetAndAddInt32(8, 1) != 7 || !b.CompareAndSetInt32(8, 8, 9) || b.CompareAndSetInt32(8, 8, 10) ||
		b.GetAndSetInt32(8, 0) != 9 || b.CompareAndExchangeInt32(8, 5, 6) != 0 {
		t.Fatal("int32")
	}
}

func TestConcurrentGetAndAdd(t *testing.T) {
	b := MakeBuffer(make([]byte, 16))
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 1000; j++ {
				b.GetAndAddInt64(0, 1)
				b.GetAndAddInt32(8, 1)
			}
		}()
	}
	wg.Wait()
	if b.GetInt64Volatile(0) != 8000 || b.GetInt32Volatile(8) != 8000 {
		t.Fatal("lost updates")
	}
}

func TestStrings(t *testing.T) {
	b := MakeBuffer(make([]byte, 128))
	n := b.PutStringAscii(0, "hello")
	if n != 9 || b.GetStringAscii(0) != "hello" || b.GetStringAsciiLength(0, 5) != "hello" {
		t.Fatal("ascii")
	}
	n = b.PutStringUtf8(16, "héllo✓", 64)
	if b.GetStringUtf8(16) != "héllo✓" || n != 4+int32(len("héllo✓")) {
		t.Fatal("utf8")
	}
	n = b.PutStringWithoutLengthUtf8(64, "abc")
	if b.GetStringWithoutLengthUtf8(64, n) != "abc" || b.GetStringWithoutLengthAscii(64, 3) != "abc" {
		t.Fatal("without length")
	}
	if b.PutStringUtf8(100, "", 8) != 4 || b.GetStringUtf8(100) != "" {
		t.Fatal("empty")
	}
}

func TestWrapAndSlices(t *testing.T) {
	data := make([]byte, 32)
	b := MakeBuffer(data, 8, 16)
	if b.Capacity() != 16 {
		t.Fatal("capacity")
	}
	b.PutBytesSlice(0, []byte{1, 2, 3})
	if data[8] != 1 || data[10] != 3 {
		t.Fatal("wrapped offset")
	}
	var sub Buffer
	sub.WrapBuffer(b, 4, 8)
	sub.PutInt32(0, 99)
	if b.GetInt32(4) != 99 {
		t.Fatal("WrapBuffer")
	}
	expectPanic(t, func() { sub.WrapBuffer(b, 12, 8) })
	src := MakeBuffer([]byte{9, 8, 7, 6})
	b.PutBytes(12, src, 0, 4)
	if got := b.GetBytesArray(12, 4); got[0] != 9 || got[3] != 6 {
		t.Fatal("PutBytes")
	}
	b.SetMemory(0, 16, 0)
	if b.GetInt64(0) != 0 || b.GetInt64(8) != 0 {
		t.Fatal("SetMemory")
	}
}

func TestAlignment(t *testing.T) {
	data := make([]byte, 32)
	if err := MakeBuffer(data).VerifyAlignment(); err != nil {
		t.Fatal(err)
	}
	if err := MakeBuffer(data, 1, 16).VerifyAlignment(); err == nil {
		t.Fatal("expected alignment error")
	}
}
