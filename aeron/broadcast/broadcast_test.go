package broadcast

import (
	"errors"
	"testing"

	"github.com/lirm/aeron-go/aeron/atomic"
)

func setup(t *testing.T, capacity int32) (*Transmitter, *atomic.Buffer) {
	t.Helper()
	buf := atomic.MakeBuffer(make([]byte, capacity+TrailerLength))
	tx, err := NewTransmitter(buf)
	if err != nil {
		t.Fatal(err)
	}
	return tx, buf
}

func TestTransmitReceive(t *testing.T) {
	tx, buf := setup(t, 1024)
	recv, err := NewReceiver(buf)
	if err != nil {
		t.Fatal(err)
	}
	src := atomic.MakeBuffer(make([]byte, 32))
	for i := int32(0); i < 50; i++ { // wraps several times
		src.PutInt32(0, i)
		tx.Transmit(3, src, 0, 4+i%20)
		if !recv.ReceiveNext() {
			t.Fatal("no message")
		}
		if recv.TypeID() != 3 || recv.Length() != 4+i%20 || buf.GetInt32(recv.Offset()) != i || !recv.Validate() {
			t.Fatalf("bad message %d", i)
		}
	}
	if recv.ReceiveNext() || recv.LappedCount() != 0 {
		t.Fatal("unexpected")
	}
}

func TestReceiverStartsAtLatest(t *testing.T) {
	tx, buf := setup(t, 1024)
	src := atomic.MakeBuffer(make([]byte, 8))
	for i := int32(0); i < 3; i++ {
		src.PutInt32(0, i)
		tx.Transmit(1, src, 0, 4)
	}
	recv, _ := NewReceiver(buf)
	if !recv.ReceiveNext() || buf.GetInt32(recv.Offset()) != 2 || recv.ReceiveNext() {
		t.Fatal("receiver should start at latest record")
	}
}

func TestLapping(t *testing.T) {
	tx, buf := setup(t, 1024)
	recv, _ := NewReceiver(buf)
	src := atomic.MakeBuffer(make([]byte, 64))
	for i := int32(0); i < 100; i++ {
		src.PutInt32(0, i)
		tx.Transmit(1, src, 0, 64)
	}
	if !recv.ReceiveNext() {
		t.Fatal("expected message")
	}
	if recv.LappedCount() != 1 || buf.GetInt32(recv.Offset()) != 99 {
		t.Fatalf("lapped=%d val=%d", recv.LappedCount(), buf.GetInt32(recv.Offset()))
	}
}

func TestCopyReceiver(t *testing.T) {
	tx, buf := setup(t, 1024)
	src := atomic.MakeBuffer(make([]byte, 64))
	tx.Transmit(1, src, 0, 8) // pre-existing, skipped
	recv, _ := NewReceiver(buf)
	cr := NewCopyReceiver(recv)
	if n := cr.Receive(func(int32, *atomic.Buffer, int32, int32) {}); n != 0 {
		t.Fatal("pre-existing message delivered")
	}
	src.PutInt64(0, 77)
	tx.Transmit(9, src, 0, 8)
	var got int64
	if n := cr.Receive(func(typ int32, b *atomic.Buffer, off, l int32) { got = b.GetInt64(off) }); n != 1 || got != 77 {
		t.Fatalf("n=%d got=%d", n, got)
	}
	for i := 0; i < 100; i++ {
		tx.Transmit(1, src, 0, 64)
	}
	if _, err := cr.TryReceive(func(int32, *atomic.Buffer, int32, int32) {}); !errors.Is(err, ErrUnableToKeepUp) {
		t.Fatalf("err=%v", err)
	}
}

func TestCopyReceiverScratchTooSmall(t *testing.T) {
	tx, buf := setup(t, 1024)
	recv, _ := NewReceiver(buf)
	cr := NewCopyReceiverWithScratch(recv, atomic.MakeBuffer(make([]byte, 8)))
	tx.Transmit(1, atomic.MakeBuffer(make([]byte, 64)), 0, 64)
	if _, err := cr.TryReceive(func(int32, *atomic.Buffer, int32, int32) {}); err == nil {
		t.Fatal("expected error")
	}
}
