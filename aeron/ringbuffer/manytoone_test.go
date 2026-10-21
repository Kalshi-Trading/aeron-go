package rb

import (
	"sync"
	"testing"

	"github.com/lirm/aeron-go/aeron/atomic"
)

func newRing(t *testing.T, capacity int32) *ManyToOne {
	t.Helper()
	r, err := NewManyToOne(atomic.MakeBuffer(make([]byte, capacity+TrailerLength)))
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func TestInvalidCapacity(t *testing.T) {
	if _, err := NewManyToOne(atomic.MakeBuffer(make([]byte, 1000+TrailerLength))); err == nil {
		t.Fatal("expected error for non power of two capacity")
	}
}

func TestWriteRead(t *testing.T) {
	r := newRing(t, 1024)
	src := atomic.MakeBuffer(make([]byte, 64))
	for i := int32(0); i < 10; i++ {
		src.PutInt32(0, i)
		if !r.Write(7, src, 0, 4+i) {
			t.Fatal("write failed")
		}
	}
	if r.Size() == 0 {
		t.Fatal("expected size > 0")
	}
	var got []int32
	n := r.Read(func(typ int32, b *atomic.Buffer, off, length int32) {
		if typ != 7 || length != 4+int32(len(got)) {
			t.Fatalf("typ=%d len=%d", typ, length)
		}
		got = append(got, b.GetInt32(off))
	}, 0)
	if n != 10 || got[9] != 9 {
		t.Fatalf("n=%d got=%v", n, got)
	}
	if r.Size() != 0 || r.ConsumerPosition() != r.ProducerPosition() {
		t.Fatal("not fully consumed")
	}
	// consumed region zeroed
	if r.Buffer().GetInt64(0) != 0 {
		t.Fatal("consumed space not zeroed")
	}
}

func TestReadLimit(t *testing.T) {
	r := newRing(t, 1024)
	src := atomic.MakeBuffer(make([]byte, 8))
	for i := 0; i < 5; i++ {
		r.Write(1, src, 0, 8)
	}
	if n := r.Read(func(int32, *atomic.Buffer, int32, int32) {}, 2); n != 2 {
		t.Fatalf("n=%d", n)
	}
	if n := r.Read(func(int32, *atomic.Buffer, int32, int32) {}, 0); n != 3 {
		t.Fatalf("n=%d", n)
	}
}

func TestReadAdvancesHeadOnPanic(t *testing.T) {
	r := newRing(t, 1024)
	src := atomic.MakeBuffer(make([]byte, 8))
	r.Write(1, src, 0, 8)
	r.Write(1, src, 0, 8)
	func() {
		defer func() { recover() }()
		r.Read(func(int32, *atomic.Buffer, int32, int32) { panic("boom") }, 0)
	}()
	if r.ConsumerPosition() != 16 {
		t.Fatalf("head=%d", r.ConsumerPosition())
	}
}

func TestPaddingWrap(t *testing.T) {
	r := newRing(t, 128)
	src := atomic.MakeBuffer(make([]byte, 16))
	// fill 96 bytes then consume, so next 40-byte record must wrap with 32 bytes padding
	for i := 0; i < 4; i++ {
		if !r.Write(1, src, 0, 16) {
			t.Fatal("write")
		}
	}
	r.Read(func(int32, *atomic.Buffer, int32, int32) {}, 0)
	if !r.Write(2, src, 0, 16) {
		t.Fatal("write")
	}
	if !r.Write(3, src, 0, 16) { // 24 bytes left at end < 24? exactly fits
		t.Fatal("write")
	}
	if !r.Write(4, src, 0, 16) {
		t.Fatal("wrap write")
	}
	var types []int32
	for r.Size() > 0 {
		r.Read(func(typ int32, _ *atomic.Buffer, _, _ int32) { types = append(types, typ) }, 0)
	}
	if len(types) != 3 || types[2] != 4 {
		t.Fatalf("types=%v", types)
	}
}

func TestInsufficientCapacity(t *testing.T) {
	r := newRing(t, 64)
	src := atomic.MakeBuffer(make([]byte, 8))
	n := 0
	for r.Write(1, src, 0, 8) {
		n++
	}
	if n != 4 {
		t.Fatalf("n=%d", n)
	}
	if r.TryClaim(1, 8) != InsufficientCapacity {
		t.Fatal("expected insufficient capacity")
	}
}

func TestTryClaimCommitAbort(t *testing.T) {
	r := newRing(t, 1024)
	i1 := r.TryClaim(5, 8)
	i2 := r.TryClaim(6, 8)
	r.Buffer().PutInt64(i1, 42)
	if n := r.Read(func(int32, *atomic.Buffer, int32, int32) {}, 0); n != 0 {
		t.Fatal("uncommitted claim visible")
	}
	r.Commit(i1)
	r.Abort(i2)
	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("expected panic on double commit")
			}
		}()
		r.Commit(i1)
	}()
	var vals []int64
	n := r.Read(func(typ int32, b *atomic.Buffer, off, l int32) { vals = append(vals, b.GetInt64(off)) }, 0)
	if n != 1 || vals[0] != 42 || r.Size() != 0 {
		t.Fatalf("n=%d vals=%v", n, vals)
	}
}

func TestControlledRead(t *testing.T) {
	r := newRing(t, 1024)
	src := atomic.MakeBuffer(make([]byte, 8))
	for i := 0; i < 4; i++ {
		r.Write(int32(i+1), src, 0, 8)
	}
	n := r.ControlledRead(func(typ int32, _ *atomic.Buffer, _, _ int32) ControlledAction {
		if typ == 2 {
			return Abort
		}
		return Continue
	}, 0)
	if n != 1 || r.ConsumerPosition() != 16 {
		t.Fatalf("n=%d head=%d", n, r.ConsumerPosition())
	}
	n = r.ControlledRead(func(typ int32, _ *atomic.Buffer, _, _ int32) ControlledAction {
		if typ == 2 {
			return Commit
		}
		return Break
	}, 0)
	if n != 2 || r.ConsumerPosition() != 48 {
		t.Fatalf("n=%d head=%d", n, r.ConsumerPosition())
	}
}

func TestUnblockNegativeLength(t *testing.T) {
	r := newRing(t, 1024)
	idx := r.TryClaim(1, 8) // producer "dies"
	_ = idx
	src := atomic.MakeBuffer(make([]byte, 8))
	r.Write(2, src, 0, 8)
	if !r.Unblock() {
		t.Fatal("expected unblock")
	}
	var types []int32
	r.Read(func(typ int32, _ *atomic.Buffer, _, _ int32) { types = append(types, typ) }, 0)
	if len(types) != 1 || types[0] != 2 {
		t.Fatalf("types=%v", types)
	}
}

func TestUnblockZeroLength(t *testing.T) {
	r := newRing(t, 1024)
	// simulate claimed-but-header-not-yet-written by advancing tail by 16, then a committed record
	r.Buffer().PutInt64(r.tailPositionIndex, 16)
	src := atomic.MakeBuffer(make([]byte, 8))
	r.Write(2, src, 0, 8)
	if !r.Unblock() {
		t.Fatal("expected unblock")
	}
	var types []int32
	r.Read(func(typ int32, _ *atomic.Buffer, _, _ int32) { types = append(types, typ) }, 0)
	if len(types) != 1 || types[0] != 2 {
		t.Fatalf("types=%v", types)
	}
	if r.Unblock() {
		t.Fatal("nothing to unblock")
	}
}

func TestCorrelationAndHeartbeat(t *testing.T) {
	r := newRing(t, 1024)
	if r.NextCorrelationID() != 0 || r.NextCorrelationID() != 1 {
		t.Fatal("correlation")
	}
	r.SetConsumerHeartbeatTime(123)
	if r.ConsumerHeartbeatTime() != 123 {
		t.Fatal("heartbeat")
	}
}

func TestConcurrentProducers(t *testing.T) {
	r := newRing(t, 64*1024)
	const producers, perProducer = 4, 20000
	var wg sync.WaitGroup
	for p := 0; p < producers; p++ {
		wg.Add(1)
		go func(p int32) {
			defer wg.Done()
			src := atomic.MakeBuffer(make([]byte, 8))
			for i := int32(0); i < perProducer; i++ {
				src.PutInt32(0, p)
				src.PutInt32(4, i)
				for !r.Write(1, src, 0, 8) {
				}
			}
		}(int32(p))
	}
	next := make([]int32, producers)
	total := 0
	for total < producers*perProducer {
		total += r.Read(func(_ int32, b *atomic.Buffer, off, _ int32) {
			p, i := b.GetInt32(off), b.GetInt32(off+4)
			if next[p] != i {
				t.Errorf("producer %d: got %d want %d", p, i, next[p])
			}
			next[p]++
		}, 0)
	}
	wg.Wait()
}
