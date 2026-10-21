package status

import (
	"reflect"
	"testing"
	"unsafe"

	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/counters"
)

type fakeCounters struct {
	values, meta *atomic.Buffer
	mem          [][]byte
}

func newFakeCounters(n int32) *fakeCounters {
	v := make([]byte, n*counters.CounterLength)
	m := make([]byte, n*counters.MetadataLength)
	return &fakeCounters{
		values: atomic.MakeBuffer(unsafe.Pointer(&v[0]), int32(len(v))),
		meta:   atomic.MakeBuffer(unsafe.Pointer(&m[0]), int32(len(m))),
		mem:    [][]byte{v, m},
	}
}

func (f *fakeCounters) addLocalAddress(id, channelStatusID int32, value int64, addr string) {
	off := id * counters.MetadataLength
	f.meta.PutInt32(off+counters.TypeIdOffset, LocalSocketAddressStatusTypeID)
	key := off + counters.KeyOffset
	f.meta.PutInt32(key+ChannelStatusIDOffset, channelStatusID)
	b := []byte(addr)
	f.meta.PutInt32(key+LocalSocketAddressLengthOffset, int32(len(b)))
	f.meta.PutBytesArray(key+LocalSocketAddressStringOffset, &b, 0, int32(len(b)))
	f.values.PutInt64(id*counters.CounterLength, value)
	f.meta.PutInt32(off, counters.RecordAllocated)
}

func TestFindAddresses(t *testing.T) {
	f := newFakeCounters(8)
	f.meta.PutInt32(0, counters.RecordAllocated)
	f.meta.PutInt32(counters.TypeIdOffset, ReceiveChannelStatusTypeID)
	f.addLocalAddress(1, 0, Active, "127.0.0.1:40123")
	f.addLocalAddress(2, 5, Active, "127.0.0.1:1")
	f.addLocalAddress(3, 0, Active, "[::1]:40123")
	f.addLocalAddress(4, 0, Closing, "127.0.0.1:9")
	reader := counters.NewReader(f.values, f.meta)

	got := FindAddresses(reader, Active, 0)
	if !reflect.DeepEqual(got, []string{"127.0.0.1:40123", "[::1]:40123"}) {
		t.Errorf("FindAddresses = %v", got)
	}
	if FindAddress(reader, Active, 0) != "127.0.0.1:40123" {
		t.Error("FindAddress")
	}
	if FindAddresses(reader, Initializing, 0) != nil || FindAddress(reader, Errored, 0) != "" {
		t.Error("non-active channels have no addresses")
	}
}

func TestStatusStrings(t *testing.T) {
	for v, s := range map[int64]string{0: "INITIALIZING", -1: "ERRORED", 1: "ACTIVE", 2: "CLOSING", 42: "unknown id=42",
		-9: "unknown id=-9"} {
		if String(v) != s {
			t.Errorf("String(%d)=%q", v, String(v))
		}
	}
}
