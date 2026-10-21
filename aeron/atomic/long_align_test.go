package atomic

import "testing"

type misalignedLong struct {
	_ int32
	l Long
}

// Long must be usable at a 4-byte-aligned offset on 386/arm (run with GOARCH=386).
func TestLongAlignmentIn32BitStruct(t *testing.T) {
	var s misalignedLong
	s.l.Set(1)
	if s.l.Inc() != 2 {
		t.Fatal("inc")
	}
}
