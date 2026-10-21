package counters

import (
	"testing"

	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAppendToLabelEncodesAsciiLikeAgrona(t *testing.T) {
	r := newTestReader()
	allocate(r, 0, 1, nil, "l", 0, 0, 0)
	n, err := AppendToLabel(r.metaData, 0, " é😀x")
	require.NoError(t, err)
	assert.EqualValues(t, 5, n)
	assert.Equal(t, "l ???x", r.GetCounterLabel(0))
}

func TestAppendToLabelTruncatesAtMaxLabelLength(t *testing.T) {
	r := newTestReader()
	allocate(r, 0, 1, nil, "", 0, 0, 0)
	long := make([]byte, MaxLabelLength+10)
	for i := range long {
		long[i] = 'a'
	}
	n, err := AppendToLabel(r.metaData, 0, string(long))
	require.NoError(t, err)
	assert.EqualValues(t, MaxLabelLength, n)
	n, err = AppendToLabel(r.metaData, 0, "more")
	require.NoError(t, err)
	assert.EqualValues(t, 0, n)
	assert.Len(t, r.GetCounterLabel(0), int(MaxLabelLength))
}

func TestAppendToLabelCorruptLengthReturnsError(t *testing.T) {
	r := newTestReader()
	allocate(r, 0, 1, nil, "", 0, 0, 0)
	r.metaData.PutInt32(MetaDataOffset(0)+LabelOffset, MaxLabelLength+100)
	assert.NotPanics(t, func() {
		_, err := AppendToLabel(r.metaData, 0, "x")
		assert.Error(t, err)
	})
	r.metaData.PutInt32(MetaDataOffset(0)+LabelOffset, -5)
	assert.NotPanics(t, func() {
		_, err := AppendToLabel(r.metaData, 0, "x")
		assert.Error(t, err)
	})
}

// A values buffer sized for more counters than the metadata buffer holds must not make the type scans read past
// the end of the metadata buffer.
func TestScansBoundedByMetaDataCapacity(t *testing.T) {
	values := atomic.MakeBuffer(make([]byte, 8*CounterLength))
	meta := atomic.MakeBuffer(make([]byte, 2*MetadataLength))
	r := NewReader(values, meta)
	allocate(r, 0, 5, nil, "a", 1, 0, 0)
	allocate(r, 1, 5, nil, "b", 2, 0, 0)
	assert.NotPanics(t, func() {
		assert.Equal(t, NullCounterId, r.FindCounter(6, func(*atomic.Buffer) bool { return true }))
		n := 0
		r.ScanForType(6, func(int32, *atomic.Buffer) bool { n++; return true })
		assert.Zero(t, n)
	})
}

func TestAtomicCounterOpaqueVariants(t *testing.T) {
	r := newTestReader()
	allocate(r, 1, 1, nil, "x", 0, 0, 0)
	c, err := NewAtomicCounter(r, 1)
	require.NoError(t, err)
	c.SetOpaque(3)
	assert.EqualValues(t, 3, c.GetOpaque())
	assert.EqualValues(t, 3, c.IncrementOpaque())
	assert.EqualValues(t, 4, c.DecrementOpaque())
	assert.EqualValues(t, 3, c.GetAndAddOpaque(7))
	assert.True(t, c.ProposeMaxOpaque(20))
	assert.False(t, c.ProposeMaxOpaque(19))
	assert.EqualValues(t, 20, r.GetCounterValue(1))
}
