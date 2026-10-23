package e2e

import (
	"testing"

	"github.com/lirm/aeron-go/aeron/logbuffer/term"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func msg(seq int64) []byte {
	b := make([]byte, 40)
	EncodeMessage(b, seq)
	return b
}

func TestEncodeDecodeRoundTrip(t *testing.T) {
	for _, seq := range []int64{0, 1, 255, 256, 1 << 40} {
		got, err := DecodeMessage(msg(seq))
		require.NoError(t, err)
		assert.Equal(t, seq, got)
	}
}

func TestDecodeDetectsCorruption(t *testing.T) {
	b := msg(7)
	b[20] ^= 0xff
	_, err := DecodeMessage(b)
	assert.ErrorContains(t, err, "corrupt payload")

	_, err = DecodeMessage(msg(7)[:30])
	assert.ErrorContains(t, err, "encoded length")

	_, err = DecodeMessage([]byte{1, 2})
	assert.ErrorContains(t, err, "too short")
}

func TestSequenceCheckerInOrder(t *testing.T) {
	c := NewSequenceChecker(5)
	for seq := int64(5); seq < 10; seq++ {
		c.OnMessage(msg(seq))
	}
	assert.NoError(t, c.Err())
	assert.Equal(t, int64(10), c.Next)
	assert.Equal(t, int64(5), c.Received)
}

func TestSequenceCheckerGapAndDuplicate(t *testing.T) {
	c := NewSequenceChecker(0)
	c.OnMessage(msg(0))
	c.OnMessage(msg(3))
	c.OnMessage(msg(3))
	c.OnMessage(msg(4))
	assert.Equal(t, int64(1), c.Gaps)
	assert.Equal(t, int64(2), c.Missing)
	assert.Equal(t, int64(1), c.Duplicates)
	assert.Equal(t, int64(5), c.Next)
	err := c.Err()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "gap: expected seq 1, got 3")
}

func TestSequenceCheckerCorrupt(t *testing.T) {
	c := NewSequenceChecker(0)
	b := msg(0)
	b[HeaderLength] ^= 1
	c.OnMessage(b)
	assert.Equal(t, int64(1), c.Corrupt)
	assert.Error(t, c.Err())
}

func TestConsumeStopsOnGap(t *testing.T) {
	msgs := [][]byte{msg(0), msg(2)}
	c := NewSequenceChecker(0)
	i := 0
	err := Consume(func(h term.FragmentHandler, limit int) (int, error) {
		if i >= len(msgs) {
			return 0, nil
		}
		c.OnMessage(msgs[i])
		i++
		return 1, nil
	}, c, 10, 0)
	assert.ErrorContains(t, err, "gap")
}

func TestConsumeTimesOut(t *testing.T) {
	c := NewSequenceChecker(0)
	err := Consume(func(term.FragmentHandler, int) (int, error) { return 0, nil }, c, 1, 0)
	assert.ErrorIs(t, err, ErrTimeout)
}
