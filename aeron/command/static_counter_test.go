package command

import (
	"testing"

	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/stretchr/testify/assert"
)

func TestStaticCounterMessageLayoutMatchesJava(t *testing.T) {
	buf := atomic.MakeBuffer(make([]byte, 256))
	var m StaticCounterMessage
	m.Wrap(buf, 0)
	m.SetClientID(1)
	m.SetCorrelationID(2)
	m.SetRegistrationID(3)
	m.SetTypeID(1001)
	key := atomic.MakeBuffer([]byte{1, 2, 3, 4, 5, 6, 7})
	m.SetKeyBuffer(key, 0, 7)
	m.SetLabel("label")

	assert.EqualValues(t, 1, buf.GetInt64(0))
	assert.EqualValues(t, 2, buf.GetInt64(8))
	assert.EqualValues(t, 3, buf.GetInt64(16))
	assert.EqualValues(t, 1001, buf.GetInt32(24))
	assert.EqualValues(t, 7, buf.GetInt32(28))
	assert.EqualValues(t, 7, buf.GetUInt8(32+6))
	// Java: MINIMUM_LENGTH(36) + align(7, 4) + SIZE_OF_INT + labelLength is the claimed length; encoded is 4 less.
	assert.EqualValues(t, 5, buf.GetInt32(32+8))
	assert.Equal(t, "label", string(buf.GetBytesArray(32+8+4, 5)))
	assert.EqualValues(t, 32+8+4+5, m.Length())
	assert.EqualValues(t, 36+8+4+5, ComputeStaticCounterMessageLength(7, 5))
	assert.Equal(t, "label", m.Label())
}
