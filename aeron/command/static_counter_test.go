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
	m.ClientID.Set(1)
	m.CorrelationID.Set(2)
	m.RegistrationID.Set(3)
	m.CounterTypeID.Set(1001)
	key := atomic.MakeBuffer([]byte{1, 2, 3, 4, 5, 6, 7})
	m.CopyKeyBuffer(key, 0, 7)
	m.CopyLabelString("label")

	assert.EqualValues(t, 1, buf.GetInt64(0))
	assert.EqualValues(t, 2, buf.GetInt64(8))
	assert.EqualValues(t, 3, buf.GetInt64(16))
	assert.EqualValues(t, 1001, buf.GetInt32(24))
	assert.EqualValues(t, 7, buf.GetInt32(28))
	assert.EqualValues(t, 7, buf.GetUInt8(32+6))
	// Java: MINIMUM_LENGTH(32) + align(7, 4) + SIZE_OF_INT + labelLength
	assert.EqualValues(t, 5, buf.GetInt32(32+8))
	assert.Equal(t, "label", string(buf.GetBytesArray(32+8+4, 5)))
	assert.Equal(t, 32+8+4+5, m.Size())
	assert.Equal(t, "label", m.LabelString())
}
