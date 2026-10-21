package term

import (
	"testing"

	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/stretchr/testify/assert"
)

func TestComputeFragmentedFrameLength(t *testing.T) {
	assert.Equal(t, int32(0), ComputeFragmentedFrameLength(0, 1376))
	assert.Equal(t, int32(64), ComputeFragmentedFrameLength(1, 1376))
	assert.Equal(t, int32(1408), ComputeFragmentedFrameLength(1376, 1376))
	assert.Equal(t, int32(1408+64), ComputeFragmentedFrameLength(1377, 1376))
	assert.Equal(t, int32(3*1408), ComputeFragmentedFrameLength(3*1376, 1376))
}

func TestDirectBufferVectorValidation(t *testing.T) {
	buf := atomic.MakeBuffer(make([]byte, 100))
	v := NewDirectBufferVector(buf, 10, 90)
	assert.NoError(t, v.Validate())
	length, err := ValidateAndComputeLength([]DirectBufferVector{v, NewDirectBufferVector(buf, 0, 5)})
	assert.NoError(t, err)
	assert.Equal(t, int32(95), length)
	assert.Error(t, v.Reset(buf, 10, 91).Validate())
	assert.Error(t, v.Reset(buf, -1, 1).Validate())
	assert.Error(t, v.Reset(nil, 0, 0).Validate())
	_, err = ValidateAndComputeLength([]DirectBufferVector{NewDirectBufferVector(buf, 0, 101)})
	assert.Error(t, err)
}
