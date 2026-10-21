package command

import (
	"testing"

	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/stretchr/testify/assert"
)

// Layout must match io.aeron.command.RemovePublicationFlyweight: clientId, correlationId, registrationId, flags.
func TestRemovePublicationMessageLayout(t *testing.T) {
	buf := atomic.MakeBuffer(make([]byte, 64))
	var m RemovePublicationMessage
	m.Wrap(buf, 0)
	m.ClientID.Set(1)
	m.CorrelationID.Set(2)
	m.RegistrationID.Set(3)
	m.Flags.Set(RemovePublicationFlagRevoke)
	assert.Equal(t, 32, m.Size())
	assert.Equal(t, int64(3), buf.GetInt64(16))
	assert.Equal(t, int64(1), buf.GetInt64(24))
}
