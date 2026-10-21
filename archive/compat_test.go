package archive

import (
	"testing"

	"github.com/lirm/aeron-go/aeron"
	"github.com/lirm/aeron-go/archive/status"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Compile-time checks that upstream lirm/aeron-go signatures restored for compatibility do not regress.
var (
	_ func(*Archive, int64) error                               = (*Archive).PurgeRecording
	_ func(*Archive, int64, int64) error                        = (*Archive).TruncateRecording
	_ func(*Archive, aeron.Publication) error                   = (*Archive).StopRecordingByPublication
	_ func(*Proxy, int64, int32, string, []uint8) error         = (*Proxy).AuthConnectRequest
	_ func(*Archive, int64) (int64, error)                      = (*Archive).PurgeRecordingWithCount
	_ func(*Archive, int64, int64) (int64, error)               = (*Archive).TruncateRecordingWithCount
	_ func(*Archive, aeron.Publisher) error                     = (*Archive).StopRecordingByPublisher
	_ func(*Proxy, int64, int32, string, []uint8, string) error = (*Proxy).AuthConnectRequestWithClientInfo
)

func untypedInt(v int) int { return v }

func TestUpstreamUntypedConstants(t *testing.T) {
	assert.Equal(t, int(status.RecordingPositionTypeId), untypedInt(RECORDING_POSITION_COUNTER_TYPE_ID))
	assert.Equal(t, int(status.NullRecordingId), untypedInt(NULL_RECORDING_ID))
	assert.Equal(t, int(status.RecordingIdOffset), untypedInt(RECORDING_ID_OFFSET))
	assert.Equal(t, int(status.SessionIdOffset), untypedInt(SESSION_ID_OFFSET))
	assert.Equal(t, int(status.SourceIdentityLengthOffset), untypedInt(SOURCE_IDENTITY_LENGTH_OFFSET))
	assert.Equal(t, int(status.SourceIdentityOffset), untypedInt(SOURCE_IDENTITY_OFFSET))
	assert.Equal(t, []int{-1, 1, 2, 3, 4, 5, 6}, []int{ControlStateError, ControlStateNew, ControlStateConnectRequestSent,
		ControlStateChallenged, ControlStateConnectRequestOk, ControlStateConnected, ControlStateTimedOut})
}

func TestUpstreamAuthOptionsConcludeToStaticSupplier(t *testing.T) {
	o := DefaultOptions()
	o.AuthEnabled = true
	o.AuthCredentials = []uint8("user:pass")
	o.AuthChallenge = []uint8("challenge")
	o.AuthResponse = []uint8("response")
	require.NoError(t, o.conclude())
	assert.Equal(t, StaticCredentialsSupplier{Credentials: o.AuthCredentials, ChallengeResponse: o.AuthResponse},
		o.CredentialsSupplier)

	o = DefaultOptions()
	require.NoError(t, o.conclude())
	assert.Equal(t, NullCredentialsSupplier{}, o.CredentialsSupplier)
}
