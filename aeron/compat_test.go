package aeron

import (
	"testing"

	"github.com/lirm/aeron-go/aeron/command"
	"github.com/lirm/aeron-go/aeron/idlestrategy"
	"github.com/lirm/aeron-go/aeron/logbuffer/term"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Compile-time checks that upstream lirm/aeron-go signatures restored for compatibility do not regress.
var (
	_ func(string) (ChannelUri, error)               = ParseChannelUri
	_ func(ChannelUri) ChannelUri                    = ChannelUri.Clone
	_ func(ChannelUri) string                        = ChannelUri.Scheme
	_ func(ChannelUri) string                        = ChannelUri.Prefix
	_ func(ChannelUri) string                        = ChannelUri.Media
	_ func(ChannelUri) bool                          = ChannelUri.IsIpc
	_ func(ChannelUri) bool                          = ChannelUri.IsUdp
	_ func(ChannelUri, string) string                = ChannelUri.Get
	_ func(ChannelUri, string, string)               = ChannelUri.Set
	_ func(ChannelUri, string)                       = ChannelUri.Remove
	_ func(ChannelUri) string                        = ChannelUri.String
	_ func(*ChannelUri, string)                      = (*ChannelUri).SetPrefix
	_ func(*ChannelUri, string)                      = (*ChannelUri).SetMedia
	_ func(*Aeron, string, int32) chan *Publication  = (*Aeron).AddPublicationDeprecated
	_ func(*Aeron, string, int32) chan *Subscription = (*Aeron).AddSubscriptionDeprecated

	_ func(int64, AvailableCounterHandler) *IdAndAvailableCounterHandler     = NewIdAndAvailablePair
	_ func(int64, UnavailableCounterHandler) *IdAndUnavailableCounterHandler = NewIdAndUnavailablePair
)

// untypedInt only compiles with untyped (or int) constants.
func untypedInt(v int) int { return v }

func TestUpstreamUntypedConstants(t *testing.T) {
	assert.Equal(t, 0x09, untypedInt(command.AddCounter))
	assert.Equal(t, 1, untypedInt(command.ErrorCodeInvalidChannel))
	assert.Equal(t, 2, untypedInt(term.ControlledPollActionBreak))
	assert.Equal(t, 3, untypedInt(term.ControlledPollActionCommit))
	assert.Equal(t, 4, untypedInt(term.ControlledPollActionContinue))
	var _ term.ControlledPollAction = term.ControlledPollActionAbort
	assert.Equal(t, 20, idlestrategy.DefaultMaxYields)
}

func TestUpstreamChannelUriValueUsage(t *testing.T) {
	uri, err := ParseChannelUri("aeron:udp?endpoint=localhost:40123|ttl=4")
	require.NoError(t, err)
	uri.Set("mtu", "1408")
	uri.Remove("ttl")
	c := uri.Clone()
	c.Set("alias", "copy")
	assert.Equal(t, "aeron:udp?endpoint=localhost:40123|mtu=1408", uri.String())
	uri.SetPrefix(SpyQualifier)
	uri.SetMedia(IpcMedia)
	assert.True(t, uri.IsIpc())
	assert.Equal(t, "aeron-spy:aeron:ipc?endpoint=localhost:40123|mtu=1408", uri.String())
}
