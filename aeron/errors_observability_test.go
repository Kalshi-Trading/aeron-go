/*
Copyright 2026 Kalshi Trading

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package aeron

import (
	"errors"
	"testing"
	"time"

	"github.com/lirm/aeron-go/aeron/cnc"
	"github.com/lirm/aeron-go/aeron/status"
	"github.com/lirm/aeron-go/aeron/util"
	"github.com/stretchr/testify/require"
)

// Java ClientConductor.onChannelEndpointError only fails async commands; the correlation id is a status indicator
// counter id and must not complete or fail an in-flight synchronous command that happens to share the value.
func TestObsChannelEndpointErrorIgnoresSyncCommand(t *testing.T) {
	tc := newTestConductor(t)
	tc.events.respond = func() {
		tc.OnChannelEndpointError(tc.proxy.nextCorr, "endpoint failed")
		tc.OnNextAvailableSessionID(tc.proxy.nextCorr, 4242)
	}
	id, err := tc.NextSessionID(StreamId1)
	require.NoError(t, err)
	require.Equal(t, int32(4242), id)
}

func TestObsChannelEndpointErrorFailsAsyncCommand(t *testing.T) {
	tc := newTestConductor(t)
	regID, err := tc.AsyncAddDestination(1, "aeron:udp?endpoint=localhost:4000")
	require.NoError(t, err)
	tc.OnChannelEndpointError(regID, "endpoint failed")
	tc.flush()
	require.False(t, tc.IsCommandActive(regID))
	errs := tc.errors()
	require.Len(t, errs, 1)
	var regErr *RegistrationError
	require.True(t, errors.As(errs[0], &regErr))
	require.Equal(t, ErrorCodeChannelEndpointError, regErr.Code)
}

// Java ClientConductor.onPublicationError invokes the handler for every Publication with a matching original
// registration id, and PublicationErrorFrame.set maps the code through ErrorCode.get.
func TestObsPublicationErrorFrameEveryPublication(t *testing.T) {
	tc := newTestConductor(t)
	var frames []PublicationErrorFrame
	tc.publicationErrorFrameHandler = func(f *PublicationErrorFrame) { frames = append(frames, *f) }

	regID1, err := tc.AsyncAddPublication(Channel, StreamId1)
	require.NoError(t, err)
	preloadLogBuffers(t, tc, regID1)
	tc.OnNewPublication(StreamId1, SessionId1, 1, 2, "unused", regID1, regID1)
	regID2, err := tc.AsyncAddPublication(Channel, StreamId1)
	require.NoError(t, err)
	tc.logBuffersByID[regID2] = tc.logBuffersByID[regID1]
	tc.OnNewPublication(StreamId1, SessionId1, 1, 2, "unused", regID2, regID1)

	buf, n := encodePublicationErrorFrame(regID1, 99, "bad")
	tc.OnPublicationError(buf, 0, n)
	tc.flush()
	require.Len(t, frames, 2)
	for _, f := range frames {
		require.Equal(t, ErrorCodeUnknownCodeValue.Value(), f.ErrorCode)
		require.Equal(t, "bad", f.ErrorMessage)
	}
}

// Java ClientConductor.channelStatus returns INITIALIZING for channel status id 0.
func TestObsPublicationChannelStatusZeroID(t *testing.T) {
	tc := newTestConductor(t)
	regID, err := tc.AsyncAddPublication(Channel, StreamId1)
	require.NoError(t, err)
	preloadLogBuffers(t, tc, regID)
	tc.counterValuesBuffer.PutInt64(0, 77)
	tc.OnNewPublication(StreamId1, SessionId1, 1, 0, "unused", regID, regID)
	pub, err := tc.GetPublication(regID)
	require.NoError(t, err)
	require.Equal(t, status.Initializing, pub.ChannelStatus())
}

// Java CommonContext.isDriverActive and requestDriverTermination only check the major CnC version.
func TestObsIsDriverActiveOlderMinorVersion(t *testing.T) {
	dir := t.TempDir()
	writeFakeCnc(t, dir, int32(util.SemanticVersionCompose(0, 1, 0)), time.Now().UnixMilli())
	active, err := IsDriverActive(dir, time.Second, nil)
	require.NoError(t, err)
	require.True(t, active)
	sent, err := RequestDriverTermination(dir, nil)
	require.NoError(t, err)
	require.True(t, sent)
}

// An incompatible CnC version is an AeronException (ERROR) in Java, not a DriverTimeoutException.
func TestObsConnectIncompatibleVersionIsNotTimeout(t *testing.T) {
	dir := t.TempDir()
	writeFakeCnc(t, dir, int32(util.SemanticVersionCompose(1, 0, 0)), time.Now().UnixMilli())
	_, err := Connect(NewContext().AeronDir(dir).MediaDriverTimeout(200 * time.Millisecond))
	require.Error(t, err)
	var dte *DriverTimeoutError
	require.False(t, errors.As(err, &dte), "got %v", err)
	require.ErrorIs(t, err, cnc.ErrIncompatibleVersion)
	require.False(t, IsFatal(err))
	require.Contains(t, err.Error(), "ERROR - CnC version not compatible: app=0.2.0 file=1.0.0")

	writeFakeCnc(t, dir, int32(util.SemanticVersionCompose(0, 1, 0)), time.Now().UnixMilli())
	_, err = Connect(NewContext().AeronDir(dir).MediaDriverTimeout(200 * time.Millisecond))
	require.Error(t, err)
	require.False(t, errors.As(err, &dte), "got %v", err)
	require.Contains(t, err.Error(), "driverVersion=0.1.0 insufficient for clientVersion=0.2.0")

	_, err = Connect(NewContext().AeronDir(t.TempDir()).MediaDriverTimeout(50 * time.Millisecond))
	require.True(t, errors.As(err, &dte), "got %v", err)
	require.ErrorIs(t, err, ErrDriverTimeout)
	require.True(t, IsFatal(err))
}
