package conductorfaults

import (
	"errors"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/lirm/aeron-go/aeron"
	"github.com/stretchr/testify/require"
)

// errorCase is a command the Java 1.53.3 driver rejects with an ON_ERROR response.
type errorCase struct {
	name string
	// run returns the error of the rejected command and its correlation id (0 if unknown to the caller).
	run      func(t *testing.T, c *client) error
	code     aeron.ErrorCode
	contains string
}

func driverErrorCases() []errorCase {
	return []errorCase{
		{name: "InvalidChannelNoEndpoint", code: aeron.ErrorCodeInvalidChannel, contains: "endpoint",
			run: func(t *testing.T, c *client) error {
				_, err := c.AddSubscription("aeron:udp?interface=localhost", nextStream())
				return err
			}},
		{name: "InvalidChannelBadMtu", code: aeron.ErrorCodeGenericError, contains: "mtu",
			run: func(t *testing.T, c *client) error {
				_, err := c.AddPublication(fmt.Sprintf("aeron:udp?endpoint=localhost:%d|mtu=33", nextPort()), nextStream())
				return err
			}},
		{name: "AddressInUse", code: aeron.ErrorCodeGenericError, contains: "in use",
			run: func(t *testing.T, c *client) error {
				port := nextPort()
				conn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: port})
				require.NoError(t, err)
				defer conn.Close()
				_, err = c.AddSubscription(udp(port), nextStream())
				return err
			}},
		{name: "DuplicateEntityTag", code: aeron.ErrorCodeGenericError, contains: "tag",
			run: func(t *testing.T, c *client) error {
				tag := 9800000 + nextPort()
				_, err := c.AddSubscription(fmt.Sprintf("aeron:udp?endpoint=localhost:%d|tags=%d", nextPort(), tag), nextStream())
				require.NoError(t, err)
				_, err = c.AddSubscription(fmt.Sprintf("aeron:udp?endpoint=localhost:%d|tags=%d", nextPort(), tag), nextStream())
				return err
			}},
		{name: "ExclusiveSessionIDClash", code: aeron.ErrorCodeInvalidChannel, contains: "clashing sessionId",
			run: func(t *testing.T, c *client) error {
				ch, stream := "aeron:ipc?session-id=98001", nextStream()
				_, err := c.AddExclusivePublication(ch, stream)
				require.NoError(t, err)
				_, err = c.AddExclusivePublication(ch, stream)
				return err
			}},
		{name: "ConcurrentPubOnExclusiveSessionID", code: aeron.ErrorCodeInvalidChannel, contains: "session",
			run: func(t *testing.T, c *client) error {
				port := nextPort()
				ch, stream := fmt.Sprintf("aeron:udp?endpoint=localhost:%d|session-id=98002", port), nextStream()
				_, err := c.AddExclusivePublication(ch, stream)
				require.NoError(t, err)
				_, err = c.AddPublication(ch, stream)
				return err
			}},
		{name: "AddDestinationUnknownPublication", code: aeron.ErrorCodeUnknownPublication, contains: "unknown publication",
			run: func(t *testing.T, c *client) error {
				_, err := c.AddDestination(c.NextCorrelationID()+1_000_000, udp(nextPort()))
				return err
			}},
		{name: "RemoveDestinationUnknownPublication", code: aeron.ErrorCodeUnknownPublication, contains: "unknown publication",
			run: func(t *testing.T, c *client) error {
				return c.RemoveDestination(c.NextCorrelationID()+1_000_000, udp(nextPort()))
			}},
		{name: "AddRcvDestinationNotMDS", code: aeron.ErrorCodeUnknownSubscription, contains: "unknown MDS subscription",
			run: func(t *testing.T, c *client) error {
				sub, err := c.AddSubscription(udp(nextPort()), nextStream())
				require.NoError(t, err)
				return c.AddRcvDestination(sub.RegistrationID(), udp(nextPort()))
			}},
	}
}

// TestDriverErrorResponsesAreTypedRegistrationErrors checks every driver rejection surfaces as a
// *aeron.RegistrationError (Java RegistrationException) with the Java error code, from both the blocking and the
// async API, that the ErrorHandler is not invoked for errors returned to the caller, and that the client stays
// fully usable and leak free.
func TestDriverErrorResponsesAreTypedRegistrationErrors(t *testing.T) {
	setup(t)
	before := snapshot(t)
	c := connect(t, 5*time.Second)
	for _, tc := range driverErrorCases() {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			var err error
			within(t, 10*time.Second, tc.name, func() { err = tc.run(t, c) })
			var regErr *aeron.RegistrationError
			require.Truef(t, errors.As(err, &regErr), "want RegistrationError, got %T %v", err, err)
			t.Logf("%s: code=%v value=%d corr=%d msg=%q", tc.name, regErr.Code, regErr.ErrorCodeValue, regErr.CorrelationID,
				regErr.Error())
			require.Equal(t, tc.code, regErr.Code, regErr.Error())
			require.Equal(t, int32(tc.code), regErr.ErrorCodeValue)
			require.Contains(t, regErr.Error(), tc.contains)
			require.NotZero(t, regErr.CorrelationID)
			require.False(t, c.IsClosed())
		})
	}
	require.Empty(t, c.errs.all(), "errors returned to the caller must not also reach the ErrorHandler")

	t.Run("AsyncAddPublicationInvalid", func(t *testing.T) {
		id, err := c.AsyncAddPublication("aeron:udp?interface=localhost", nextStream())
		require.NoError(t, err)
		var regErr *aeron.RegistrationError
		awaitTrue(t, 5*time.Second, "async error", func() bool {
			pub, err := c.GetPublication(id)
			require.Nil(t, pub)
			return errors.As(err, &regErr)
		})
		require.Equal(t, id, regErr.CorrelationID)
		require.Equal(t, aeron.ErrorCodeInvalidChannel, regErr.Code)
		require.False(t, c.IsCommandActive(id))
	})
	t.Run("AsyncAddSubscriptionInvalid", func(t *testing.T) {
		id, err := c.AsyncAddSubscription("aeron:udp?interface=localhost", nextStream())
		require.NoError(t, err)
		var regErr *aeron.RegistrationError
		awaitTrue(t, 5*time.Second, "async error", func() bool {
			sub, err := c.GetSubscription(id)
			require.Nil(t, sub)
			return errors.As(err, &regErr)
		})
		require.Equal(t, id, regErr.CorrelationID)
	})
	require.Empty(t, c.errs.all(), "async add errors are reported through Get*, not the ErrorHandler")

	t.Run("RemoveDestinationNotAddedIsIgnoredByDriver", func(t *testing.T) {
		pub, err := c.AddPublication("aeron:udp?control-mode=manual", nextStream())
		require.NoError(t, err)
		require.NoError(t, c.RemoveDestination(pub.RegistrationID(), udp(nextPort())))
	})
	t.Run("AsyncAddDestinationErrorGoesToErrorHandler", func(t *testing.T) {
		id, err := c.AsyncAddDestination(c.NextCorrelationID()+1_000_000, udp(nextPort()))
		require.NoError(t, err)
		var regErr *aeron.RegistrationError
		awaitTrue(t, 5*time.Second, "error handler", func() bool { return c.errs.find(&regErr) })
		require.Equal(t, id, regErr.CorrelationID)
		require.Equal(t, aeron.ErrorCodeUnknownPublication, regErr.Code)
		require.False(t, c.IsCommandActive(id))
	})

	requireIPCRoundTrip(t)
	require.NoError(t, c.Close())
	assertNoLeaks(t, before)
}
