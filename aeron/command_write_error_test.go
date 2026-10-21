package aeron

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

// Java DriverProxy throws AeronException (category ERROR) when a command cannot be written to the to-driver ring
// buffer; every client API must surface it as an *AeronError and leave the client open.
func TestCommandWriteFailureIsAeronError(t *testing.T) {
	tc := newTestConductor(t)
	sub := addReadySubscription(t, tc, nil, nil)
	tc.proxy.sendErr = errors.New("failed to write ADD_PUBLICATION command to driver")
	calls := map[string]func() error{
		"AsyncAddPublication":  func() error { _, err := tc.AsyncAddPublication(Channel, StreamId1); return err },
		"AddPublication":       func() error { _, err := tc.AddPublication(Channel, StreamId1); return err },
		"AsyncAddSubscription": func() error { _, err := tc.AsyncAddSubscription(Channel, StreamId1); return err },
		"AsyncAddCounter":      func() error { _, err := tc.AddCounterByLabel(CounterTypeId, CounterLabel); return err },
		"AddCounterSync": func() error {
			_, err := tc.addCounterSync(kindPendingCounter, func() (int64, error) {
				return tc.driverProxy.AddCounterByLabel(CounterTypeId, CounterLabel)
			})
			return err
		},
		"AddDestination":      func() error { return tc.AddDestination(sub.RegistrationID(), Channel) },
		"AsyncAddDestination": func() error { _, err := tc.AsyncAddDestination(sub.RegistrationID(), Channel); return err },
		"NextSessionID":       func() error { _, err := tc.NextSessionID(StreamId1); return err },
		"RemoveSubscription":  func() error { return sub.Close() },
	}
	for name, call := range calls {
		err := call()
		var aerr *AeronError
		require.Truef(t, errors.As(err, &aerr), "%s: want *AeronError, got %T %v", name, err, err)
		require.Equal(t, CategoryError, aerr.Category, name)
		require.Contains(t, err.Error(), "failed to write", name)
		require.False(t, tc.IsClosed(), name)
	}
}
