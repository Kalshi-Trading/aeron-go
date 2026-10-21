package systests

import (
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/lirm/aeron-go/aeron"
	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/counters"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func allocatedCounterWithLabel(reader *counters.Reader, label string) int32 {
	id := counters.NullCounterId
	reader.ForEach(func(_ int64, counterId int32, l string) {
		if l == label {
			id = counterId
		}
	})
	return id
}

// Removing a counter whose ADD_COUNTER is still in flight must send REMOVE_COUNTER so the Java driver frees it
// (ClientConductor.asyncRemoveCounter), rather than leaking it until the client closes.
func TestCountersE2EAsyncRemovePendingCounter(t *testing.T) {
	dir := countersE2EDir(t)
	client := connectE2E(t, dir)
	defer client.Close()
	reader := client.CounterReader()
	label := "go-e2e-pending-remove-" + strconv.FormatInt(time.Now().UnixNano(), 36)

	regID, err := client.AsyncAddCounterByLabel(9201, label)
	require.NoError(t, err)
	require.NoError(t, client.AsyncRemoveCounter(regID))

	// A second counter added afterwards proves the driver processed both earlier commands.
	marker, err := client.AddCounterByLabel(9201, label+"-marker")
	require.NoError(t, err)
	waitFor(t, 5*time.Second, "pending counter to be removed by the driver", func() bool {
		return allocatedCounterWithLabel(reader, label) == counters.NullCounterId
	})
	_, err = client.GetCounter(regID)
	assert.Error(t, err)
	require.NoError(t, marker.Close())
}

func TestCountersE2EValidationAndWrapExisting(t *testing.T) {
	dir := countersE2EDir(t)
	client := connectE2E(t, dir)
	defer client.Close()
	reader := client.CounterReader()

	_, err := client.AddCounterByLabel(9202, strings.Repeat("x", int(counters.MaxLabelLength)+1))
	assert.Error(t, err)
	buf := atomic.MakeBuffer(make([]byte, 256))
	_, err = client.AddCounter(9202, buf, 0, counters.MaxKeyLength+1, buf, 0, 4)
	assert.Error(t, err)
	err = client.AsyncRemoveCounter(aeron.NullValue)
	assert.NoError(t, err)

	label := "go-e2e-wrap-" + strconv.FormatInt(time.Now().UnixNano(), 36)
	buf.PutInt64(0, 0x0102030405060708)
	labelBuf := atomic.MakeBuffer([]byte(label))
	owned, err := client.AddCounter(9202, buf, 0, counters.MaxKeyLength, labelBuf, 0, labelBuf.Capacity())
	require.NoError(t, err)
	k, err := reader.GetKeyPartInt64(owned.Id(), 0)
	require.NoError(t, err)
	assert.EqualValues(t, 0x0102030405060708, k)

	view, err := aeron.NewCounterFromReader(reader, owned.Id())
	require.NoError(t, err)
	assert.Equal(t, owned.RegistrationId(), view.RegistrationId())
	assert.EqualValues(t, aeron.NullValue, view.CorrelationId())
	view.Set(31)
	assert.EqualValues(t, 31, owned.Get())
	require.NoError(t, view.Close())
	assert.Equal(t, counters.RecordAllocated, reader.GetCounterState(owned.Id()), "view close must not free")

	id := owned.Id()
	require.NoError(t, owned.Close())
	waitFor(t, 5*time.Second, "owned counter to be freed", func() bool {
		return reader.GetCounterState(id) != counters.RecordAllocated || reader.GetCounterLabel(id) != label
	})
	_, err = aeron.NewCounterFromReader(reader, id)
	if reader.GetCounterState(id) != counters.RecordAllocated {
		assert.Error(t, err)
	}
}
