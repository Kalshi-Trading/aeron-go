package driver

import (
	"fmt"
	"math/rand"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/broadcast"
	"github.com/lirm/aeron-go/aeron/command"
	"github.com/lirm/aeron-go/aeron/counters"
	rb "github.com/lirm/aeron-go/aeron/ringbuffer"
	"github.com/lirm/aeron-go/aeron/util/memmap"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// End-to-end tests against a real Java 1.53.3 media driver (see docker/README.md):
//
//	docker compose -f docker/docker-compose.yml up -d --build archiving-driver
//	AERON_E2E=1 AERON_DIR=/dev/shm/aeron-compose go test ./aeron/driver/ -run E2E -v

const clientHeartbeatTypeID int32 = 11

func e2eDir(t *testing.T, envName, def string) string {
	if os.Getenv("AERON_E2E") != "1" {
		t.Skip("set AERON_E2E=1 to run against a Java media driver")
	}
	dir := os.Getenv(envName)
	if dir == "" {
		dir = def
	}
	return dir
}

type e2eClient struct {
	t        *testing.T
	cnc      *memmap.File
	meta     *counters.MetaDataFlyweight
	ring     rb.ManyToOne
	proxy    Proxy
	adapter  *ListenerAdapter
	listener *extendedListener
	reader   *counters.Reader
	// heartbeat keeps the driver's client liveness check satisfied, like the client conductor does.
	heartbeat bool
}

func newE2EClient(t *testing.T, dir string) *e2eClient {
	meta, cnc, err := counters.MapFile(filepath.Join(dir, "cnc.dat"))
	require.NoError(t, err, "is the Java driver running with aeron.dir=%s?", dir)
	c := &e2eClient{t: t, cnc: cnc, meta: meta, listener: &extendedListener{}, heartbeat: true}
	c.ring.Init(meta.ToDriverBuf.Get())
	c.proxy.Init(&c.ring)
	receiver, err := broadcast.NewReceiver(meta.ToClientsBuf.Get())
	require.NoError(t, err)
	c.adapter = NewAdapter(c.listener, broadcast.NewCopyReceiver(receiver))
	c.reader = counters.NewReader(meta.ValuesBuf.Get(), meta.MetaDataBuf.Get())
	t.Cleanup(func() { _ = c.cnc.Close() })
	return c
}

func (c *e2eClient) doHeartbeat() {
	if !c.heartbeat {
		return
	}
	id := c.reader.FindCounter(clientHeartbeatTypeID, func(key *atomic.Buffer) bool {
		return key.GetInt64(0) == c.proxy.ClientID()
	})
	if id != counters.NullCounterId {
		ctr, err := counters.NewAtomicCounter(c.reader, id)
		if err == nil {
			ctr.Set(time.Now().UnixMilli())
		}
	}
}

// await polls the broadcast buffer until a callback named name satisfies match, returning its arguments.
func (c *e2eClient) await(timeout time.Duration, name string, match func(args []interface{}) bool) []interface{} {
	c.t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		for i := range c.listener.calls {
			if c.listener.calls[i] == name && c.listener.args[i] != nil {
				args := c.listener.args[i].([]interface{})
				if match == nil || match(args) {
					c.listener.args[i] = nil // consume
					return args
				}
			}
		}
		if time.Now().After(deadline) {
			c.t.Fatalf("timed out waiting for %s; received %v", name, c.listener.calls)
		}
		c.doHeartbeat()
		if c.adapter.ReceiveMessages() == 0 {
			time.Sleep(time.Millisecond)
		}
	}
}

func corrIs(index int, id int64) func([]interface{}) bool {
	return func(args []interface{}) bool { return args[index].(int64) == id }
}

func (c *e2eClient) awaitSuccess(correlationID int64) {
	c.t.Helper()
	c.await(5*time.Second, "OnOperationSuccess", corrIs(0, correlationID))
}

func (c *e2eClient) counterLabel(counterID int32) string {
	label := ""
	c.reader.Scan(func(ctr counters.Counter) {
		if ctr.Id == counterID {
			label = ctr.Label
		}
	})
	return label
}

func TestE2EEveryCommand(t *testing.T) {
	dir := e2eDir(t, "AERON_DIR", "/dev/shm/aeron-compose")
	c := newE2EClient(t, dir)
	p := &c.proxy
	base := 21000 + rand.Intn(2000)*4
	udp := func(port int) string { return fmt.Sprintf("aeron:udp?endpoint=localhost:%d", port) }
	streamID := int32(30000 + rand.Intn(10000))

	subID, err := p.AddSubscription(udp(base), streamID)
	require.NoError(t, err)
	args := c.await(5*time.Second, "OnSubscriptionReady", corrIs(0, subID))
	t.Logf("ON_SUBSCRIPTION_READY correlationId=%d channelStatusIndicatorId=%d", subID, args[1])

	pubID, err := p.AddPublication(udp(base), streamID)
	require.NoError(t, err)
	args = c.await(5*time.Second, "OnNewPublication", corrIs(5, pubID))
	assert.Equal(t, streamID, args[0])
	assert.Equal(t, pubID, args[6], "registration id of a new publication is its correlation id")
	assert.Contains(t, args[4], ".logbuffer")
	pubSessionID := args[1].(int32)
	t.Logf("ON_PUBLICATION_READY %v", args)

	excID, err := p.AddExclusivePublication(udp(base+1), streamID)
	require.NoError(t, err)
	args = c.await(5*time.Second, "OnNewExclusivePublication", corrIs(5, excID))
	assert.Equal(t, streamID, args[0])
	t.Logf("ON_EXCLUSIVE_PUBLICATION_READY %v", args)

	args = c.await(10*time.Second, "OnAvailableImage", func(a []interface{}) bool {
		return a[5].(int64) == subID
	})
	assert.Equal(t, pubSessionID, args[1])
	assert.NotEmpty(t, args[3], "source identity")
	imageCorrelationID := args[6].(int64)
	t.Logf("ON_AVAILABLE_IMAGE %v", args)

	// REJECT_IMAGE: the receiver sends an error frame back to the publication -> ON_PUBLICATION_ERROR.
	rejectID, err := p.RejectImage(imageCorrelationID, 0, "go e2e reject")
	require.NoError(t, err)
	c.awaitSuccess(rejectID)
	args = c.await(10*time.Second, "OnPublicationError", func(a []interface{}) bool {
		return a[0].(command.PublicationError).RegistrationID == pubID
	})
	pubErr := args[0].(command.PublicationError)
	assert.Equal(t, command.ErrorCodeImageRejected, pubErr.ErrorCode)
	assert.Equal(t, "go e2e reject", pubErr.ErrorMessage)
	assert.Equal(t, streamID, pubErr.StreamID)
	assert.Equal(t, pubSessionID, pubErr.SessionID)
	require.NotNil(t, pubErr.SourceAddress)
	t.Logf("ON_PUBLICATION_ERROR %+v", pubErr)

	// ON_UNAVAILABLE_IMAGE: an IPC image goes away promptly once its publication is removed.
	ipcSubID, err := p.AddSubscription("aeron:ipc", streamID+3)
	require.NoError(t, err)
	c.await(5*time.Second, "OnSubscriptionReady", corrIs(0, ipcSubID))
	ipcPubID, err := p.AddPublication("aeron:ipc", streamID+3)
	require.NoError(t, err)
	c.await(5*time.Second, "OnNewPublication", corrIs(5, ipcPubID))
	args = c.await(5*time.Second, "OnAvailableImage", func(a []interface{}) bool { return a[5].(int64) == ipcSubID })
	ipcImageID := args[6].(int64)
	remIpc, err := p.RemovePublication(ipcPubID, false)
	require.NoError(t, err)
	c.awaitSuccess(remIpc)
	args = c.await(15*time.Second, "OnUnavailableImage", corrIs(0, ipcImageID))
	assert.Equal(t, ipcSubID, args[1])
	t.Logf("ON_UNAVAILABLE_IMAGE %v", args)

	// GET_NEXT_AVAILABLE_SESSION_ID
	nextID, err := p.NextAvailableSessionID(streamID)
	require.NoError(t, err)
	args = c.await(5*time.Second, "OnNextAvailableSessionID", corrIs(0, nextID))
	t.Logf("ON_NEXT_AVAILABLE_SESSION_ID %v", args)

	// MDC publication destinations, including REMOVE_DESTINATION_BY_ID.
	mdcID, err := p.AddPublication("aeron:udp?control-mode=manual", streamID+1)
	require.NoError(t, err)
	c.await(5*time.Second, "OnNewPublication", corrIs(5, mdcID))
	destID, err := p.AddDestination(mdcID, udp(base+2))
	require.NoError(t, err)
	c.awaitSuccess(destID)
	removeByID, err := p.RemoveDestinationByID(mdcID, destID)
	require.NoError(t, err)
	c.awaitSuccess(removeByID)
	destID, err = p.AddDestination(mdcID, udp(base+2))
	require.NoError(t, err)
	c.awaitSuccess(destID)
	removeID, err := p.RemoveDestination(mdcID, udp(base+2))
	require.NoError(t, err)
	c.awaitSuccess(removeID)

	// MDS subscription destinations.
	mdsID, err := p.AddSubscription("aeron:udp?control-mode=manual", streamID+2)
	require.NoError(t, err)
	c.await(5*time.Second, "OnSubscriptionReady", corrIs(0, mdsID))
	rcvID, err := p.AddRcvDestination(mdsID, udp(base+3))
	require.NoError(t, err)
	c.awaitSuccess(rcvID)
	rcvRemoveID, err := p.RemoveRcvDestination(mdsID, udp(base+3))
	require.NoError(t, err)
	c.awaitSuccess(rcvRemoveID)

	// ADD_COUNTER with an unaligned (5 byte) key exercises the label alignment.
	key := atomic.MakeBuffer([]byte{1, 2, 3, 4, 5})
	label := atomic.MakeBuffer([]byte("go e2e counter"))
	ctrID, err := p.AddCounter(1101, key, 0, 5, label, 0, label.Capacity())
	require.NoError(t, err)
	args = c.await(5*time.Second, "OnAvailableCounter", corrIs(0, ctrID))
	counterID := args[1].(int32)
	assert.Equal(t, "go e2e counter", c.counterLabel(counterID))
	assert.Equal(t, ctrID, c.reader.GetCounterRegistrationId(counterID))
	removeCtrID, err := p.RemoveCounter(ctrID)
	require.NoError(t, err)
	c.awaitSuccess(removeCtrID)
	args = c.await(5*time.Second, "OnUnavailableCounter", corrIs(0, ctrID))
	assert.Equal(t, counterID, args[1])

	labelCtrID, err := p.AddCounterByLabel(1102, "go e2e label counter")
	require.NoError(t, err)
	args = c.await(5*time.Second, "OnAvailableCounter", corrIs(0, labelCtrID))
	labelCounterID := args[1].(int32)
	assert.Equal(t, "go e2e label counter", c.counterLabel(labelCounterID))

	// ADD_STATIC_COUNTER is idempotent for the same (typeId, registrationId).
	staticRegID := int64(1_000_000 + rand.Intn(1_000_000))
	staticID, err := p.AddStaticCounter(1103, key, 0, 5, label, 0, label.Capacity(), staticRegID)
	require.NoError(t, err)
	args = c.await(5*time.Second, "OnStaticCounter", corrIs(0, staticID))
	staticCounterID := args[1].(int32)
	assert.Equal(t, staticRegID, c.reader.GetCounterRegistrationId(staticCounterID))
	assert.Equal(t, "go e2e counter", c.counterLabel(staticCounterID))
	staticID2, err := p.AddStaticCounterByLabel(1103, "ignored", staticRegID)
	require.NoError(t, err)
	args = c.await(5*time.Second, "OnStaticCounter", corrIs(0, staticID2))
	assert.Equal(t, staticCounterID, args[1])

	// Errors.
	badID, err := p.AddPublication("aeron:bogus?x=y", streamID)
	require.NoError(t, err)
	args = c.await(5*time.Second, "OnErrorResponse", corrIs(0, badID))
	assert.Equal(t, command.ErrorCodeInvalidChannel, args[1])
	t.Logf("ON_ERROR %v", args)
	unknownID, err := p.RemoveSubscription(-12345)
	require.NoError(t, err)
	args = c.await(5*time.Second, "OnErrorResponse", corrIs(0, unknownID))
	assert.Equal(t, command.ErrorCodeUnknownSubscription, args[1])
	unknownPub, err := p.RemovePublication(-12345, false)
	require.NoError(t, err)
	args = c.await(5*time.Second, "OnErrorResponse", corrIs(0, unknownPub))
	assert.Equal(t, command.ErrorCodeUnknownPublication, args[1])

	// The driver validates command lengths: a truncated ADD_COUNTER is rejected as MALFORMED_COMMAND.
	short := atomic.MakeBuffer(make([]byte, 128))
	var cm command.CounterMessage
	cm.Wrap(short, 0)
	cm.SetClientID(p.ClientID())
	cm.SetCorrelationID(p.NextCorrelationID())
	cm.SetTypeID(1105)
	cm.SetKey([]byte{1, 2, 3, 4, 5})
	cm.SetLabel("truncated")
	require.True(t, c.ring.Write(command.AddCounter, short, 0, cm.Length()-1))
	// Validation fails before the driver reads the correlation id, so the error is not correlated (id 0).
	args = c.await(5*time.Second, "OnErrorResponse", func(a []interface{}) bool {
		return a[1].(int32) == command.ErrorCodeMalformedCommand
	})
	assert.Contains(t, args[2], "too short for label")
	t.Logf("ON_ERROR %v", args)

	// TERMINATE_DRIVER is denied by the default termination validator: the driver keeps serving commands.
	require.NoError(t, p.TerminateDriver([]byte("not allowed")))
	aliveID, err := p.NextAvailableSessionID(streamID)
	require.NoError(t, err)
	c.await(5*time.Second, "OnNextAvailableSessionID", corrIs(0, aliveID))

	// Removals, including a revoking REMOVE_PUBLICATION.
	require.NoError(t, p.ClientKeepalive())
	remSub, err := p.RemoveSubscription(subID)
	require.NoError(t, err)
	c.awaitSuccess(remSub)
	remPub, err := p.RemovePublication(pubID, true)
	require.NoError(t, err)
	c.awaitSuccess(remPub)
	remExc, err := p.RemovePublication(excID, false)
	require.NoError(t, err)
	c.awaitSuccess(remExc)

	// CLIENT_CLOSE releases the client's remaining resources.
	require.NoError(t, p.ClientClose())
	c.heartbeat = false
	args = c.await(10*time.Second, "OnUnavailableCounter", corrIs(0, labelCtrID))
	assert.Equal(t, labelCounterID, args[1])
}

func TestE2EClientTimeout(t *testing.T) {
	dir := e2eDir(t, "AERON_DIR", "/dev/shm/aeron-compose")
	c := newE2EClient(t, dir)
	c.heartbeat = false
	livenessTimeout := time.Duration(c.meta.ClientLivenessTo.Get())
	if livenessTimeout > 30*time.Second {
		t.Skipf("client liveness timeout %v too long", livenessTimeout)
	}
	// Any command registers the client with the driver; it then times out without heartbeats.
	id, err := c.proxy.AddCounterByLabel(1104, "go e2e timeout")
	require.NoError(t, err)
	c.await(5*time.Second, "OnAvailableCounter", corrIs(0, id))
	c.await(livenessTimeout+10*time.Second, "OnClientTimeout", corrIs(0, c.proxy.ClientID()))
}

func TestE2ETerminateDriver(t *testing.T) {
	dir := e2eDir(t, "AERON_TERMINATE_DIR", "")
	if dir == "" {
		t.Skip("set AERON_TERMINATE_DIR to a driver started from docker/driver-protocol.compose.yml")
	}
	c := newE2EClient(t, dir)
	id, err := c.proxy.NextAvailableSessionID(1)
	require.NoError(t, err)
	c.await(5*time.Second, "OnNextAvailableSessionID", corrIs(0, id))

	require.NoError(t, c.proxy.TerminateDriver(nil))
	deadline := time.Now().Add(15 * time.Second)
	for {
		if _, err := os.Stat(filepath.Join(dir, "cnc.dat")); os.IsNotExist(err) {
			break // aeron.dir.delete.on.shutdown=true: the driver has shut down
		}
		require.True(t, time.Now().Before(deadline), "driver did not terminate")
		time.Sleep(50 * time.Millisecond)
	}
}

// A bind failure on the Java driver is reported as ON_ERROR with GENERIC_ERROR and the command's correlation id;
// it must reach OnErrorResponse (as DriverEventsAdapter does), not OnChannelEndpointError.
func TestE2EAddressInUseIsErrorResponse(t *testing.T) {
	dir := e2eDir(t, "AERON_DIR", "/dev/shm/aeron-compose")
	c := newE2EClient(t, dir)
	conn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	require.NoError(t, err)
	defer conn.Close()
	port := conn.LocalAddr().(*net.UDPAddr).Port

	subID, err := c.proxy.AddSubscription(fmt.Sprintf("aeron:udp?endpoint=127.0.0.1:%d", port), 1001)
	require.NoError(t, err)
	args := c.await(5*time.Second, "OnErrorResponse", corrIs(0, subID))
	t.Logf("ON_ERROR %v", args)
	assert.Equal(t, command.ErrorCodeGenericError, args[1])
	assert.Contains(t, args[2], "Address already in use")
	assert.NotContains(t, c.listener.calls, "OnChannelEndpointError")
}
