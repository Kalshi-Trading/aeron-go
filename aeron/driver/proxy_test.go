package driver

import (
	"net"
	"testing"

	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/command"
	rb "github.com/lirm/aeron-go/aeron/ringbuffer"
	"github.com/lirm/aeron-go/aeron/util"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const testRingCapacity = 64 * 1024

type record struct {
	msgTypeID int32
	buffer    *atomic.Buffer
	offset    int32
	length    int32
}

type ringFixture struct {
	buffer *atomic.Buffer
	ring   rb.ManyToOne
	proxy  Proxy
	next   int32
}

func newRingFixture(t *testing.T) *ringFixture {
	f := &ringFixture{}
	f.buffer = atomic.MakeBuffer(make([]byte, testRingCapacity+util.CacheLineLength*12))
	f.ring.Init(f.buffer)
	f.proxy.InitWithClientID(&f.ring, 42)
	return f
}

// read returns the next record written to the ring buffer (the Go ManyToOne has no reader).
func (f *ringFixture) read(t *testing.T) record {
	recordLength := f.buffer.GetInt32(f.next)
	require.Greater(t, recordLength, int32(0), "no record at %d", f.next)
	r := record{
		msgTypeID: f.buffer.GetInt32(f.next + 4),
		buffer:    f.buffer,
		offset:    f.next + rb.RecordDescriptor.HeaderLength,
		length:    recordLength - rb.RecordDescriptor.HeaderLength,
	}
	f.next += util.AlignInt32(recordLength, rb.RecordDescriptor.RecordAlignment)
	return r
}

func TestProxyEncodesEveryCommand(t *testing.T) {
	f := newRingFixture(t)
	p := &f.proxy
	channel := "aeron:udp?endpoint=localhost:20121|mtu=1408"

	id, err := p.AddPublication(channel, 1001)
	require.NoError(t, err)
	r := f.read(t)
	assert.Equal(t, command.AddPublication, r.msgTypeID)
	var pub command.PublicationMessage
	pub.Wrap(r.buffer, r.offset)
	assert.Equal(t, int64(42), pub.ClientID())
	assert.Equal(t, id, pub.CorrelationID())
	assert.Equal(t, int32(1001), pub.StreamID())
	assert.Equal(t, channel, pub.Channel())
	assert.Equal(t, command.ComputePublicationMessageLength(len(channel)), r.length)
	assert.NoError(t, pub.ValidateLength(r.msgTypeID, r.length))

	_, err = p.AddExclusivePublication(channel, 1001)
	require.NoError(t, err)
	assert.Equal(t, command.AddExclusivePublication, f.read(t).msgTypeID)

	id, err = p.RemovePublication(77, true)
	require.NoError(t, err)
	r = f.read(t)
	assert.Equal(t, command.RemovePublication, r.msgTypeID)
	assert.Equal(t, command.RemovePublicationMessageLength, r.length)
	var rp command.RemovePublicationMessage
	rp.Wrap(r.buffer, r.offset)
	assert.Equal(t, id, rp.CorrelationID())
	assert.Equal(t, int64(77), rp.RegistrationID())
	assert.True(t, rp.Revoke(r.length))

	_, err = p.RemovePublication(78, false)
	require.NoError(t, err)
	r = f.read(t)
	rp.Wrap(r.buffer, r.offset)
	assert.False(t, rp.Revoke(r.length))

	id, err = p.AddSubscription(channel, 1002)
	require.NoError(t, err)
	r = f.read(t)
	assert.Equal(t, command.AddSubscription, r.msgTypeID)
	var sub command.SubscriptionMessage
	sub.Wrap(r.buffer, r.offset)
	assert.Equal(t, id, sub.CorrelationID())
	assert.Equal(t, NullValue, sub.RegistrationCorrelationID())
	assert.Equal(t, int32(1002), sub.StreamID())
	assert.Equal(t, channel, sub.Channel())

	for _, tc := range []struct {
		msgTypeID int32
		call      func() (int64, error)
	}{
		{command.RemoveSubscription, func() (int64, error) { return p.RemoveSubscription(88) }},
		{command.RemoveCounter, func() (int64, error) { return p.RemoveCounter(88) }},
	} {
		id, err = tc.call()
		require.NoError(t, err)
		r = f.read(t)
		assert.Equal(t, tc.msgTypeID, r.msgTypeID)
		assert.Equal(t, command.RemoveMessageLength, r.length)
		var rm command.RemoveMessage
		rm.Wrap(r.buffer, r.offset)
		assert.Equal(t, id, rm.CorrelationID())
		assert.Equal(t, int64(88), rm.RegistrationID())
	}

	for _, tc := range []struct {
		msgTypeID int32
		call      func(int64, string) (int64, error)
	}{
		{command.AddDestination, p.AddDestination},
		{command.RemoveDestination, p.RemoveDestination},
		{command.AddRcvDestination, p.AddRcvDestination},
		{command.RemoveRcvDestination, p.RemoveRcvDestination},
	} {
		id, err = tc.call(99, channel)
		require.NoError(t, err)
		r = f.read(t)
		assert.Equal(t, tc.msgTypeID, r.msgTypeID)
		var d command.DestinationMessage
		d.Wrap(r.buffer, r.offset)
		assert.Equal(t, id, d.CorrelationID())
		assert.Equal(t, int64(99), d.RegistrationCorrelationID())
		assert.Equal(t, channel, d.Channel())
		assert.NoError(t, d.ValidateLength(r.msgTypeID, r.length))
	}

	id, err = p.RemoveDestinationByID(5, 6)
	require.NoError(t, err)
	r = f.read(t)
	assert.Equal(t, command.RemoveDestinationByID, r.msgTypeID)
	var byID command.DestinationByIDMessage
	byID.Wrap(r.buffer, r.offset)
	assert.Equal(t, id, byID.CorrelationID())
	assert.Equal(t, int64(5), byID.ResourceRegistrationID())
	assert.Equal(t, int64(6), byID.DestinationRegistrationID())

	key := atomic.MakeBuffer([]byte{0xAA, 1, 2, 3, 4, 5, 0xBB})
	label := atomic.MakeBuffer([]byte("xxcounter label"))
	id, err = p.AddCounter(1001, key, 1, 5, label, 2, 13)
	require.NoError(t, err)
	r = f.read(t)
	assert.Equal(t, command.AddCounter, r.msgTypeID)
	var ctr command.CounterMessage
	ctr.Wrap(r.buffer, r.offset)
	assert.Equal(t, id, ctr.CorrelationID())
	assert.Equal(t, int32(1001), ctr.TypeID())
	assert.Equal(t, []byte{1, 2, 3, 4, 5}, ctr.Key())
	assert.Equal(t, "counter label", ctr.Label())
	assert.NoError(t, ctr.ValidateLength(r.msgTypeID, r.length), "unaligned key length must still validate")

	_, err = p.AddCounterByLabel(1002, "by label")
	require.NoError(t, err)
	r = f.read(t)
	ctr.Wrap(r.buffer, r.offset)
	assert.Equal(t, int32(0), ctr.KeyBufferLength())
	assert.Equal(t, "by label", ctr.Label())

	id, err = p.AddStaticCounter(1003, key, 1, 5, label, 2, 13, 12345)
	require.NoError(t, err)
	r = f.read(t)
	assert.Equal(t, command.AddStaticCounter, r.msgTypeID)
	var sctr command.StaticCounterMessage
	sctr.Wrap(r.buffer, r.offset)
	assert.Equal(t, id, sctr.CorrelationID())
	assert.Equal(t, int64(12345), sctr.RegistrationID())
	assert.Equal(t, int32(1003), sctr.TypeID())
	assert.Equal(t, []byte{1, 2, 3, 4, 5}, sctr.Key())
	assert.Equal(t, "counter label", sctr.Label())
	assert.NoError(t, sctr.ValidateLength(r.msgTypeID, r.length))

	_, err = p.AddStaticCounterByLabel(1004, "static", 6789)
	require.NoError(t, err)
	r = f.read(t)
	sctr.Wrap(r.buffer, r.offset)
	assert.Equal(t, int64(6789), sctr.RegistrationID())
	assert.Equal(t, "static", sctr.Label())

	require.NoError(t, p.ClientKeepalive())
	r = f.read(t)
	assert.Equal(t, command.ClientKeepalive, r.msgTypeID)
	assert.Equal(t, command.CorrelatedMessageLength, r.length)

	require.NoError(t, p.ClientClose())
	r = f.read(t)
	assert.Equal(t, command.ClientClose, r.msgTypeID)
	var cm command.CorrelatedMessage
	cm.Wrap(r.buffer, r.offset)
	assert.Equal(t, int64(42), cm.ClientID())
	assert.Equal(t, NullValue, cm.CorrelationID())

	require.NoError(t, p.TerminateDriver([]byte("secret")))
	r = f.read(t)
	assert.Equal(t, command.TerminateDriver, r.msgTypeID)
	var term command.TerminateDriverMessage
	term.Wrap(r.buffer, r.offset)
	assert.Equal(t, []byte("secret"), term.Token())
	assert.NoError(t, term.ValidateLength(r.msgTypeID, r.length))

	require.NoError(t, p.TerminateDriver(nil))
	r = f.read(t)
	term.Wrap(r.buffer, r.offset)
	assert.Equal(t, int32(0), term.TokenBufferLength())

	id, err = p.RejectImage(55, 4096, "bad")
	require.NoError(t, err)
	r = f.read(t)
	assert.Equal(t, command.RejectImage, r.msgTypeID)
	var rej command.RejectImageMessage
	rej.Wrap(r.buffer, r.offset)
	assert.Equal(t, id, rej.CorrelationID())
	assert.Equal(t, int64(55), rej.ImageCorrelationID())
	assert.Equal(t, int64(4096), rej.Position())
	assert.Equal(t, "bad", rej.Reason())

	id, err = p.NextAvailableSessionID(1005)
	require.NoError(t, err)
	r = f.read(t)
	assert.Equal(t, command.GetNextAvailableSessionID, r.msgTypeID)
	var ns command.GetNextAvailableSessionIDMessage
	ns.Wrap(r.buffer, r.offset)
	assert.Equal(t, id, ns.CorrelationID())
	assert.Equal(t, int32(1005), ns.StreamID())
}

func TestProxyLongChannel(t *testing.T) {
	// The previous implementation encoded into a fixed 512 byte scratch buffer and panicked on long channels.
	f := newRingFixture(t)
	channel := "aeron:udp?endpoint=localhost:20121|alias="
	for len(channel) < 2000 {
		channel += "x"
	}
	_, err := f.proxy.AddPublication(channel, 1)
	require.NoError(t, err)
	r := f.read(t)
	var pub command.PublicationMessage
	pub.Wrap(r.buffer, r.offset)
	assert.Equal(t, channel, pub.Channel())
}

func TestProxyReturnsErrorWhenRingFull(t *testing.T) {
	f := newRingFixture(t)
	var err error
	for i := 0; i < testRingCapacity && err == nil; i++ {
		_, err = f.proxy.AddRcvDestination(1, "aeron:udp?endpoint=localhost:20121")
	}
	assert.Error(t, err, "AddRcvDestination must report a failed write (it used to return nil)")
}

type recordingListener struct {
	calls []string
	args  []interface{}
}

func (l *recordingListener) record(name string, args ...interface{}) {
	l.calls = append(l.calls, name)
	l.args = append(l.args, args)
}

func (l *recordingListener) OnNewPublication(streamID int32, sessionID int32, positionLimitCounterID int32,
	channelStatusIndicatorID int32, logFileName string, correlationID int64, registrationID int64) {
	l.record("OnNewPublication", streamID, sessionID, positionLimitCounterID, channelStatusIndicatorID, logFileName,
		correlationID, registrationID)
}
func (l *recordingListener) OnNewExclusivePublication(streamID int32, sessionID int32, positionLimitCounterID int32,
	channelStatusIndicatorID int32, logFileName string, correlationID int64, registrationID int64) {
	l.record("OnNewExclusivePublication", streamID, sessionID, positionLimitCounterID, channelStatusIndicatorID,
		logFileName, correlationID, registrationID)
}
func (l *recordingListener) OnAvailableImage(streamID int32, sessionID int32, logFilename string,
	sourceIdentity string, subscriberPositionID int32, subsRegID int64, correlationID int64) {
	l.record("OnAvailableImage", streamID, sessionID, logFilename, sourceIdentity, subscriberPositionID, subsRegID,
		correlationID)
}
func (l *recordingListener) OnUnavailableImage(correlationID int64, subscriptionRegistrationID int64) {
	l.record("OnUnavailableImage", correlationID, subscriptionRegistrationID)
}
func (l *recordingListener) OnOperationSuccess(correlationID int64) {
	l.record("OnOperationSuccess", correlationID)
}
func (l *recordingListener) OnErrorResponse(offendingCommandCorrelationID int64, errorCode int32, errorMessage string) {
	l.record("OnErrorResponse", offendingCommandCorrelationID, errorCode, errorMessage)
}
func (l *recordingListener) OnChannelEndpointError(correlationID int64, errorMessage string) {
	l.record("OnChannelEndpointError", correlationID, errorMessage)
}
func (l *recordingListener) OnSubscriptionReady(correlationID int64, channelStatusIndicatorID int32) {
	l.record("OnSubscriptionReady", correlationID, channelStatusIndicatorID)
}
func (l *recordingListener) OnAvailableCounter(correlationID int64, counterID int32) {
	l.record("OnAvailableCounter", correlationID, counterID)
}
func (l *recordingListener) OnUnavailableCounter(correlationID int64, counterID int32) {
	l.record("OnUnavailableCounter", correlationID, counterID)
}
func (l *recordingListener) OnClientTimeout(clientID int64) { l.record("OnClientTimeout", clientID) }
func (l *recordingListener) OnPublicationError(buffer *atomic.Buffer, offset int32, length int32) {
	var frame command.PublicationErrorFrame
	l.record("OnPublicationError", frame.Wrap(buffer, offset).Value(), length)
}

type extendedListener struct {
	recordingListener
}

func (l *extendedListener) OnStaticCounter(correlationID int64, counterID int32) {
	l.record("OnStaticCounter", correlationID, counterID)
}

func (l *extendedListener) OnNextAvailableSessionID(correlationID int64, nextSessionID int32) {
	l.record("OnNextAvailableSessionID", correlationID, nextSessionID)
}

func TestListenerAdapterDispatch(t *testing.T) {
	listener := &extendedListener{}
	adapter := NewAdapter(listener, nil)
	buf := atomic.MakeBuffer(make([]byte, 1024))
	const off = 64

	var pr command.PublicationBuffersReady
	pr.Wrap(buf, off)
	pr.SetCorrelationID(1)
	pr.SetRegistrationID(2)
	pr.SetSessionID(3)
	pr.SetStreamID(4)
	pr.SetPublicationLimitCounterID(5)
	pr.SetChannelStatusIndicatorID(6)
	pr.SetLogFileName("log")
	adapter.OnMessage(command.OnPublicationReady, buf, off, pr.Length())
	adapter.OnMessage(command.OnExclusivePublicationReady, buf, off, pr.Length())

	var ir command.ImageBuffersReady
	ir.Wrap(buf, off)
	ir.SetCorrelationID(10)
	ir.SetSessionID(11)
	ir.SetStreamID(12)
	ir.SetSubscriptionRegistrationID(13)
	ir.SetSubscriberPositionID(14)
	ir.SetLogFileName("image.log")
	ir.SetSourceIdentity("127.0.0.1:1234")
	adapter.OnMessage(command.OnAvailableImage, buf, off, ir.Length())

	var sr command.SubscriptionReady
	sr.Wrap(buf, off)
	sr.SetCorrelationID(20)
	sr.SetChannelStatusIndicatorID(21)
	adapter.OnMessage(command.OnSubscriptionReady, buf, off, sr.Length())

	var os command.OperationSucceeded
	os.Wrap(buf, off)
	os.SetCorrelationID(30)
	adapter.OnMessage(command.OnOperationSuccess, buf, off, os.Length())

	var im command.ImageMessage
	im.Wrap(buf, off)
	im.SetCorrelationID(40)
	im.SetSubscriptionRegistrationID(41)
	im.SetStreamID(42)
	im.SetChannel("chan")
	adapter.OnMessage(command.OnUnavailableImage, buf, off, im.Length())

	var er command.ErrorResponse
	er.Wrap(buf, off)
	er.SetOffendingCommandCorrelationID(50)
	er.SetErrorCode(command.ErrorCodeInvalidChannel)
	er.SetErrorMessage("bad channel")
	adapter.OnMessage(command.OnError, buf, off, er.Length())
	er.SetErrorCode(command.ErrorCodeChannelEndpointError)
	adapter.OnMessage(command.OnError, buf, off, er.Length())

	var cu command.CounterUpdate
	cu.Wrap(buf, off)
	cu.SetCorrelationID(60)
	cu.SetCounterID(61)
	adapter.OnMessage(command.OnCounterReady, buf, off, cu.Length())
	adapter.OnMessage(command.OnUnavailableCounter, buf, off, cu.Length())

	var ct command.ClientTimeout
	ct.Wrap(buf, off)
	ct.SetClientID(70)
	adapter.OnMessage(command.OnClientTimeout, buf, off, ct.Length())

	var sc command.StaticCounter
	sc.Wrap(buf, off)
	sc.SetCorrelationID(80)
	sc.SetCounterID(81)
	adapter.OnMessage(command.OnStaticCounter, buf, off, sc.Length())

	var pe command.PublicationErrorFrame
	pe.Wrap(buf, off)
	pe.SetRegistrationID(90)
	pe.SetDestinationRegistrationID(91)
	pe.SetSessionID(92)
	pe.SetStreamID(93)
	pe.SetReceiverID(94)
	pe.SetGroupTag(95)
	require.NoError(t, pe.SetSourceAddress(&net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 9999}))
	pe.SetErrorCode(command.ErrorCodeImageRejected)
	pe.SetErrorMessage("rejected")
	adapter.OnMessage(command.OnPublicationError, buf, off, pe.Length())

	var ns command.NextAvailableSessionID
	ns.Wrap(buf, off)
	ns.SetCorrelationID(100)
	ns.SetNextSessionID(101)
	adapter.OnMessage(command.OnNextAvailableSessionID, buf, off, ns.Length())

	assert.NotPanics(t, func() { adapter.OnMessage(0x0FFF, buf, off, 8) }, "unknown types are ignored as in Java")

	assert.Equal(t, []string{
		"OnNewPublication", "OnNewExclusivePublication", "OnAvailableImage", "OnSubscriptionReady",
		"OnOperationSuccess", "OnUnavailableImage", "OnErrorResponse", "OnChannelEndpointError",
		"OnAvailableCounter", "OnUnavailableCounter", "OnClientTimeout", "OnStaticCounter", "OnPublicationError",
		"OnNextAvailableSessionID",
	}, listener.calls)
	assert.Equal(t, []interface{}{int32(4), int32(3), int32(5), int32(6), "log", int64(1), int64(2)}, listener.args[0])
	assert.Equal(t, []interface{}{int32(12), int32(11), "image.log", "127.0.0.1:1234", int32(14), int64(13),
		int64(10)}, listener.args[2])
	assert.Equal(t, []interface{}{int64(50), command.ErrorCodeInvalidChannel, "bad channel"}, listener.args[6])
	pubErr := listener.args[12].([]interface{})[0].(command.PublicationError)
	assert.Equal(t, int64(90), pubErr.RegistrationID)
	assert.Equal(t, "127.0.0.1:9999", pubErr.SourceAddress.String())
	assert.Equal(t, "rejected", pubErr.ErrorMessage)
	assert.Equal(t, []interface{}{int64(100), int32(101)}, listener.args[13])
}

func TestListenerAdapterSkipsOptionalCallbacks(t *testing.T) {
	listener := &recordingListener{}
	adapter := NewAdapter(listener, nil)
	buf := atomic.MakeBuffer(make([]byte, 256))
	assert.NotPanics(t, func() {
		adapter.OnMessage(command.OnStaticCounter, buf, 0, command.StaticCounterLength)
		adapter.OnMessage(command.OnNextAvailableSessionID, buf, 0, command.NextAvailableSessionIDLength)
	})
	assert.Empty(t, listener.calls)
}
