package command

import (
	"bufio"
	"encoding/hex"
	"net"
	"os"
	"strings"
	"testing"

	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// golden.txt is produced by testdata/GoldenBytes.java using the Java 1.53.3 io.aeron.command flyweights.

const (
	goldenClientID      int64 = 0x0102030405060708
	goldenCorrelationID int64 = 0x1112131415161718
	goldenRegID         int64 = 0x2122232425262728
	goldenChannel             = "aeron:udp?endpoint=localhost:20121"
	goldenKey                 = "key12"
	goldenLabel               = "my counter label"
)

func loadGolden(t *testing.T) map[string][]byte {
	f, err := os.Open("testdata/golden.txt")
	require.NoError(t, err)
	defer f.Close()
	golden := map[string][]byte{}
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 64*1024), 64*1024)
	for scanner.Scan() {
		parts := strings.SplitN(scanner.Text(), " ", 2)
		require.Len(t, parts, 2)
		b, err := hex.DecodeString(parts[1])
		require.NoError(t, err)
		golden[parts[0]] = b
	}
	require.NoError(t, scanner.Err())
	return golden
}

func newBuffer() (*atomic.Buffer, []byte) {
	b := make([]byte, 1024)
	return atomic.MakeBuffer(b), b
}

func wrapBytes(b []byte) *atomic.Buffer {
	c := make([]byte, 1024)
	copy(c, b)
	return atomic.MakeBuffer(c)
}

type encoder func(buf *atomic.Buffer) int32

func goldenEncoders() map[string]encoder {
	return map[string]encoder{
		"CorrelatedMessage": func(buf *atomic.Buffer) int32 {
			var m CorrelatedMessage
			m.Wrap(buf, 0)
			m.SetClientID(goldenClientID)
			m.SetCorrelationID(goldenCorrelationID)
			return m.Length()
		},
		"PublicationMessage": func(buf *atomic.Buffer) int32 {
			var m PublicationMessage
			m.Wrap(buf, 0)
			m.SetClientID(goldenClientID)
			m.SetCorrelationID(goldenCorrelationID)
			m.SetStreamID(1001)
			m.SetChannel(goldenChannel)
			return ComputePublicationMessageLength(len(goldenChannel))
		},
		"SubscriptionMessage": func(buf *atomic.Buffer) int32 {
			var m SubscriptionMessage
			m.Wrap(buf, 0)
			m.SetClientID(goldenClientID)
			m.SetCorrelationID(goldenCorrelationID)
			m.SetRegistrationCorrelationID(-1)
			m.SetStreamID(1002)
			m.SetChannel(goldenChannel)
			return ComputeSubscriptionMessageLength(len(goldenChannel))
		},
		"RemoveMessage": func(buf *atomic.Buffer) int32 {
			var m RemoveMessage
			m.Wrap(buf, 0)
			m.SetClientID(goldenClientID)
			m.SetCorrelationID(goldenCorrelationID)
			m.SetRegistrationID(goldenRegID)
			return m.Length()
		},
		"RemovePublicationMessage": func(buf *atomic.Buffer) int32 {
			var m RemovePublicationMessage
			m.Wrap(buf, 0)
			m.SetClientID(goldenClientID)
			m.SetCorrelationID(goldenCorrelationID)
			m.SetRegistrationID(goldenRegID)
			m.SetRevoke(true)
			return m.Length()
		},
		"DestinationMessage": func(buf *atomic.Buffer) int32 {
			var m DestinationMessage
			m.Wrap(buf, 0)
			m.SetClientID(goldenClientID)
			m.SetCorrelationID(goldenCorrelationID)
			m.SetRegistrationCorrelationID(goldenRegID)
			m.SetChannel(goldenChannel)
			return ComputeDestinationMessageLength(len(goldenChannel))
		},
		"DestinationByIDMessage": func(buf *atomic.Buffer) int32 {
			var m DestinationByIDMessage
			m.Wrap(buf, 0)
			m.SetClientID(goldenClientID)
			m.SetCorrelationID(goldenCorrelationID)
			m.SetResourceRegistrationID(goldenRegID)
			m.SetDestinationRegistrationID(0x3132333435363738)
			return m.Length()
		},
		"CounterMessage": func(buf *atomic.Buffer) int32 {
			var m CounterMessage
			m.Wrap(buf, 0)
			m.SetClientID(goldenClientID)
			m.SetCorrelationID(goldenCorrelationID)
			m.SetTypeID(1101)
			m.SetKey([]byte(goldenKey))
			label := atomic.MakeBuffer([]byte(goldenLabel))
			m.SetLabelBuffer(label, 0, label.Capacity())
			return ComputeCounterMessageLength(len(goldenKey), len(goldenLabel))
		},
		"CounterMessageLabelOnly": func(buf *atomic.Buffer) int32 {
			var m CounterMessage
			m.Wrap(buf, 0)
			m.SetClientID(goldenClientID)
			m.SetCorrelationID(goldenCorrelationID)
			m.SetTypeID(1102)
			m.SetKeyBuffer(nil, 0, 0)
			m.SetLabel("label only")
			return ComputeCounterMessageLength(0, 10)
		},
		"StaticCounterMessage": func(buf *atomic.Buffer) int32 {
			var m StaticCounterMessage
			m.Wrap(buf, 0)
			m.SetClientID(goldenClientID)
			m.SetCorrelationID(goldenCorrelationID)
			m.SetRegistrationID(0x4142434445464748)
			m.SetTypeID(1103)
			key := atomic.MakeBuffer([]byte(goldenKey))
			m.SetKeyBuffer(key, 0, key.Capacity())
			m.SetLabel(goldenLabel)
			return ComputeStaticCounterMessageLength(len(goldenKey), len(goldenLabel))
		},
		"TerminateDriverMessage": func(buf *atomic.Buffer) int32 {
			var m TerminateDriverMessage
			m.Wrap(buf, 0)
			m.SetClientID(goldenClientID)
			m.SetCorrelationID(-1)
			m.SetToken([]byte("tok"))
			return ComputeTerminateDriverMessageLength(3)
		},
		"RejectImageMessage": func(buf *atomic.Buffer) int32 {
			var m RejectImageMessage
			m.Wrap(buf, 0)
			m.SetClientID(goldenClientID)
			m.SetCorrelationID(goldenCorrelationID)
			m.SetImageCorrelationID(0x5152535455565758)
			m.SetPosition(4096)
			m.SetReason("bad image")
			return ComputeRejectImageMessageLength(len("bad image"))
		},
		"GetNextAvailableSessionIDMessage": func(buf *atomic.Buffer) int32 {
			var m GetNextAvailableSessionIDMessage
			m.Wrap(buf, 0)
			m.SetClientID(goldenClientID)
			m.SetCorrelationID(goldenCorrelationID)
			m.SetStreamID(1003)
			return m.Length()
		},
		"ErrorResponse": func(buf *atomic.Buffer) int32 {
			var m ErrorResponse
			m.Wrap(buf, 0)
			m.SetOffendingCommandCorrelationID(goldenCorrelationID)
			m.SetErrorCode(ErrorCodeInvalidChannel)
			m.SetErrorMessage("invalid channel")
			return m.Length()
		},
		"PublicationBuffersReady": func(buf *atomic.Buffer) int32 {
			var m PublicationBuffersReady
			m.Wrap(buf, 0)
			m.SetCorrelationID(goldenCorrelationID)
			m.SetRegistrationID(goldenRegID)
			m.SetSessionID(-12345)
			m.SetStreamID(1001)
			m.SetPublicationLimitCounterID(7)
			m.SetChannelStatusIndicatorID(8)
			m.SetLogFileName("/dev/shm/a/1.logbuffer")
			return m.Length()
		},
		"ImageBuffersReady": func(buf *atomic.Buffer) int32 {
			var m ImageBuffersReady
			m.Wrap(buf, 0)
			m.SetCorrelationID(goldenCorrelationID)
			m.SetSessionID(-12345)
			m.SetStreamID(1002)
			m.SetSubscriptionRegistrationID(goldenRegID)
			m.SetSubscriberPositionID(9)
			m.SetLogFileName("/dev/shm/a/images/2.logbuffer")
			m.SetSourceIdentity("127.0.0.1:20121")
			return m.Length()
		},
		"SubscriptionReady": func(buf *atomic.Buffer) int32 {
			var m SubscriptionReady
			m.Wrap(buf, 0)
			m.SetCorrelationID(goldenCorrelationID)
			m.SetChannelStatusIndicatorID(10)
			return m.Length()
		},
		"OperationSucceeded": func(buf *atomic.Buffer) int32 {
			var m OperationSucceeded
			m.Wrap(buf, 0)
			m.SetCorrelationID(goldenCorrelationID)
			return m.Length()
		},
		"ImageMessage": func(buf *atomic.Buffer) int32 {
			var m ImageMessage
			m.Wrap(buf, 0)
			m.SetCorrelationID(goldenCorrelationID)
			m.SetSubscriptionRegistrationID(goldenRegID)
			m.SetStreamID(1002)
			m.SetChannel(goldenChannel)
			return m.Length()
		},
		"CounterUpdate": func(buf *atomic.Buffer) int32 {
			var m CounterUpdate
			m.Wrap(buf, 0)
			m.SetCorrelationID(goldenCorrelationID)
			m.SetCounterID(11)
			return m.Length()
		},
		"ClientTimeout": func(buf *atomic.Buffer) int32 {
			var m ClientTimeout
			m.Wrap(buf, 0)
			m.SetClientID(goldenClientID)
			return m.Length()
		},
		"StaticCounter": func(buf *atomic.Buffer) int32 {
			var m StaticCounter
			m.Wrap(buf, 0)
			m.SetCorrelationID(goldenCorrelationID)
			m.SetCounterID(12)
			return m.Length()
		},
		"NextAvailableSessionID": func(buf *atomic.Buffer) int32 {
			var m NextAvailableSessionID
			m.Wrap(buf, 0)
			m.SetCorrelationID(goldenCorrelationID)
			m.SetNextSessionID(-777)
			return m.Length()
		},
		"PublicationErrorFrame": func(buf *atomic.Buffer) int32 {
			var m PublicationErrorFrame
			m.Wrap(buf, 0)
			m.SetRegistrationID(goldenRegID)
			m.SetDestinationRegistrationID(-1)
			m.SetSessionID(-12345)
			m.SetStreamID(1001)
			m.SetReceiverID(0x6162636465666768)
			m.SetGroupTag(99)
			if err := m.SetSourceAddress(&net.UDPAddr{IP: net.ParseIP("10.1.2.3"), Port: 40123}); err != nil {
				panic(err)
			}
			m.SetErrorCode(ErrorCodeImageRejected)
			m.SetErrorMessage("rejected")
			return m.Length()
		},
		"PublicationErrorFrameIPv6": func(buf *atomic.Buffer) int32 {
			var m PublicationErrorFrame
			m.Wrap(buf, 0)
			m.SetRegistrationID(1)
			m.SetDestinationRegistrationID(2)
			m.SetSessionID(3)
			m.SetStreamID(4)
			m.SetReceiverID(5)
			m.SetGroupTag(6)
			if err := m.SetSourceAddress(&net.UDPAddr{IP: net.ParseIP("fe80::1"), Port: 65535}); err != nil {
				panic(err)
			}
			m.SetErrorCode(ErrorCodeGenericError)
			m.SetErrorMessage("v6")
			return m.Length()
		},
	}
}

func TestGoldenEncodingMatchesJava(t *testing.T) {
	golden := loadGolden(t)
	encoders := goldenEncoders()
	assert.Len(t, encoders, len(golden), "every golden message must have an encoder")
	for name, want := range golden {
		enc, ok := encoders[name]
		if !assert.True(t, ok, "missing encoder for %s", name) {
			continue
		}
		buf, raw := newBuffer()
		length := enc(buf)
		assert.EqualValues(t, len(want), length, "%s length", name)
		assert.Equal(t, hex.EncodeToString(want), hex.EncodeToString(raw[:len(want)]), "%s bytes", name)
	}
}

func TestGoldenDecodeCommands(t *testing.T) {
	g := loadGolden(t)

	var pub PublicationMessage
	pub.Wrap(wrapBytes(g["PublicationMessage"]), 0)
	assert.Equal(t, goldenClientID, pub.ClientID())
	assert.Equal(t, goldenCorrelationID, pub.CorrelationID())
	assert.EqualValues(t, 1001, pub.StreamID())
	assert.Equal(t, goldenChannel, pub.Channel())
	assert.NoError(t, pub.ValidateLength(AddPublication, int32(len(g["PublicationMessage"]))))
	assert.Error(t, pub.ValidateLength(AddPublication, int32(len(g["PublicationMessage"])-1)))

	var sub SubscriptionMessage
	sub.Wrap(wrapBytes(g["SubscriptionMessage"]), 0)
	assert.EqualValues(t, -1, sub.RegistrationCorrelationID())
	assert.EqualValues(t, 1002, sub.StreamID())
	assert.Equal(t, goldenChannel, sub.Channel())
	assert.EqualValues(t, len(g["SubscriptionMessage"]), sub.Length())

	var rem RemovePublicationMessage
	rem.Wrap(wrapBytes(g["RemovePublicationMessage"]), 0)
	assert.Equal(t, goldenRegID, rem.RegistrationID())
	assert.True(t, rem.Revoke(RemovePublicationMessageLength))
	assert.False(t, rem.Revoke(RemoveMessageLength), "flags absent in a short (legacy) message")

	var dest DestinationMessage
	dest.Wrap(wrapBytes(g["DestinationMessage"]), 0)
	assert.Equal(t, goldenRegID, dest.RegistrationCorrelationID())
	assert.Equal(t, goldenChannel, dest.Channel())

	var byID DestinationByIDMessage
	byID.Wrap(wrapBytes(g["DestinationByIDMessage"]), 0)
	assert.Equal(t, goldenRegID, byID.ResourceRegistrationID())
	assert.EqualValues(t, 0x3132333435363738, byID.DestinationRegistrationID())

	var ctr CounterMessage
	ctr.Wrap(wrapBytes(g["CounterMessage"]), 0)
	assert.EqualValues(t, 1101, ctr.TypeID())
	assert.Equal(t, []byte(goldenKey), ctr.Key())
	assert.EqualValues(t, 24, ctr.KeyBufferOffset())
	assert.EqualValues(t, 36, ctr.LabelBufferOffset(), "label length is 4-byte aligned after a 5 byte key")
	assert.Equal(t, goldenLabel, ctr.Label())
	assert.EqualValues(t, len(g["CounterMessage"])-4, ctr.Length(), "Java claims 4 bytes more than it encodes")
	assert.NoError(t, ctr.ValidateLength(AddCounter, int32(len(g["CounterMessage"]))))
	assert.Error(t, ctr.ValidateLength(AddCounter, ctr.Length()-1))

	var sctr StaticCounterMessage
	sctr.Wrap(wrapBytes(g["StaticCounterMessage"]), 0)
	assert.EqualValues(t, 0x4142434445464748, sctr.RegistrationID())
	assert.EqualValues(t, 1103, sctr.TypeID())
	assert.Equal(t, []byte(goldenKey), sctr.Key())
	assert.Equal(t, goldenLabel, sctr.Label())
	assert.NoError(t, sctr.ValidateLength(AddStaticCounter, int32(len(g["StaticCounterMessage"]))))

	var term TerminateDriverMessage
	term.Wrap(wrapBytes(g["TerminateDriverMessage"]), 0)
	assert.Equal(t, []byte("tok"), term.Token())
	assert.EqualValues(t, -1, term.CorrelationID())
	assert.NoError(t, term.ValidateLength(TerminateDriver, term.Length()))

	var rej RejectImageMessage
	rej.Wrap(wrapBytes(g["RejectImageMessage"]), 0)
	assert.EqualValues(t, 0x5152535455565758, rej.ImageCorrelationID())
	assert.EqualValues(t, 4096, rej.Position())
	assert.Equal(t, "bad image", rej.Reason())

	var next GetNextAvailableSessionIDMessage
	next.Wrap(wrapBytes(g["GetNextAvailableSessionIDMessage"]), 0)
	assert.EqualValues(t, 1003, next.StreamID())
	assert.Equal(t, goldenClientID, next.ClientID())
}

func TestGoldenDecodeResponses(t *testing.T) {
	g := loadGolden(t)

	var er ErrorResponse
	er.Wrap(wrapBytes(g["ErrorResponse"]), 0)
	assert.Equal(t, goldenCorrelationID, er.OffendingCommandCorrelationID())
	assert.Equal(t, ErrorCodeInvalidChannel, er.ErrorCode())
	assert.Equal(t, "invalid channel", er.ErrorMessage())

	var pr PublicationBuffersReady
	pr.Wrap(wrapBytes(g["PublicationBuffersReady"]), 0)
	assert.Equal(t, goldenCorrelationID, pr.CorrelationID())
	assert.Equal(t, goldenRegID, pr.RegistrationID())
	assert.EqualValues(t, -12345, pr.SessionID())
	assert.EqualValues(t, 1001, pr.StreamID())
	assert.EqualValues(t, 7, pr.PublicationLimitCounterID())
	assert.EqualValues(t, 8, pr.ChannelStatusIndicatorID())
	assert.Equal(t, "/dev/shm/a/1.logbuffer", pr.LogFileName())

	var ir ImageBuffersReady
	ir.Wrap(wrapBytes(g["ImageBuffersReady"]), 0)
	assert.Equal(t, goldenCorrelationID, ir.CorrelationID())
	assert.EqualValues(t, -12345, ir.SessionID())
	assert.EqualValues(t, 1002, ir.StreamID())
	assert.Equal(t, goldenRegID, ir.SubscriptionRegistrationID())
	assert.EqualValues(t, 9, ir.SubscriberPositionID())
	assert.Equal(t, "/dev/shm/a/images/2.logbuffer", ir.LogFileName())
	assert.Equal(t, "127.0.0.1:20121", ir.SourceIdentity())

	var sr SubscriptionReady
	sr.Wrap(wrapBytes(g["SubscriptionReady"]), 0)
	assert.EqualValues(t, 10, sr.ChannelStatusIndicatorID())

	var im ImageMessage
	im.Wrap(wrapBytes(g["ImageMessage"]), 0)
	assert.Equal(t, goldenRegID, im.SubscriptionRegistrationID())
	assert.EqualValues(t, 1002, im.StreamID())
	assert.Equal(t, goldenChannel, im.Channel())

	var cu CounterUpdate
	cu.Wrap(wrapBytes(g["CounterUpdate"]), 0)
	assert.EqualValues(t, 11, cu.CounterID())

	var ct ClientTimeout
	ct.Wrap(wrapBytes(g["ClientTimeout"]), 0)
	assert.Equal(t, goldenClientID, ct.ClientID())

	var sc StaticCounter
	sc.Wrap(wrapBytes(g["StaticCounter"]), 0)
	assert.EqualValues(t, 12, sc.CounterID())

	var ns NextAvailableSessionID
	ns.Wrap(wrapBytes(g["NextAvailableSessionID"]), 0)
	assert.EqualValues(t, -777, ns.NextSessionID())

	var pe PublicationErrorFrame
	pe.Wrap(wrapBytes(g["PublicationErrorFrame"]), 0)
	v := pe.Value()
	assert.Equal(t, goldenRegID, v.RegistrationID)
	assert.EqualValues(t, -1, v.DestinationRegistrationID)
	assert.EqualValues(t, -12345, v.SessionID)
	assert.EqualValues(t, 1001, v.StreamID)
	assert.EqualValues(t, 0x6162636465666768, v.ReceiverID)
	assert.EqualValues(t, 99, v.GroupTag)
	assert.Equal(t, "10.1.2.3:40123", v.SourceAddress.String())
	assert.Equal(t, ErrorCodeImageRejected, v.ErrorCode)
	assert.Equal(t, "rejected", v.ErrorMessage)

	pe.Wrap(wrapBytes(g["PublicationErrorFrameIPv6"]), 0)
	addr, err := pe.SourceAddress()
	require.NoError(t, err)
	assert.Equal(t, "[fe80::1]:65535", addr.String())
	assert.Equal(t, "v6", pe.ErrorMessage())
}

func TestNames(t *testing.T) {
	assert.Equal(t, "ADD_STATIC_COUNTER", TypeName(AddStaticCounter))
	assert.Equal(t, "ON_NEXT_AVAILABLE_SESSION_ID", TypeName(OnNextAvailableSessionID))
	assert.Equal(t, "UNKNOWN", TypeName(0x7777))
	assert.Equal(t, "PUBLICATION_REVOKED", ErrorCodeName(ErrorCodePublicationRevoked))
	assert.Equal(t, "UNKNOWN_CODE_VALUE", ErrorCodeName(99))
	assert.EqualValues(t, 1<<16, ControlProtocolSemanticVersion)
}

func TestFlyweightAtNonZeroOffset(t *testing.T) {
	buf, raw := newBuffer()
	var m CounterMessage
	m.Wrap(buf, 100)
	m.SetClientID(1)
	m.SetCorrelationID(2)
	m.SetTypeID(3)
	m.SetKey([]byte{1, 2, 3})
	m.SetLabel("abc")
	assert.Equal(t, "abc", m.Label())
	assert.EqualValues(t, 24+4+4+3, m.Length())
	assert.Equal(t, byte(0), raw[99])
	assert.EqualValues(t, 3, buf.GetInt32(100+24+4))
}

func TestPublicationErrorFrameValidateLength(t *testing.T) {
	buf, _ := newBuffer()
	var pe PublicationErrorFrame
	pe.Wrap(buf, 0)
	pe.SetErrorMessage("rejected")
	assert.NoError(t, pe.ValidateLength(pe.Length()))
	assert.Error(t, pe.ValidateLength(pe.Length()-1), "message truncated")
	assert.Error(t, pe.ValidateLength(pubErrErrorMessageOffset), "missing message length")
	buf.PutInt32(pubErrErrorMessageOffset, -1)
	assert.Error(t, pe.ValidateLength(pe.Length()), "negative message length")
}
