// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package aeron

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Port of io.aeron.ChannelUriStringBuilderTest (1.53.3) plus extra coverage.

func TestChannelUriStringBuilderValidate(t *testing.T) {
	assert.Error(t, NewChannelUriStringBuilder().Validate())
	assert.Error(t, NewChannelUriStringBuilder().Media("udp").Validate())
	assert.Error(t, NewChannelUriStringBuilder().Media("udp").Endpoint("address:port").TermID(999).Validate())
	assert.NoError(t, NewChannelUriStringBuilder().Media("udp").ControlEndpoint("localhost:1").Validate())
	assert.NoError(t, NewChannelUriStringBuilder().Media("ipc").Validate())
	assert.Error(t, NewChannelUriStringBuilder().Media("ipc").InitialTermID(10).TermID(5).TermOffset(0).Validate())
	assert.Error(t, NewChannelUriStringBuilder().Media("ipc").InitialTermID(0).TermID(0).TermOffset(1<<20).
		TermLength(64*1024).Validate())
	assert.NoError(t, NewChannelUriStringBuilder().Media("ipc").InitialTermID(0).TermID(1).TermOffset(64).
		TermLength(64*1024).Validate())
}

func TestChannelUriStringBuilderGenerate(t *testing.T) {
	for _, tc := range []struct {
		name     string
		b        *ChannelUriStringBuilder
		expected string
	}{
		{"basicIpc", NewChannelUriStringBuilder().Media("ipc"), "aeron:ipc"},
		{"basicUdp", NewChannelUriStringBuilder().Media("udp").Endpoint("localhost:9999"),
			"aeron:udp?endpoint=localhost:9999"},
		{"basicUdpSpy", NewChannelUriStringBuilder().Prefix("aeron-spy").Media("udp").Endpoint("localhost:9999"),
			"aeron-spy:aeron:udp?endpoint=localhost:9999"},
		{"complexUdp", NewChannelUriStringBuilder().Media("udp").Endpoint("localhost:9999").Ttl(9).
			TermLength(1024 * 128), "aeron:udp?endpoint=localhost:9999|term-length=128k|ttl=9"},
		{"replayUdp", NewChannelUriStringBuilder().Media("udp").Endpoint("address:9999").TermLength(1024 * 128).
			InitialTermID(777).TermID(999).TermOffset(64),
			"aeron:udp?endpoint=address:9999|term-length=128k|init-term-id=777|term-id=999|term-offset=64"},
		{"socketParams", NewChannelUriStringBuilder().Media("udp").Endpoint("address:9999").
			SocketSndbufLength(8192).SocketRcvbufLength(4096), "aeron:udp?endpoint=address:9999|so-sndbuf=8k|so-rcvbuf=4k"},
		{"receiverWindow", NewChannelUriStringBuilder().Media("udp").Endpoint("address:9999").
			ReceiverWindowLength(8192), "aeron:udp?endpoint=address:9999|rcv-wnd=8k"},
		{"linger", NewChannelUriStringBuilder().Media("ipc").Linger(987654321123456789),
			"aeron:ipc?linger=987654321123456789ns"},
		{"mdcDynamic", NewChannelUriStringBuilder().Media("udp").ControlEndpoint("localhost:40456").
			ControlMode(MdcControlModeDynamic).ChannelAndEntityTags(1, 2),
			"aeron:udp?tags=1,2|control=localhost:40456|control-mode=dynamic"},
		{"taggedSession", NewChannelUriStringBuilder().Media("ipc").TaggedSessionID(123456),
			"aeron:ipc?session-id=tag:123456"},
		{"untaggedSessionAfterTag", NewChannelUriStringBuilder().Media("ipc").TaggedSessionID(5).SessionID(-6),
			"aeron:ipc?session-id=-6"},
		{"maxFc", NewChannelUriStringBuilder().Media("udp").Endpoint("x:1").MaxFlowControl(),
			"aeron:udp?endpoint=x:1|fc=max"},
		{"minFc", NewChannelUriStringBuilder().Media("udp").Endpoint("x:1").MinFlowControl(cubPtr(int32(3)), "5s"),
			"aeron:udp?endpoint=x:1|fc=min,g:/3,t:5s"},
		{"minFcNoOpts", NewChannelUriStringBuilder().Media("udp").Endpoint("x:1").MinFlowControl(nil, ""),
			"aeron:udp?endpoint=x:1|fc=min"},
		{"taggedFc", NewChannelUriStringBuilder().Media("udp").Endpoint("x:1").
			TaggedFlowControl(cubPtr(int64(100)), cubPtr(int32(2)), "1ms").GroupTag(100),
			"aeron:udp?endpoint=x:1|fc=tagged,g:100/2,t:1ms|gtag=100"},
		{"taggedFcGroupOnly", NewChannelUriStringBuilder().Media("udp").Endpoint("x:1").
			TaggedFlowControl(cubPtr(int64(7)), nil, ""), "aeron:udp?endpoint=x:1|fc=tagged,g:7"},
		{"taggedFcMinOnly", NewChannelUriStringBuilder().Media("udp").Endpoint("x:1").
			TaggedFlowControl(nil, cubPtr(int32(4)), ""), "aeron:udp?endpoint=x:1|fc=tagged,g:/4"},
		{"response", NewChannelUriStringBuilder().Media("udp").ControlEndpoint("localhost:10001").
			ControlMode(ControlModeResponse).ResponseCorrelationID(42),
			"aeron:udp?control=localhost:10001|control-mode=response|response-correlation-id=42"},
		{"responsePrototype", NewChannelUriStringBuilder().Media("ipc").ResponseCorrelationIDString("prototype"),
			"aeron:ipc?response-correlation-id=prototype"},
		{"bools", NewChannelUriStringBuilder().Media("ipc").Reliable(false).Sparse(true).Eos(false).Tether(true).
			Group(true).Rejoin(false).SpiesSimulateConnection(true),
			"aeron:ipc?reliable=false|sparse=true|eos=false|tether=true|group=true|rejoin=false|ssc=true"},
		{"misc", NewChannelUriStringBuilder().Media("udp").Endpoint("e:1").NetworkInterface("lo").Alias("a").
			CongestionControl("cubic").Mtu(1408).SocketTos(16).MediaReceiveTimestampOffset("reserved").
			ChannelReceiveTimestampOffset("0").ChannelSendTimestampOffset("8").ResponseEndpoint("r:0"),
			"aeron:udp?endpoint=e:1|interface=lo|mtu=1408|alias=a|cc=cubic|so-tos=16|media-rcv-ts-offset=reserved|" +
				"channel-rcv-ts-offset=0|channel-snd-ts-offset=8|response-endpoint=r:0"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := tc.b.Build()
			require.NoError(t, err)
			assert.Equal(t, tc.expected, got)
			assert.Equal(t, tc.expected, tc.b.String())
			parsed := MustParseChannelUri(got).String()
			assert.Equal(t, parsed, MustParseChannelUri(parsed).String())
		})
	}
}

func TestChannelUriStringBuilderLinger(t *testing.T) {
	b := NewChannelUriStringBuilder().Media("ipc").Linger(987654321123456789)
	v, ok := b.GetLinger()
	assert.True(t, ok)
	assert.Equal(t, time.Duration(987654321123456789), v)

	b = NewChannelUriStringBuilder().Media("udp").Endpoint("address:9999").Linger(5).Unset(LingerParamName)
	_, ok = b.GetLinger()
	assert.False(t, ok)
	assert.Equal(t, "aeron:udp?endpoint=address:9999", b.MustBuild())

	_, err := NewChannelUriStringBuilder().Media("udp").Endpoint("address:9999").Linger(-1).Build()
	require.Error(t, err)
	assert.Equal(t, "`linger` value cannot be negative: -1", err.Error())

	b = NewChannelUriStringBuilder().CopyFrom(MustParseChannelUri("aeron:ipc?linger=7200s"), LingerParamName)
	v, _ = b.GetLinger()
	assert.Equal(t, 2*time.Hour, v)

	b = NewChannelUriStringBuilder().CopyFrom(MustParseChannelUri("aeron:udp?linger=19191919191919191"), LingerParamName)
	v, _ = b.GetLinger()
	assert.Equal(t, time.Duration(19191919191919191), v)

	b = NewChannelUriStringBuilder().Linger(1).
		CopyFrom(MustParseChannelUri("aeron:udp?endpoint=localhost:8080"), LingerParamName)
	_, ok = b.GetLinger()
	assert.False(t, ok)

	b = NewChannelUriStringBuilder().CopyFrom(MustParseChannelUri("aeron:udp?linger=-1000"), LingerParamName)
	assert.Error(t, b.Err())
}

func TestChannelUriStringBuilderFromStringWithAllTheFields(t *testing.T) {
	uri := "aeron-spy:aeron:udp?endpoint=127.0.0.1:0|interface=127.0.0.1|control=127.0.0.2:0|" +
		"control-mode=manual|tags=2,4|alias=foo|cc=cubic|fc=min|reliable=false|ttl=16|mtu=8992|" +
		"term-length=1m|init-term-id=5|term-offset=64|term-id=4353|session-id=2314234|gtag=3|" +
		"linger=100000055000001ns|sparse=true|eos=true|tether=false|group=false|ssc=true|so-sndbuf=8m|" +
		"so-rcvbuf=2m|rcv-wnd=1m|media-rcv-ts-offset=reserved|channel-rcv-ts-offset=0|" +
		"channel-snd-ts-offset=8|response-endpoint=127.0.0.3:0|response-correlation-id=12345|nak-delay=100us|" +
		"untethered-window-limit-timeout=1us|untethered-resting-timeout=5us|stream-id=87|pub-wnd=10224|" +
		"untethered-linger-timeout=7ms|max-resend=9|so-tos=4"
	fromString := MustParseChannelUri(uri)
	b, err := NewChannelUriStringBuilderFromString(uri)
	require.NoError(t, err)
	fromBuilder := MustParseChannelUri(b.MustBuild())
	assert.Empty(t, fromString.diff(fromBuilder))
}

func TestChannelUriStringBuilderFromStringWithTaggedSessionIdAndIpc(t *testing.T) {
	uri := "aeron:ipc?session-id=tag:123456"
	b, err := NewChannelUriStringBuilderFromString(uri)
	require.NoError(t, err)
	assert.True(t, b.GetIsSessionIDTagged())
	id, _ := b.GetSessionID()
	assert.Equal(t, int64(123456), id)
	assert.Empty(t, MustParseChannelUri(uri).diff(MustParseChannelUri(b.MustBuild())))
}

func TestChannelUriStringBuilderFromStringErrors(t *testing.T) {
	for _, uri := range []string{
		"aeron:ipc?ttl=abc", "aeron:ipc?ttl=256", "aeron:ipc?mtu=33", "aeron:ipc?mtu=8g", "aeron:ipc?mtu=16",
		"aeron:ipc?term-length=1000", "aeron:ipc?term-length=4g", "aeron:ipc?init-term-id=x", "aeron:ipc?term-id=x",
		"aeron:ipc?term-offset=33", "aeron:ipc?term-offset=-32", "aeron:ipc?session-id=x",
		"aeron:ipc?session-id=tag:x", "aeron:ipc?gtag=x", "aeron:ipc?so-sndbuf=-1", "aeron:ipc?so-rcvbuf=3g",
		"aeron:ipc?rcv-wnd=1x", "aeron:ipc?pub-wnd=abc", "aeron:ipc?pub-wnd=1000000000000",
		"aeron:ipc?nak-delay=foo", "aeron:ipc?untethered-window-limit-timeout=1x",
		"aeron:ipc?untethered-linger-timeout=-1", "aeron:ipc?untethered-resting-timeout=s",
		"aeron:ipc?max-resend=-1", "aeron:ipc?stream-id=abc", "aeron:ipc?control-mode=bogus",
		"aeron:ipc?media-rcv-ts-offset=breserved", "aeron:ipc?channel-rcv-ts-offset=breserved",
		"aeron:ipc?channel-snd-ts-offset=breserved", "aeron:ipc?response-correlation-id=this.will.not.work",
		"aeron:ipc?response-correlation-id=-2", "aeron:ipc?so-tos=256",
	} {
		_, err := NewChannelUriStringBuilderFromString(uri)
		assert.Error(t, err, uri)
	}
	for _, uri := range []string{
		"aeron:udp?response-correlation-id=prototype", "aeron:udp?response-correlation-id=2",
		"aeron:udp?response-correlation-id=-1", "aeron:ipc?unknown=1",
	} {
		_, err := NewChannelUriStringBuilderFromString(uri)
		assert.NoError(t, err, uri)
	}
}

func TestChannelUriStringBuilderSetterErrors(t *testing.T) {
	for name, b := range map[string]*ChannelUriStringBuilder{
		"prefix":       NewChannelUriStringBuilder().Prefix("aeron-foo"),
		"media":        NewChannelUriStringBuilder().Media("tcp"),
		"controlMode":  NewChannelUriStringBuilder().ControlMode("auto"),
		"ttl":          NewChannelUriStringBuilder().Ttl(-1),
		"mtu":          NewChannelUriStringBuilder().Mtu(65536),
		"mtuAlign":     NewChannelUriStringBuilder().Mtu(1000),
		"termLength":   NewChannelUriStringBuilder().TermLength(3 * 64 * 1024),
		"termOffset":   NewChannelUriStringBuilder().TermOffset(31),
		"socketTos":    NewChannelUriStringBuilder().SocketTos(-1),
		"sndbuf":       NewChannelUriStringBuilder().SocketSndbufLength(-1),
		"rcvbuf":       NewChannelUriStringBuilder().SocketRcvbufLength(-1),
		"rcvWnd":       NewChannelUriStringBuilder().ReceiverWindowLength(-1),
		"pubWnd":       NewChannelUriStringBuilder().PublicationWindowLength(-1),
		"maxResend":    NewChannelUriStringBuilder().MaxResend(-1),
		"nakDelay":     NewChannelUriStringBuilder().NakDelay(-1),
		"untethered1":  NewChannelUriStringBuilder().UntetheredWindowLimitTimeout(-1),
		"untethered2":  NewChannelUriStringBuilder().UntetheredLingerTimeout(-1),
		"untethered3":  NewChannelUriStringBuilder().UntetheredRestingTimeout(-1),
		"mediaRcvTs":   NewChannelUriStringBuilder().MediaReceiveTimestampOffset("breserved"),
		"channelRcvTs": NewChannelUriStringBuilder().ChannelReceiveTimestampOffset("breserved"),
		"channelSndTs": NewChannelUriStringBuilder().ChannelSendTimestampOffset("breserved"),
		"responseCorr": NewChannelUriStringBuilder().ResponseCorrelationID(-2),
		"position":     NewChannelUriStringBuilder().InitialPosition(-1, 0, 64*1024),
		"positionAlgn": NewChannelUriStringBuilder().InitialPosition(31, 0, 64*1024),
		"positionTerm": NewChannelUriStringBuilder().InitialPosition(0, 0, 100),
		"unknownParam": NewChannelUriStringBuilder().SetParam("bogus", "1"),
		"unknownUnset": NewChannelUriStringBuilder().Unset("bogus"),
	} {
		b.Media("ipc")
		assert.Error(t, b.Err(), name)
		_, err := b.Build()
		assert.Error(t, err, name)
		assert.Error(t, b.Validate(), name)
	}
	// first error wins and the field is unchanged
	b := NewChannelUriStringBuilder().Ttl(3).Ttl(300).Ttl(400)
	assert.Contains(t, b.Err().Error(), "300")
	ttl, _ := b.GetTtl()
	assert.Equal(t, int32(3), ttl)
	assert.NoError(t, b.Clear().Err())
}

func TestChannelUriStringBuilderDurationsWithUnits(t *testing.T) {
	for _, tc := range []struct {
		value    string
		expected time.Duration
	}{{"1us", time.Microsecond}, {"1ns", 1}, {"1ms", time.Millisecond}, {"2s", 2 * time.Second}, {"7", 7}} {
		for _, name := range []string{NakDelayParamName, UntetheredWindowLimitTimeoutParamName,
			UntetheredLingerTimeoutParamName, UntetheredRestingTimeoutParamName, LingerParamName} {
			b := NewChannelUriStringBuilder().SetParam(name, tc.value)
			require.NoError(t, b.Err())
			got, ok := (*b.durationField(name)), true
			require.True(t, ok)
			assert.Equal(t, int64(tc.expected), *got, "%s=%s", name, tc.value)
		}
	}
	v, _ := NewChannelUriStringBuilder().SetParam(NakDelayParamName, "1us").GetNakDelay()
	assert.Equal(t, time.Microsecond, v)
	assert.Error(t, NewChannelUriStringBuilder().SetParam(NakDelayParamName, "foo").Err())
}

func TestChannelUriStringBuilderMaxResendStreamIdPubWnd(t *testing.T) {
	b := NewChannelUriStringBuilder().MaxResend(20)
	v, _ := b.GetMaxResend()
	assert.Equal(t, int32(20), v)
	assert.True(t, strings.Contains(b.String(), MaxResendParamName+"=20"))
	b = NewChannelUriStringBuilder().CopyFrom(
		MustParseChannelUri(NewChannelUriStringBuilder().Media("ipc").MaxResend(30).MustBuild()), MaxResendParamName)
	v, _ = b.GetMaxResend()
	assert.Equal(t, int32(30), v)

	_, ok := NewChannelUriStringBuilder().GetStreamID()
	assert.False(t, ok)
	s := NewChannelUriStringBuilder().Media("ipc").StreamID(1234).MustBuild()
	assert.Equal(t, "1234", MustParseChannelUri(s).Get(StreamIdParamName))

	_, ok = NewChannelUriStringBuilder().GetPublicationWindowLength()
	assert.False(t, ok)
	s = NewChannelUriStringBuilder().Media("ipc").PublicationWindowLength(7777).MustBuild()
	assert.Equal(t, "7777", MustParseChannelUri(s).Get(PublicationWindowLengthParamName))
}

func TestChannelUriStringBuilderUntetheredParameters(t *testing.T) {
	for _, tc := range [][3]int64{{1000, 666, 2002}, {50, 40, 30}} {
		b, err := NewChannelUriStringBuilderFromString("aeron:ipc")
		require.NoError(t, err)
		b.UntetheredWindowLimitTimeout(time.Duration(tc[0])).UntetheredLingerTimeout(time.Duration(tc[1])).
			UntetheredRestingTimeout(time.Duration(tc[2]))
		a, _ := b.GetUntetheredWindowLimitTimeout()
		l, _ := b.GetUntetheredLingerTimeout()
		r, _ := b.GetUntetheredRestingTimeout()
		assert.Equal(t, [3]int64{int64(a), int64(l), int64(r)}, tc)
		uri := MustParseChannelUri(b.MustBuild())
		assert.Equal(t, uriFormatDuration(tc[0]), uri.Get(UntetheredWindowLimitTimeoutParamName))
		assert.Equal(t, uriFormatDuration(tc[1]), uri.Get(UntetheredLingerTimeoutParamName))
		assert.Equal(t, uriFormatDuration(tc[2]), uri.Get(UntetheredRestingTimeoutParamName))
	}
}

func TestChannelUriStringBuilderShouldFormatSizeAndDurations(t *testing.T) {
	channel := NewChannelUriStringBuilder().
		Media("udp").
		Endpoint("localhost:5050").
		ReceiverWindowLength(1024).
		Mtu(8192).
		TermLength(4*1024*1024).
		SocketSndbufLength(64*1024).
		SocketRcvbufLength(32*1024).
		PublicationWindowLength(1024*1024).
		UntetheredWindowLimitTimeout(100*time.Microsecond).
		UntetheredLingerTimeout(3*time.Millisecond).
		UntetheredRestingTimeout(time.Second).
		Linger(50*time.Millisecond).
		NakDelay(123456789).
		MaxResend(1000).
		Tether(true).
		Rejoin(false).
		StreamID(-87).
		InitialPosition(17*1024*1024, -9, 4*1024*1024).
		MustBuild()
	expected := "aeron:udp?endpoint=localhost:5050|mtu=8k|term-length=4m|rcv-wnd=1k|so-sndbuf=64k|" +
		"so-rcvbuf=32k|pub-wnd=1m|untethered-linger-timeout=3ms|untethered-window-limit-timeout=100us|" +
		"untethered-resting-timeout=1s|linger=50ms|nak-delay=123456789ns|max-resend=1000|rejoin=false|" +
		"tether=true|stream-id=-87|term-id=-5|init-term-id=-9|term-offset=1048576"
	assert.Empty(t, MustParseChannelUri(channel).diff(MustParseChannelUri(expected)))
}

func TestChannelUriStringBuilderUnsetAll(t *testing.T) {
	uri := "aeron-spy:aeron:udp?endpoint=127.0.0.1:0|interface=127.0.0.1|control=127.0.0.2:0|" +
		"control-mode=manual|tags=2,4|alias=foo|cc=cubic|fc=min|reliable=false|ttl=16|mtu=8992|" +
		"term-length=1m|init-term-id=5|term-offset=64|term-id=4353|session-id=tag:2314234|gtag=3|" +
		"linger=1ns|sparse=true|eos=true|tether=false|group=false|rejoin=true|ssc=true|so-sndbuf=8m|" +
		"so-rcvbuf=2m|so-tos=1|rcv-wnd=1m|media-rcv-ts-offset=reserved|channel-rcv-ts-offset=0|" +
		"channel-snd-ts-offset=8|response-endpoint=127.0.0.3:0|response-correlation-id=12345|nak-delay=100us|" +
		"untethered-window-limit-timeout=1us|untethered-linger-timeout=1us|untethered-resting-timeout=5us|" +
		"max-resend=3|stream-id=87|pub-wnd=10224"
	b, err := NewChannelUriStringBuilderFromString(uri)
	require.NoError(t, err)
	for _, name := range channelUriBuilderParams {
		b.Unset(name)
	}
	require.NoError(t, b.Err())
	assert.Equal(t, "aeron-spy:aeron:udp", b.String())
	assert.False(t, b.GetIsSessionIDTagged())
}

func TestUriSizeAndDurationHelpers(t *testing.T) {
	for _, tc := range []struct {
		in  string
		out int64
	}{{"0", 0}, {"1", 1}, {"8k", 8192}, {"8K", 8192}, {"2m", 2 << 20}, {"1g", 1 << 30}, {"1G", 1 << 30}} {
		v, err := uriParseSize("x", tc.in)
		require.NoError(t, err, tc.in)
		assert.Equal(t, tc.out, v, tc.in)
	}
	for _, s := range []string{"", "-1", "k", "1x", "+1", "9223372036854775807g"} {
		_, err := uriParseSize("x", s)
		assert.Error(t, err, s)
	}
	for _, tc := range []struct {
		in  string
		out int64
	}{{"5", 5}, {"5ns", 5}, {"5NS", 5}, {"5us", 5000}, {"5ms", 5000000}, {"5s", 5000000000},
		{"9223372036854775807s", 9223372036854775807}} {
		v, err := uriParseDuration("x", tc.in)
		require.NoError(t, err, tc.in)
		assert.Equal(t, tc.out, v, tc.in)
	}
	for _, s := range []string{"", "-1s", "s", "ms", "5x", "5xs", "5h"} {
		_, err := uriParseDuration("x", s)
		assert.Error(t, err, s)
	}
	assert.Equal(t, "1g", uriFormatSize(1<<30))
	assert.Equal(t, "1025k", uriFormatSize(1025*1024))
	assert.Equal(t, "1000", uriFormatSize(1000))
	assert.Equal(t, "0", uriFormatSize(0))
	assert.Equal(t, "1s", uriFormatDuration(1e9))
	assert.Equal(t, "1001ms", uriFormatDuration(1001e6))
	assert.Equal(t, "1us", uriFormatDuration(1000))
	assert.Equal(t, "0ns", uriFormatDuration(0))
	assert.Equal(t, "1001ns", uriFormatDuration(1001))
}

func TestChannelUriStringBuilderEmptyStringsAreSetNotUnset(t *testing.T) {
	b := NewChannelUriStringBuilder().Media(UdpMedia).Endpoint("").Tags("").Alias("").NetworkInterface("").
		CongestionControl("").FlowControl("").ResponseEndpoint("")
	require.NoError(t, b.Validate())
	assert.Equal(t, "aeron:udp?tags=|endpoint=|interface=|alias=|cc=|fc=|response-endpoint=", b.MustBuild())
	b.Unset(EndpointParamName).Unset(TagsParamName).Unset(InterfaceParamName).Unset(AliasParamName).
		Unset(CongestionControlParamName).Unset(FlowControlParamName).Unset(ResponseEndpointParamName)
	assert.Equal(t, "aeron:udp", b.MustBuild())
	assert.Error(t, b.Validate())

	for name, b := range map[string]*ChannelUriStringBuilder{
		"controlMode":  NewChannelUriStringBuilder().ControlMode(""),
		"mediaRcvTs":   NewChannelUriStringBuilder().MediaReceiveTimestampOffset(""),
		"channelRcvTs": NewChannelUriStringBuilder().ChannelReceiveTimestampOffset(""),
		"channelSndTs": NewChannelUriStringBuilder().ChannelSendTimestampOffset(""),
		"responseCorr": NewChannelUriStringBuilder().ResponseCorrelationIdString(""),
	} {
		assert.Error(t, b.Err(), name)
	}
}
