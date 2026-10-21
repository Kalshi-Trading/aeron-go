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
	"strconv"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Port of io.aeron.ChannelUriTest (1.53.3).

func TestChannelUriShouldParseSimpleDefaultUri(t *testing.T) {
	for _, tc := range []struct{ uri, prefix, media string }{
		{"aeron:udp", "", "udp"},
		{"aeron:ipc", "", "ipc"},
		{"aeron-spy:aeron:ipc", "aeron-spy", "ipc"},
	} {
		uri, err := ParseChannelUri(tc.uri)
		require.NoError(t, err, tc.uri)
		assert.Equal(t, "aeron", uri.Scheme())
		assert.Equal(t, tc.prefix, uri.Prefix())
		assert.Equal(t, tc.media, uri.Media())
	}
}

func TestChannelUriShouldRejectInvalid(t *testing.T) {
	for _, s := range []string{
		":udp", "aeron", "aron:", "eeron:", // without aeron prefix
		"aeron:",                 // empty media
		"aeron:udp:",             // out of place colon
		"aeron:ipcsdfgfdhfgf",    // invalid media
		"aeron:ipc|sparse=true",  // missing query separator
		"aeron:ipc=x",            // '=' in media
		"aeron:udp?",             // ends in key state
		"aeron:udp?=x",           // empty key
		"aeron:udp?endpoint|x=y", // invalid end of key
		"aeron:udp?endpoint=localhost:4652|-~@{]|=??#s!£$%====", // invalid params
	} {
		_, err := ParseChannelUri(s)
		assert.Error(t, err, s)
	}
}

func TestChannelUriShouldRejectUriIfLengthExceedsMaxAllowed(t *testing.T) {
	for _, length := range []int{MaxUriLength + 1, 10000} {
		uri := "aeron:ipc?alias=" + strings.Repeat("x", length-len("aeron:ipc?alias="))
		_, err := ParseChannelUri(uri)
		require.Error(t, err)
		assert.Equal(t, "URI length ("+strconv.Itoa(len(uri))+") exceeds max supported length (4095): "+uri[:MaxUriLength],
			err.Error())
	}
	_, err := ParseChannelUri("aeron:ipc?alias=" + strings.Repeat("x", MaxUriLength-len("aeron:ipc?alias=")))
	assert.NoError(t, err)
}

func TestChannelUriShouldParseWithMultipleParameters(t *testing.T) {
	uri := MustParseChannelUri("aeron:udp?endpoint=224.10.9.8|port=4567|interface=192.168.0.3|ttl=16")
	assert.Equal(t, "224.10.9.8", uri.Get("endpoint"))
	assert.Equal(t, "4567", uri.Get("port"))
	assert.Equal(t, "192.168.0.3", uri.Get("interface"))
	assert.Equal(t, "16", uri.Get("ttl"))
}

func TestChannelUriLastDuplicateWins(t *testing.T) {
	uri := MustParseChannelUri("aeron:udp?endpoint=a:1|ttl=4|endpoint=b:2")
	assert.Equal(t, "b:2", uri.Get("endpoint"))
	assert.Equal(t, "aeron:udp?endpoint=b:2|ttl=4", uri.String())
}

func TestChannelUriShouldAllowReturnDefaultIfParamNotSpecified(t *testing.T) {
	uri := MustParseChannelUri("aeron:udp?endpoint=224.10.9.8")
	v, ok := uri.Lookup("interface")
	assert.False(t, ok)
	assert.Equal(t, "", v)
	assert.Equal(t, "192.168.0.0", uri.GetOrDefault("interface", "192.168.0.0"))
	assert.True(t, uri.ContainsKey("endpoint"))
}

func TestChannelUriShouldRoundTripToString(t *testing.T) {
	for _, s := range []string{
		"aeron:udp?endpoint=224.10.9.8:777",
		"aeron-spy:aeron:udp?control=224.10.9.8:777",
		"aeron:udp?ttl=16|endpoint=224.10.9.8|interface=199.168.0.3|port=4567",
		"aeron:ipc",
		"aeron:udp?endpoint=localhost:0|tags=1,2|session-id=tag:7",
	} {
		uri := MustParseChannelUri(s)
		assert.Equal(t, s, uri.String())
		assert.Equal(t, s, uri.Clone().String())
	}
}

func TestChannelUriShouldRoundTripToStringBuilder(t *testing.T) {
	for _, prefix := range []string{"", SpyQualifier} {
		s := NewChannelUriStringBuilder().Prefix(prefix).Media("udp").Endpoint("224.10.9.8:777").MustBuild()
		assert.Equal(t, s, MustParseChannelUri(s).String())
	}
}

func TestChannelUriCloneIsDeep(t *testing.T) {
	uri := MustParseChannelUri("aeron:udp?endpoint=a:1")
	c := uri.Clone()
	c.Put("ttl", "3")
	c.Put("endpoint", "b:2")
	assert.Equal(t, "aeron:udp?endpoint=a:1", uri.String())
	assert.Equal(t, "aeron:udp?endpoint=b:2|ttl=3", c.String())
}

func TestChannelUriPutRemove(t *testing.T) {
	uri := MustParseChannelUri("aeron:udp?endpoint=a:1|ttl=3|mtu=8k")
	uri.Remove("ttl")
	assert.False(t, uri.ContainsKey("ttl"))
	uri.Remove("ttl")
	assert.Equal(t, "a:1", uri.Put("endpoint", "b:2"))
	assert.Equal(t, "", uri.Put("alias", "x"))
	assert.Equal(t, "aeron:udp?endpoint=b:2|mtu=8k|alias=x", uri.String())
	uri.SetMedia("ipc")
	uri.SetPrefix(SpyQualifier)
	assert.Equal(t, "aeron-spy:aeron:ipc?endpoint=b:2|mtu=8k|alias=x", uri.String())
}

func TestChannelUriValueCopiesShareParams(t *testing.T) {
	uri := MustParseChannelUri("aeron:udp?endpoint=a:1|ttl=3")
	cp := uri
	cp.Set("mtu", "8k")
	cp.Remove("ttl")
	assert.Equal(t, "aeron:udp?endpoint=a:1|mtu=8k", uri.String())
	cp.SetMedia("ipc")
	assert.Equal(t, "udp", uri.Media())

	var zero ChannelUri
	assert.Equal(t, "", zero.Get("x"))
	zero.Remove("x")
	assert.Panics(t, func() { zero.Set("x", "y") })
	zero.Put("x", "y")
	assert.Equal(t, "y", zero.Get("x"))
}

func TestChannelUriEquality(t *testing.T) {
	for _, tc := range []struct {
		a, b     string
		expected bool
	}{
		{"aeron:udp?endpoint=224.10.9.8", "aeron:udp?endpoint=224.10.9.8", true},
		{"aeron:udp?endpoint=224.10.9.8|port=4567|interface=192.168.0.3|ttl=16",
			"aeron:udp?port=4567|endpoint=224.10.9.8|ttl=16|interface=192.168.0.3", true},
		{"aeron:udp?endpoint=224.10.9.8|tags=1,2", "aeron:udp?tags=1,2|endpoint=224.10.9.8", true},
		{"aeron:udp?endpoint=224.10.9.8", "aeron-spy:aeron:udp?endpoint=224.10.9.8", false},
		{"aeron:udp?endpoint=224.10.9.8", "aeron:ipc?endpoint=224.10.9.8", false},
		{"aeron:udp?endpoint=224.10.9.9", "aeron:udp?endpoint=224.10.8.8", false},
		{"aeron:udp?endpoint=224.10.9.8|ttl=16", "aeron:ipc?endpoint=224.10.9.8|port=4567", false},
		{"aeron:udp?endpoint=224.10.9.8|tags=2,1", "aeron:udp?endpoint=224.10.9.8|tags=1,2", false},
	} {
		a, b := MustParseChannelUri(tc.a), MustParseChannelUri(tc.b)
		assert.Equal(t, tc.expected, a.Equals(b), "%s vs %s", tc.a, tc.b)
		assert.Equal(t, tc.expected, len(a.diff(b)) == 0, "%s vs %s", tc.a, tc.b)
	}
	uri := MustParseChannelUri("aeron:ipc")
	assert.True(t, uri.Equals(uri))
	assert.False(t, uri.Equals(ChannelUri{}))
}

func TestChannelUriCreateDestinationUri(t *testing.T) {
	for _, tc := range []struct{ channel, endpoint, expected string }{
		{"aeron:udp?endpoint=poison|interface=iface|mtu=4444", "dest1", "aeron:udp?endpoint=dest1|interface=iface"},
		{"aeron:ipc", "dest2", "aeron:ipc?endpoint=dest2"},
		{"aeron:udp", "localhost", "aeron:udp?endpoint=localhost"},
		{"aeron:ipc?interface=here", "there", "aeron:ipc?endpoint=there|interface=here"},
		{"aeron-spy:aeron:udp?eol=true|interface=none", "abc", "aeron:udp?endpoint=abc|interface=none"},
		{"aeron:udp?interface=eth0|term-length=64k|ttl=0|endpoint=some", "vm1", "aeron:udp?endpoint=vm1|interface=eth0"},
	} {
		got, err := CreateDestinationUri(tc.channel, tc.endpoint)
		require.NoError(t, err)
		assert.Equal(t, tc.expected, got)
	}
	_, err := CreateDestinationUri("bogus", "x")
	assert.Error(t, err)
}

func TestChannelUriShouldSubstituteEndpoint(t *testing.T) {
	for _, tc := range []struct{ expected, original, resolved string }{
		{"aeron:udp?endpoint=localhost:12345", "aeron:udp?endpoint=localhost:0", "localhost:12345"},
		{"aeron:udp?endpoint=localhost:12345", "aeron:udp?endpoint=localhost:12345", "localhost:54321"},
		{"aeron:udp?endpoint=localhost:12345", "aeron:udp?endpoint=localhost:0", "127.0.0.1:12345"},
		{"aeron:udp?endpoint=127.0.0.1:12345", "aeron:udp", "127.0.0.1:12345"},
	} {
		uri := MustParseChannelUri(tc.original)
		require.NoError(t, uri.ReplaceEndpointWildcardPort(tc.resolved))
		assert.Equal(t, tc.expected, uri.String())
	}
}

func TestChannelUriShouldErrorIfResolvedEndpointInvalid(t *testing.T) {
	uri := MustParseChannelUri("aeron:udp?endpoint=localhost:0")
	assert.Error(t, uri.ReplaceEndpointWildcardPort("localhost:0"))
	assert.Error(t, uri.ReplaceEndpointWildcardPort("localhost"))
	assert.Error(t, uri.ReplaceEndpointWildcardPort(""))
}

func TestChannelUriShouldIterateOverParameters(t *testing.T) {
	uri := MustParseChannelUri("aeron:udp?endpoint=myhost:0|ttl=1|mtu=8k|term-length=64M|alias=text|x=42")
	var got [][2]string
	uri.ForEachParameter(func(k, v string) { got = append(got, [2]string{k, v}) })
	assert.Equal(t, [][2]string{
		{"endpoint", "myhost:0"}, {"ttl", "1"}, {"mtu", "8k"}, {"term-length", "64M"}, {"alias", "text"}, {"x", "42"},
	}, got)

	called := false
	MustParseChannelUri("aeron:ipc").ForEachParameter(func(string, string) { called = true })
	assert.False(t, called)
}

func TestChannelUriAddAliasIfAbsent(t *testing.T) {
	for _, alias := range []string{""} {
		got, err := AddAliasIfAbsent("aeron:udp", alias)
		require.NoError(t, err)
		assert.Equal(t, "aeron:udp", got)
	}
	got, err := AddAliasIfAbsent("aeron:udp?alias=xyz|term-length=64k", "new alias")
	require.NoError(t, err)
	assert.Equal(t, "aeron:udp?alias=xyz|term-length=64k", got)

	for _, s := range []string{"aeron:udp?term-length=64k|ssc=false|mtu=8k", "aeron:ipc", "aeron:udp?custom=alias"} {
		expected := MustParseChannelUri(s)
		expected.Put(AliasParamName, "my alias")
		got, err := AddAliasIfAbsent(s, "my alias")
		require.NoError(t, err)
		assert.True(t, expected.Equals(MustParseChannelUri(got)), got)
	}
}

func TestChannelUriTransformAlias(t *testing.T) {
	identity := func(a string) string { return a }
	empty := func(string) string { return "" }
	fun := func(a string) string {
		if a == "" {
			return "default"
		}
		return a + "bar"
	}
	for _, tc := range []struct {
		uri      string
		fn       func(string) string
		expected string
	}{
		{"aeron:ipc", identity, "aeron:ipc"},
		{"aeron:ipc?alias=foo", identity, "aeron:ipc?alias=foo"},
		{"aeron:ipc?alias=foo", empty, "aeron:ipc"},
		{"aeron:ipc", fun, "aeron:ipc?alias=default"},
		{"aeron:ipc?alias=foo", fun, "aeron:ipc?alias=foobar"},
	} {
		got, err := TransformAlias(tc.uri, tc.fn)
		require.NoError(t, err)
		assert.Equal(t, tc.expected, got)
	}
}

func TestChannelUriTags(t *testing.T) {
	uri := MustParseChannelUri("aeron:udp?tags=1001,1002|endpoint=localhost:0")
	assert.Equal(t, "1001", uri.ChannelTag())
	assert.Equal(t, "1002", uri.EntityTag())
	uri = MustParseChannelUri("aeron:udp?tags=1001")
	assert.Equal(t, "1001", uri.ChannelTag())
	assert.Equal(t, "", uri.EntityTag())
	uri = MustParseChannelUri("aeron:udp")
	assert.Equal(t, "", uri.ChannelTag())
	assert.Equal(t, "", uri.EntityTag())

	assert.True(t, IsTagged("tag:123"))
	assert.False(t, IsTagged("123"))
	assert.False(t, IsTagged("tag"))
	tag, err := GetTag("tag:123")
	require.NoError(t, err)
	assert.Equal(t, int64(123), tag)
	tag, err = GetTag("123")
	require.NoError(t, err)
	assert.Equal(t, InvalidTag, tag)
	_, err = GetTag("tag:abc")
	assert.Error(t, err)
}

func TestChannelUriAddSessionId(t *testing.T) {
	got, err := AddSessionID("aeron:udp?endpoint=localhost:1", -5)
	require.NoError(t, err)
	assert.Equal(t, "aeron:udp?endpoint=localhost:1|session-id=-5", got)
	got, err = AddSessionID("aeron:udp?session-id=1|endpoint=localhost:1", 7)
	require.NoError(t, err)
	assert.Equal(t, "aeron:udp?session-id=7|endpoint=localhost:1", got)
	_, err = AddSessionID("udp", 7)
	assert.Error(t, err)
}

func TestChannelUriControlModeResponse(t *testing.T) {
	ok, err := IsControlModeResponse("aeron:udp?control-mode=response|control=localhost:10000")
	require.NoError(t, err)
	assert.True(t, ok)
	ok, err = IsControlModeResponse("aeron:udp?control-mode=dynamic|control=localhost:10000")
	require.NoError(t, err)
	assert.False(t, ok)
}

func TestChannelUriReplacePortWithWildcard(t *testing.T) {
	assert.Equal(t, "localhost:0", ReplacePortWithWildcard("localhost:1234"))
	assert.Equal(t, "[::1]:0", ReplacePortWithWildcard("[::1]:1234"))
	assert.Equal(t, "", ReplacePortWithWildcard("localhost"))
	assert.Equal(t, "", ReplacePortWithWildcard(""))
}

func TestChannelUriInitialPosition(t *testing.T) {
	uri := MustParseChannelUri("aeron:ipc")
	require.NoError(t, uri.InitialPosition(17*1024*1024, -9, 4*1024*1024))
	assert.Equal(t, "aeron:ipc?init-term-id=-9|term-id=-5|term-offset=1048576|term-length=4194304", uri.String())
	assert.Error(t, uri.InitialPosition(-32, 0, 64*1024))
	assert.Error(t, uri.InitialPosition(33, 0, 64*1024))
	assert.Error(t, uri.InitialPosition(0, 0, 1000))
}

func TestValidateSocketTos(t *testing.T) {
	v, err := ValidateSocketTos("16")
	require.NoError(t, err)
	assert.Equal(t, int32(16), v)
	for _, s := range []string{"-1", "256", "abc"} {
		_, err := ValidateSocketTos(s)
		assert.Error(t, err, s)
	}
}

func TestChannelUriShouldOnlyValidateMediaWithoutQuery(t *testing.T) {
	for media, s := range map[string]string{
		"foo": "aeron:foo?endpoint=localhost:1",
		"":    "aeron:?endpoint=localhost:1",
		"UDP": "aeron:UDP?endpoint=localhost:1",
	} {
		uri, err := ParseChannelUri(s)
		require.NoError(t, err, s)
		assert.Equal(t, media, uri.Media(), s)
		assert.Equal(t, s, uri.String())
		_, err = NewChannelUriStringBuilderFromUri(uri)
		assert.Error(t, err, s)
	}
}

func TestChannelUriMaxLengthIsInUtf16CodeUnits(t *testing.T) {
	const prefix = "aeron:ipc?alias="
	atMax := prefix + strings.Repeat("é", MaxUriLength-len(prefix))
	require.Greater(t, len(atMax), MaxUriLength)
	_, err := ParseChannelUri(atMax)
	assert.NoError(t, err)

	tooLong := prefix + strings.Repeat("😀", MaxUriLength)
	_, err = ParseChannelUri(tooLong)
	require.Error(t, err)
	assert.True(t, utf8.ValidString(err.Error()))
	want := prefix + strings.Repeat("😀", (MaxUriLength-len(prefix))/2)
	assert.Equal(t, "URI length ("+strconv.Itoa(len(prefix)+2*MaxUriLength)+") exceeds max supported length (4095): "+
		want, err.Error())
}
