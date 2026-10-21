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
	"fmt"
	"strconv"
	"strings"

	"github.com/lirm/aeron-go/aeron/logbuffer"
)

// Channel URI schemes, media and parameter names. These mirror the constants in io.aeron.ChannelUri and
// io.aeron.CommonContext (Aeron 1.53.3, plus so-tos from 1.54).
const (
	AeronScheme  = "aeron"
	SpyQualifier = "aeron-spy"
	AeronPrefix  = AeronScheme + ":"
	SpyPrefix    = SpyQualifier + ":"

	// InvalidTag is returned by GetTag when the value is not tagged.
	InvalidTag int64 = NullValue
	// MaxUriLength is the maximum length in characters of a channel URI.
	MaxUriLength = 4095

	IpcMedia   = "ipc"
	UdpMedia   = "udp"
	IpcChannel = "aeron:ipc"

	EndpointParamName                  = "endpoint"
	InterfaceParamName                 = "interface"
	InitialTermIdParamName             = "init-term-id"
	TermIdParamName                    = "term-id"
	TermOffsetParamName                = "term-offset"
	TermLengthParamName                = "term-length"
	MtuLengthParamName                 = "mtu"
	TtlParamName                       = "ttl"
	MdcControlParamName                = "control"
	MdcControlModeParamName            = "control-mode"
	MdcControlModeManual               = "manual"
	MdcControlModeDynamic              = "dynamic"
	SessionIdParamName                 = "session-id"
	LingerParamName                    = "linger"
	ReliableStreamParamName            = "reliable"
	TagsParamName                      = "tags"
	TagPrefix                          = "tag:"
	SparseParamName                    = "sparse"
	AliasParamName                     = "alias"
	EosParamName                       = "eos"
	TetherParamName                    = "tether"
	GroupParamName                     = "group"
	RejoinParamName                    = "rejoin"
	CongestionControlParamName         = "cc"
	FlowControlParamName               = "fc"
	GroupTagParamName                  = "gtag"
	SpiesSimulateConnectionParamName   = "ssc"
	SocketSndbufParamName              = "so-sndbuf"
	SocketRcvbufParamName              = "so-rcvbuf"
	SocketTosParamName                 = "so-tos"
	ReceiverWindowLengthParamName      = "rcv-wnd"
	MediaRcvTimestampOffsetParamName   = "media-rcv-ts-offset"
	ChannelRcvTimestampOffsetParamName = "channel-rcv-ts-offset"
	ChannelSndTimestampOffsetParamName = "channel-snd-ts-offset"

	// Deprecated: use ChannelRcvTimestampOffsetParamName.
	ChannelReceiveTimestampOffsetParamName = ChannelRcvTimestampOffsetParamName
	// Deprecated: use ChannelSndTimestampOffsetParamName.
	ChannelSendTimestampOffsetParamName = ChannelSndTimestampOffsetParamName
)

// ChannelUri is a parser for Aeron channel URIs. The format is:
//
//	aeron-uri = "aeron:" media [ "?" param *( "|" param ) ]
//	media     = *( "[^?:]" )
//	param     = key "=" value
//	key       = *( "[^=]" )
//	value     = *( "[^|]" )
//
// Multiple params with the same key are allowed, the last value specified takes precedence. Parameters are kept in
// the order they were first inserted so that String() round-trips a parsed URI.
type ChannelUri struct {
	prefix string
	media  string
	keys   []string
	params map[string]string
}

type parseState int8

const (
	parseStateMedia parseState = iota + 1
	parseStateParamsKey
	parseStateParamsValue
)

// NewChannelUri creates an empty ChannelUri for the given prefix (may be empty) and media.
func NewChannelUri(prefix string, media string) (*ChannelUri, error) {
	if err := validateMedia(media); err != nil {
		return nil, err
	}
	return &ChannelUri{prefix: prefix, media: media, params: make(map[string]string)}, nil
}

// ParseChannelUri parses a string which contains an Aeron URI.
func ParseChannelUri(uriStr string) (*ChannelUri, error) {
	length := len(uriStr)
	if length > MaxUriLength {
		return nil, fmt.Errorf("URI length (%d) exceeds max supported length (%d): %s",
			length, MaxUriLength, uriStr[:MaxUriLength])
	}

	uri := &ChannelUri{params: make(map[string]string)}
	position := 0
	if strings.HasPrefix(uriStr, SpyPrefix) {
		uri.prefix = SpyQualifier
		position = len(SpyPrefix)
	}
	if !strings.HasPrefix(uriStr[position:], AeronPrefix) {
		return nil, fmt.Errorf("Aeron URIs must start with 'aeron:', found: %s", uriStr)
	}
	position += len(AeronPrefix)

	var sb strings.Builder
	key := ""
	state := parseStateMedia
	for i := position; i < length; i++ {
		c := uriStr[i]
		switch state {
		case parseStateMedia:
			switch c {
			case '?':
				// Stricter than Java, which only validates media when there is no query.
				uri.media = sb.String()
				if err := validateMedia(uri.media); err != nil {
					return nil, err
				}
				sb.Reset()
				state = parseStateParamsKey
			case ':', '|', '=':
				return nil, fmt.Errorf("encountered '%c' within media definition at index %d in %s", c, i, uriStr)
			default:
				sb.WriteByte(c)
			}
		case parseStateParamsKey:
			switch c {
			case '=':
				if sb.Len() == 0 {
					return nil, fmt.Errorf("empty key not allowed at index %d in %s", i, uriStr)
				}
				key = sb.String()
				sb.Reset()
				state = parseStateParamsValue
			case '|':
				return nil, fmt.Errorf("invalid end of key at index %d in %s", i, uriStr)
			default:
				sb.WriteByte(c)
			}
		case parseStateParamsValue:
			if c == '|' {
				uri.Put(key, sb.String())
				sb.Reset()
				state = parseStateParamsKey
			} else {
				sb.WriteByte(c)
			}
		}
	}

	switch state {
	case parseStateMedia:
		uri.media = sb.String()
		if err := validateMedia(uri.media); err != nil {
			return nil, err
		}
	case parseStateParamsValue:
		uri.Put(key, sb.String())
	default:
		return nil, fmt.Errorf("no more input found, state=PARAMS_KEY in %s", uriStr)
	}
	return uri, nil
}

// MustParseChannelUri is like ParseChannelUri but panics on error.
func MustParseChannelUri(uriStr string) *ChannelUri {
	uri, err := ParseChannelUri(uriStr)
	if err != nil {
		panic(err)
	}
	return uri
}

func validateMedia(media string) error {
	if media == IpcMedia || media == UdpMedia {
		return nil
	}
	return fmt.Errorf("unknown media: %s", media)
}

// Clone returns a deep copy of a ChannelUri.
func (uri *ChannelUri) Clone() *ChannelUri {
	res := &ChannelUri{
		prefix: uri.prefix,
		media:  uri.media,
		keys:   append([]string(nil), uri.keys...),
		params: make(map[string]string, len(uri.params)),
	}
	for k, v := range uri.params {
		res.params[k] = v
	}
	return res
}

// Scheme always returns "aeron".
func (uri *ChannelUri) Scheme() string {
	return AeronScheme
}

// Prefix returns the prefix, e.g. "aeron-spy", or "" if none.
func (uri *ChannelUri) Prefix() string {
	return uri.prefix
}

// SetPrefix sets the prefix, e.g. "aeron-spy".
func (uri *ChannelUri) SetPrefix(prefix string) *ChannelUri {
	uri.prefix = prefix
	return uri
}

// Media returns the media, "udp" or "ipc".
func (uri *ChannelUri) Media() string {
	return uri.media
}

// SetMedia sets the media, which must be "udp" or "ipc".
func (uri *ChannelUri) SetMedia(media string) error {
	if err := validateMedia(media); err != nil {
		return err
	}
	uri.media = media
	return nil
}

// IsUdp returns true if the media is "udp".
func (uri *ChannelUri) IsUdp() bool {
	return uri.media == UdpMedia
}

// IsIpc returns true if the media is "ipc".
func (uri *ChannelUri) IsIpc() bool {
	return uri.media == IpcMedia
}

// Get returns the value for a key or "" if not present. Use Lookup to distinguish absent from empty.
func (uri *ChannelUri) Get(key string) string {
	return uri.params[key]
}

// GetOrDefault returns the value for a key or defaultValue if not present.
func (uri *ChannelUri) GetOrDefault(key string, defaultValue string) string {
	if v, ok := uri.params[key]; ok {
		return v
	}
	return defaultValue
}

// Lookup returns the value for a key and whether it was present.
func (uri *ChannelUri) Lookup(key string) (string, bool) {
	v, ok := uri.params[key]
	return v, ok
}

// Put sets a key/value pair, returning the previous value ("" if none).
func (uri *ChannelUri) Put(key string, value string) string {
	if uri.params == nil {
		uri.params = make(map[string]string)
	}
	prev, ok := uri.params[key]
	if !ok {
		uri.keys = append(uri.keys, key)
	}
	uri.params[key] = value
	return prev
}

// Set sets a key/value pair. Equivalent to Put ignoring the previous value.
func (uri *ChannelUri) Set(key string, value string) {
	uri.Put(key, value)
}

// Remove removes a key, returning the previous value ("" if none).
func (uri *ChannelUri) Remove(key string) string {
	prev, ok := uri.params[key]
	if !ok {
		return ""
	}
	delete(uri.params, key)
	for i, k := range uri.keys {
		if k == key {
			uri.keys = append(uri.keys[:i], uri.keys[i+1:]...)
			break
		}
	}
	return prev
}

// ContainsKey returns true if the key is present.
func (uri *ChannelUri) ContainsKey(key string) bool {
	_, ok := uri.params[key]
	return ok
}

// ForEachParameter calls fn for every parameter in insertion order.
func (uri *ChannelUri) ForEachParameter(fn func(key, value string)) {
	for _, k := range uri.keys {
		fn(k, uri.params[k])
	}
}

// SetControlMode sets the control-mode parameter.
func (uri *ChannelUri) SetControlMode(controlMode string) {
	uri.Put(MdcControlModeParamName, controlMode)
}

// SetSessionID sets the session-id parameter.
func (uri *ChannelUri) SetSessionID(sessionID int32) {
	uri.Put(SessionIdParamName, strconv.Itoa(int(sessionID)))
}

func (uri *ChannelUri) tags() []string {
	v, ok := uri.params[TagsParamName]
	if !ok {
		return nil
	}
	return strings.Split(v, ",")
}

// ChannelTag returns the channel tag (first element of the tags param) or "" if not present.
func (uri *ChannelUri) ChannelTag() string {
	if tags := uri.tags(); len(tags) > 0 {
		return tags[0]
	}
	return ""
}

// EntityTag returns the entity (publication/subscription) tag (second element of the tags param) or "" if not
// present.
func (uri *ChannelUri) EntityTag() string {
	if tags := uri.tags(); len(tags) > 1 {
		return tags[1]
	}
	return ""
}

// HasControlModeResponse returns true if control-mode=response.
func (uri *ChannelUri) HasControlModeResponse() bool {
	return uri.params[MdcControlModeParamName] == ControlModeResponse
}

// Equals compares prefix, media and parameters (order independent).
func (uri *ChannelUri) Equals(other *ChannelUri) bool {
	if uri == other {
		return true
	}
	if uri == nil || other == nil {
		return false
	}
	if uri.prefix != other.prefix || uri.media != other.media || len(uri.params) != len(other.params) {
		return false
	}
	for k, v := range uri.params {
		if ov, ok := other.params[k]; !ok || ov != v {
			return false
		}
	}
	return true
}

// diff returns the differing values between two URIs, keyed by param name (or "prefix"/"media").
func (uri *ChannelUri) diff(that *ChannelUri) map[string]string {
	d := make(map[string]string)
	if uri.prefix != that.prefix {
		d["prefix"] = uri.prefix + " != " + that.prefix
	}
	if uri.media != that.media {
		d["media"] = uri.media + " != " + that.media
	}
	for k, v := range uri.params {
		tv, ok := that.params[k]
		if !ok {
			d[k] = v + " != <nil>"
		} else if tv != v {
			d[k] = v + " != " + tv
		}
	}
	for k, tv := range that.params {
		if _, ok := uri.params[k]; !ok {
			d[k] = "<nil> != " + tv
		}
	}
	return d
}

// String renders the URI, with parameters in insertion order.
func (uri *ChannelUri) String() string {
	var sb strings.Builder
	sb.Grow(len(uri.params)*20 + 20)
	if uri.prefix != "" {
		sb.WriteString(uri.prefix)
		if !strings.HasSuffix(uri.prefix, ":") {
			sb.WriteByte(':')
		}
	}
	sb.WriteString(AeronPrefix)
	sb.WriteString(uri.media)
	if len(uri.keys) > 0 {
		sb.WriteByte('?')
		for i, k := range uri.keys {
			if i > 0 {
				sb.WriteByte('|')
			}
			sb.WriteString(k)
			sb.WriteByte('=')
			sb.WriteString(uri.params[k])
		}
	}
	return sb.String()
}

// InitialPosition sets init-term-id, term-id, term-offset and term-length so that a publication starts at the given
// position.
func (uri *ChannelUri) InitialPosition(position int64, initialTermId int32, termLength int32) error {
	if position < 0 || 0 != (position&int64(logbuffer.FrameAlignment-1)) {
		return fmt.Errorf("invalid position: %d", position)
	}
	bitsToShift, err := uriPositionBitsToShift(termLength)
	if err != nil {
		return err
	}
	termId := uriComputeTermIdFromPosition(position, bitsToShift, initialTermId)
	termOffset := int32(position & int64(termLength-1))
	uri.Put(InitialTermIdParamName, strconv.Itoa(int(initialTermId)))
	uri.Put(TermIdParamName, strconv.Itoa(int(termId)))
	uri.Put(TermOffsetParamName, strconv.Itoa(int(termOffset)))
	uri.Put(TermLengthParamName, strconv.Itoa(int(termLength)))
	return nil
}

// ReplaceEndpointWildcardPort replaces the wildcard port (":0") of the endpoint param with the port of
// resolvedEndpoint, or sets the endpoint if not present.
func (uri *ChannelUri) ReplaceEndpointWildcardPort(resolvedEndpoint string) error {
	portSeparatorIndex := strings.LastIndexByte(resolvedEndpoint, ':')
	if -1 == portSeparatorIndex {
		return fmt.Errorf("No port specified on resolvedEndpoint=%s", resolvedEndpoint)
	}
	if strings.HasSuffix(resolvedEndpoint, ":0") {
		return fmt.Errorf("Wildcard port specified on resolvedEndpoint=%s", resolvedEndpoint)
	}
	existingEndpoint, ok := uri.Lookup(EndpointParamName)
	if !ok {
		uri.Put(EndpointParamName, resolvedEndpoint)
	} else if strings.HasSuffix(existingEndpoint, ":0") {
		uri.Put(EndpointParamName, existingEndpoint[:len(existingEndpoint)-2]+resolvedEndpoint[portSeparatorIndex:])
	}
	return nil
}

// AddSessionID adds (or replaces) the session-id param on a channel string.
func AddSessionID(channel string, sessionId int32) (string, error) {
	uri, err := ParseChannelUri(channel)
	if err != nil {
		return "", err
	}
	uri.Put(SessionIdParamName, strconv.Itoa(int(sessionId)))
	return uri.String(), nil
}

// AddAliasIfAbsent adds the alias param to a channel if the alias is non-empty and the channel has no alias.
func AddAliasIfAbsent(channel string, alias string) (string, error) {
	if alias == "" {
		return channel, nil
	}
	uri, err := ParseChannelUri(channel)
	if err != nil {
		return "", err
	}
	if uri.ContainsKey(AliasParamName) {
		return channel, nil
	}
	uri.Put(AliasParamName, alias)
	return uri.String(), nil
}

// TransformAlias applies fn to the alias of a channel (fn receives "" if no alias). An empty result removes the
// alias.
func TransformAlias(channel string, fn func(alias string) string) (string, error) {
	uri, err := ParseChannelUri(channel)
	if err != nil {
		return "", err
	}
	transformed := fn(uri.Get(AliasParamName))
	if transformed == "" {
		uri.Remove(AliasParamName)
	} else {
		uri.Put(AliasParamName, transformed)
	}
	return uri.String(), nil
}

// IsTagged returns true if the param value is a tag reference, i.e. starts with "tag:".
func IsTagged(paramValue string) bool {
	return strings.HasPrefix(paramValue, TagPrefix)
}

// GetTag returns the tag value of a tagged param value, or InvalidTag if not tagged. Returns an error if the tag is
// not a valid number.
func GetTag(paramValue string) (int64, error) {
	if !IsTagged(paramValue) {
		return InvalidTag, nil
	}
	return uriParseLong(paramValue[len(TagPrefix):])
}

// CreateDestinationUri creates a destination URI for use with add/remove destination from the media of channel, the
// given endpoint and the interface of channel if present.
func CreateDestinationUri(channel string, endpoint string) (string, error) {
	uri, err := ParseChannelUri(channel)
	if err != nil {
		return "", err
	}
	result := AeronPrefix + uri.Media() + "?" + EndpointParamName + "=" + endpoint
	if networkInterface, ok := uri.Lookup(InterfaceParamName); ok {
		return result + "|" + InterfaceParamName + "=" + networkInterface, nil
	}
	return result, nil
}

// ReplacePortWithWildcard replaces the port of an endpoint with 0. Returns "" if endpoint has no port.
func ReplacePortWithWildcard(endpoint string) string {
	i := strings.LastIndexByte(endpoint, ':')
	if -1 == i {
		return ""
	}
	return endpoint[:i+1] + "0"
}

// IsControlModeResponse returns true if channel has control-mode=response.
func IsControlModeResponse(channel string) (bool, error) {
	uri, err := ParseChannelUri(channel)
	if err != nil {
		return false, err
	}
	return uri.HasControlModeResponse(), nil
}

// ValidateSocketTos parses and validates the value of the so-tos param (0-255).
func ValidateSocketTos(value string) (int32, error) {
	v, err := strconv.ParseInt(value, 10, 32)
	if err != nil {
		return 0, fmt.Errorf("%s must be a number: %w", SocketTosParamName, err)
	}
	if v < 0 || v > 255 {
		return 0, fmt.Errorf("%s must be between 0 and 255 inclusive: %s", SocketTosParamName, value)
	}
	return int32(v), nil
}
