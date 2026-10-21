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
	"math"
	"strconv"
)

// Helpers ported from org.agrona.SystemUtil / AsciiEncoding and io.aeron.logbuffer.LogBufferDescriptor used by
// ChannelUri and ChannelUriStringBuilder.

const (
	uriTermMinLength int32 = 64 * 1024
	uriTermMaxLength int32 = 1024 * 1024 * 1024

	uriOneKilobyte int64 = 1024
	uriOneMegabyte       = 1024 * uriOneKilobyte
	uriOneGigabyte       = 1024 * uriOneMegabyte

	uriOneMicrosecondNs int64 = 1000
	uriOneMillisecondNs       = 1000 * uriOneMicrosecondNs
	uriOneSecondNs            = 1000 * uriOneMillisecondNs
)

func isASCIIDigit(c byte) bool {
	return c >= '0' && c <= '9'
}

// uriParseLong parses an ASCII decimal long with an optional leading '-' (no '+', no whitespace).
func uriParseLong(s string) (int64, error) {
	if len(s) == 0 {
		return 0, fmt.Errorf("empty string")
	}
	start := 0
	if s[0] == '-' {
		start = 1
	}
	if start == len(s) {
		return 0, fmt.Errorf("error parsing long: %s", s)
	}
	for i := start; i < len(s); i++ {
		if !isASCIIDigit(s[i]) {
			return 0, fmt.Errorf("error parsing long: %s", s)
		}
	}
	v, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("error parsing long: %s", s)
	}
	return v, nil
}

func uriValidateNonNegative(name string, value string) error {
	if value == "" {
		return fmt.Errorf("%s must be non-empty: %s", name, value)
	}
	if value[0] == '-' {
		return fmt.Errorf("%s must be positive: %s", name, value)
	}
	return nil
}

// uriParseSize parses a size with an optional k, m or g suffix (case-insensitive), as SystemUtil.parseSize.
func uriParseSize(name string, value string) (int64, error) {
	if err := uriValidateNonNegative(name, value); err != nil {
		return 0, err
	}
	last := value[len(value)-1]
	if isASCIIDigit(last) {
		v, err := uriParseLong(value)
		if err != nil {
			return 0, fmt.Errorf("%s: %w", name, err)
		}
		return v, nil
	}
	v, err := uriParseLong(value[:len(value)-1])
	if err != nil {
		return 0, fmt.Errorf("%s: %w", name, err)
	}
	var multiplier int64
	switch last {
	case 'k', 'K':
		multiplier = uriOneKilobyte
	case 'm', 'M':
		multiplier = uriOneMegabyte
	case 'g', 'G':
		multiplier = uriOneGigabyte
	default:
		return 0, fmt.Errorf("%s: %s should end with: k, m, or g.", name, value)
	}
	if v > math.MaxInt64/multiplier {
		return 0, fmt.Errorf("%s would overflow a long: %s", name, value)
	}
	return v * multiplier, nil
}

// uriFormatSize formats a size using the largest exact k, m or g suffix, as SystemUtil.formatSize.
func uriFormatSize(size int64) string {
	for _, u := range []struct {
		n      int64
		suffix string
	}{{uriOneGigabyte, "g"}, {uriOneMegabyte, "m"}, {uriOneKilobyte, "k"}} {
		if size >= u.n && size%u.n == 0 {
			return strconv.FormatInt(size/u.n, 10) + u.suffix
		}
	}
	return strconv.FormatInt(size, 10)
}

// uriParseDuration parses a duration in nanoseconds with an optional s, ms, us or ns suffix, as
// SystemUtil.parseDuration.
func uriParseDuration(name string, value string) (int64, error) {
	if err := uriValidateNonNegative(name, value); err != nil {
		return 0, err
	}
	invalid := fmt.Errorf("%s: %s should end with: s, ms, us, or ns.", name, value)
	length := len(value)
	c := value[length-1]
	if isASCIIDigit(c) {
		v, err := uriParseLong(value)
		if err != nil {
			return 0, fmt.Errorf("%s: %w", name, err)
		}
		return v, nil
	}
	suffixIndex := length - 1
	suffixStart := c
	if (c == 's' || c == 'S') && length > 1 {
		c = value[length-2]
		if !isASCIIDigit(c) {
			suffixIndex--
			if suffixIndex <= 0 {
				return 0, invalid
			}
			suffixStart = c
		}
	} else {
		return 0, invalid
	}
	v, err := uriParseLong(value[:suffixIndex])
	if err != nil {
		return 0, fmt.Errorf("%s: %w", name, err)
	}
	var multiplier int64
	switch suffixStart {
	case 'n', 'N':
		multiplier = 1
	case 'u', 'U':
		multiplier = uriOneMicrosecondNs
	case 'm', 'M':
		multiplier = uriOneMillisecondNs
	case 's', 'S':
		multiplier = uriOneSecondNs
	default:
		return 0, invalid
	}
	if v > math.MaxInt64/multiplier {
		return math.MaxInt64, nil
	}
	return v * multiplier, nil
}

// uriFormatDuration formats nanoseconds using the largest exact s, ms, us or ns suffix, as
// SystemUtil.formatDuration.
func uriFormatDuration(durationNs int64) string {
	for _, u := range []struct {
		n      int64
		suffix string
	}{{uriOneSecondNs, "s"}, {uriOneMillisecondNs, "ms"}, {uriOneMicrosecondNs, "us"}} {
		if durationNs >= u.n && durationNs%u.n == 0 {
			return strconv.FormatInt(durationNs/u.n, 10) + u.suffix
		}
	}
	return strconv.FormatInt(durationNs, 10) + "ns"
}

func uriCheckTermLength(termLength int32) error {
	if termLength < uriTermMinLength {
		return fmt.Errorf("Term length less than min length of %d: length=%d", uriTermMinLength, termLength)
	}
	if termLength > uriTermMaxLength {
		return fmt.Errorf("Term length more than max length of %d: length=%d", uriTermMaxLength, termLength)
	}
	if termLength&(termLength-1) != 0 {
		return fmt.Errorf("Term length not a power of 2: length=%d", termLength)
	}
	return nil
}

func uriPositionBitsToShift(termBufferLength int32) (int32, error) {
	if termBufferLength >= uriTermMinLength && termBufferLength <= uriTermMaxLength &&
		termBufferLength&(termBufferLength-1) == 0 {
		shift := int32(0)
		for v := termBufferLength; v > 1; v >>= 1 {
			shift++
		}
		return shift, nil
	}
	return 0, fmt.Errorf("invalid term buffer length: %d", termBufferLength)
}

func uriComputeTermIdFromPosition(position int64, positionBitsToShift int32, initialTermId int32) int32 {
	return int32(uint64(position)>>uint(positionBitsToShift)) + initialTermId
}
