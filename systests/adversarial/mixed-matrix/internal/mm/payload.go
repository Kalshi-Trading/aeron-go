// Package mm is the harness shared by the systests/adversarial/mixed-matrix tests and the mmpeer child process:
// the message wire format, Go publisher/consumer runners of every type and a per-image delivery tracker.
//
// Message seq of writer w (identical to docker/adversarial-mixed-matrix/src/MatrixPeer.java):
//
//	length     = sizes[seq % len(sizes)]
//	payload[j] = byte(seq*31 + j + w*7)
//	reserved   = crc32(payload)<<32 | w<<24 | seq&0xFFFFFF
package mm

import (
	"hash/crc32"
	"strconv"
	"strings"
)

// SeqMask bounds the sequence carried in the reserved value.
const SeqMask = 0xFFFFFF

// Fill writes the payload of message seq from writer into buf.
func Fill(buf []byte, writer uint8, seq uint32) {
	base := byte(seq*31) + writer*7
	for j := range buf {
		buf[j] = base + byte(j)
	}
}

// ReservedValue is the header reserved value of a message: CRC32 (IEEE) of the payload, writer and sequence.
func ReservedValue(payload []byte, writer uint8, seq uint32) int64 {
	return int64(uint64(crc32.ChecksumIEEE(payload))<<32 | uint64(writer)<<24 | uint64(seq&SeqMask))
}

// Decode splits a reserved value.
func Decode(rv int64) (crc uint32, writer uint8, seq uint32) {
	u := uint64(rv)
	return uint32(u >> 32), uint8(u >> 24), uint32(u & SeqMask)
}

// Check verifies payload against the reserved value, returning a description of the first mismatch or "".
func Check(payload []byte, rv int64) string {
	crc, writer, seq := Decode(rv)
	if got := crc32.ChecksumIEEE(payload); got != crc {
		return "crc mismatch writer " + strconv.Itoa(int(writer)) + " seq " + strconv.Itoa(int(seq))
	}
	base := byte(seq*31) + writer*7
	for j, b := range payload {
		if b != base+byte(j) {
			return "payload mismatch writer " + strconv.Itoa(int(writer)) + " seq " + strconv.Itoa(int(seq)) +
				" at " + strconv.Itoa(j)
		}
	}
	return ""
}

// ParseSizes parses a comma separated size list.
func ParseSizes(s string) []int {
	var sizes []int
	for _, p := range strings.Split(s, ",") {
		n, err := strconv.Atoi(strings.TrimSpace(p))
		if err == nil {
			sizes = append(sizes, n)
		}
	}
	return sizes
}

// FormatSizes is the inverse of ParseSizes.
func FormatSizes(sizes []int) string {
	parts := make([]string, len(sizes))
	for i, s := range sizes {
		parts[i] = strconv.Itoa(s)
	}
	return strings.Join(parts, ",")
}
