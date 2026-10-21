// Package interop contains the Go half of the Go <-> Java Aeron interoperability tests and the deterministic
// message format shared with docker/interop/src/io/aeron/interop/InteropTool.java.
//
// Message i of a run is generated identically on both sides:
//
//	length(i)   = sizes[i % len(sizes)]
//	bytes[0..8) = little-endian i, bytes[j] = byte(i*31 + j) for j >= 8
//	reserved(i) = (i << 32) | 0xCAFE
//
// Receivers verify every message in order and report a SHA-256 over all received payload bytes.
package interop

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"hash"
	"strconv"
	"strings"

	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/logbuffer"
)

// MinMessageLength is the smallest message length the format supports (the sequence number).
const MinMessageLength = 8

// Sizes is a cyclic list of message lengths.
type Sizes []int32

// ParseSizes parses a comma separated list such as "8,100,1048576".
func ParseSizes(s string) (Sizes, error) {
	parts := strings.Split(s, ",")
	sizes := make(Sizes, 0, len(parts))
	for _, p := range parts {
		v, err := strconv.ParseInt(strings.TrimSpace(p), 10, 32)
		if err != nil {
			return nil, err
		}
		if v < MinMessageLength {
			return nil, fmt.Errorf("message length %d < %d", v, MinMessageLength)
		}
		sizes = append(sizes, int32(v))
	}
	return sizes, nil
}

// String renders the sizes in the form accepted by ParseSizes and the Java tool.
func (s Sizes) String() string {
	parts := make([]string, len(s))
	for i, v := range s {
		parts[i] = strconv.Itoa(int(v))
	}
	return strings.Join(parts, ",")
}

// Length of message seq.
func (s Sizes) Length(seq int64) int32 {
	return s[seq%int64(len(s))]
}

// Max message length in the cycle.
func (s Sizes) Max() int32 {
	var m int32
	for _, v := range s {
		if v > m {
			m = v
		}
	}
	return m
}

// ReservedValue is the reserved value publishers attach to message seq.
func ReservedValue(seq int64) int64 {
	return seq<<32 | 0xCAFE
}

// Fill writes message seq of the given length into dst.
func Fill(dst []byte, seq int64) {
	binary.LittleEndian.PutUint64(dst, uint64(seq))
	for j := MinMessageLength; j < len(dst); j++ {
		dst[j] = byte(seq*31 + int64(j))
	}
}

// FillBuffer writes message seq of the given length into buffer at offset.
func FillBuffer(buffer *atomic.Buffer, offset int32, seq int64, length int32) {
	b := make([]byte, length)
	Fill(b, seq)
	buffer.PutBytesSlice(offset, b[:length])
}

// Verify checks that msg is message expectedSeq. It returns "" when valid.
func Verify(msg []byte, expectedSeq int64, sizes Sizes) string {
	if len(msg) < MinMessageLength {
		return fmt.Sprintf("seq %d short message length %d", expectedSeq, len(msg))
	}
	seq := int64(binary.LittleEndian.Uint64(msg))
	if seq != expectedSeq {
		return fmt.Sprintf("expected seq %d got %d", expectedSeq, seq)
	}
	if want := sizes.Length(seq); int32(len(msg)) != want {
		return fmt.Sprintf("seq %d length %d != %d", seq, len(msg), want)
	}
	for j := MinMessageLength; j < len(msg); j++ {
		if msg[j] != byte(seq*31+int64(j)) {
			return fmt.Sprintf("seq %d payload mismatch at byte %d", seq, j)
		}
	}
	return ""
}

// ExpectedDigest is the SHA-256 a receiver reports after receiving messages [0, count).
func ExpectedDigest(sizes Sizes, count int64) string {
	return ExpectedDigestFrom(sizes, 0, count)
}

// ExpectedDigestFrom is the SHA-256 a receiver reports after receiving messages [first, first+count).
func ExpectedDigestFrom(sizes Sizes, first, count int64) string {
	h := sha256.New()
	buf := make([]byte, sizes.Max())
	for i := first; i < first+count; i++ {
		msg := buf[:sizes.Length(i)]
		Fill(msg, i)
		h.Write(msg)
	}
	return hex.EncodeToString(h.Sum(nil))
}

// Result is what a receiver reports; the JSON form matches the Java tool's "result" event.
type Result struct {
	Count      int64  `json:"count"`
	Errors     int64  `json:"errors"`
	FirstError string `json:"firstError"`
	SHA256     string `json:"sha256"`
	Bytes      int64  `json:"bytes"`
	Position   int64  `json:"position"`
	SessionID  int32  `json:"sessionId"`
	TimedOut   bool   `json:"timedOut"`
}

// Verifier is a fragment handler verifying whole messages (wrap it in a FragmentAssembler for fragmented ones).
type Verifier struct {
	Sizes         Sizes
	CheckReserved bool
	// First is the sequence number of the first expected message (e.g. a replay from a mid position).
	First int64

	count, errors, bytes int64
	firstError           string
	lastPosition         int64
	sessionID            int32
	digest               hash.Hash
}

// NewVerifier creates a Verifier for messages generated with sizes.
func NewVerifier(sizes Sizes, checkReserved bool) *Verifier {
	return &Verifier{Sizes: sizes, CheckReserved: checkReserved, digest: sha256.New()}
}

// Count of messages received so far.
func (v *Verifier) Count() int64 {
	return v.count
}

// OnFragment is a term.FragmentHandler.
func (v *Verifier) OnFragment(buffer *atomic.Buffer, offset int32, length int32, header *logbuffer.Header) {
	msg := buffer.GetBytesArray(offset, length)
	v.OnMessage(msg, header.Position(), header.SessionID(), header.ReservedValue())
}

// OnMessage verifies one whole message.
func (v *Verifier) OnMessage(msg []byte, position int64, sessionID int32, reservedValue int64) {
	expected := v.First + v.count
	if err := Verify(msg, expected, v.Sizes); err != "" {
		v.error(err)
	} else if v.CheckReserved && reservedValue != ReservedValue(expected) {
		v.error(fmt.Sprintf("seq %d reserved value %d != %d", expected, reservedValue, ReservedValue(expected)))
	}
	v.digest.Write(msg)
	v.bytes += int64(len(msg))
	v.count++
	v.lastPosition = position
	v.sessionID = sessionID
}

func (v *Verifier) error(err string) {
	if v.errors == 0 {
		v.firstError = err
	}
	v.errors++
}

// Result snapshot of what has been received.
func (v *Verifier) Result(timedOut bool) Result {
	return Result{
		Count:      v.count,
		Errors:     v.errors,
		FirstError: v.firstError,
		SHA256:     hex.EncodeToString(v.digest.Sum(nil)),
		Bytes:      v.bytes,
		Position:   v.lastPosition,
		SessionID:  v.sessionID,
		TimedOut:   timedOut,
	}
}
