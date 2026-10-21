package interop

import (
	"crypto/sha256"
	"encoding/hex"
	"testing"

	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/logbuffer"
	"github.com/stretchr/testify/require"
)

func TestParseSizes(t *testing.T) {
	s, err := ParseSizes("8, 100,1048576")
	require.NoError(t, err)
	require.Equal(t, Sizes{8, 100, 1048576}, s)
	require.Equal(t, "8,100,1048576", s.String())
	require.Equal(t, int32(100), s.Length(4))
	require.Equal(t, int32(1048576), s.Max())
	_, err = ParseSizes("7")
	require.Error(t, err)
	_, err = ParseSizes("x")
	require.Error(t, err)
}

func TestFillVerifyRoundTrip(t *testing.T) {
	sizes := Sizes{8, 9, 300}
	for seq := int64(0); seq < 10; seq++ {
		msg := make([]byte, sizes.Length(seq))
		Fill(msg, seq)
		require.Equal(t, "", Verify(msg, seq, sizes))
	}
	msg := make([]byte, 300)
	Fill(msg, 2)
	require.Contains(t, Verify(msg, 3, sizes), "expected seq 3")
	msg[100]++
	require.Contains(t, Verify(msg, 2, sizes), "mismatch at byte 100")
	require.Contains(t, Verify(msg[:9], 2, sizes), "length 9 != 300")
	require.Contains(t, Verify(msg[:4], 2, sizes), "short")
}

// The wire format is pinned so that the Java tool (which implements the same formula) stays in sync.
func TestKnownMessageBytes(t *testing.T) {
	msg := make([]byte, 12)
	Fill(msg, 0x0102)
	require.Equal(t, []byte{0x02, 0x01, 0, 0, 0, 0, 0, 0, 70, 71, 72, 73}, msg)
	require.Equal(t, int64(0x0000_0005_0000_CAFE), ReservedValue(5))
}

func TestVerifierDigestMatchesExpected(t *testing.T) {
	sizes := Sizes{8, 64, 5000}
	v := NewVerifier(sizes, true)
	buf := atomic.MakeBuffer(make([]byte, 5000))
	var header logbuffer.Header
	headerBuf := make([]byte, logbuffer.DataFrameHeader.Length)
	header.Wrap(atomic.MakeBuffer(headerBuf).Ptr(), int32(len(headerBuf)))
	for seq := int64(0); seq < 7; seq++ {
		l := sizes.Length(seq)
		FillBuffer(buf, 0, seq, l)
		header.SetReservedValue(ReservedValue(seq))
		v.OnFragment(buf, 0, l, &header)
	}
	r := v.Result(false)
	require.Equal(t, int64(7), r.Count)
	require.Equal(t, int64(0), r.Errors, r.FirstError)
	require.Equal(t, ExpectedDigest(sizes, 7), r.SHA256)

	header.SetReservedValue(42)
	FillBuffer(buf, 0, 7, sizes.Length(7))
	v.OnFragment(buf, 0, sizes.Length(7), &header)
	require.Equal(t, int64(1), v.Result(false).Errors)
	require.Contains(t, v.Result(false).FirstError, "reserved value 42")
}

func TestExpectedDigestEmpty(t *testing.T) {
	empty := sha256.Sum256(nil)
	require.Equal(t, hex.EncodeToString(empty[:]), ExpectedDigest(Sizes{8}, 0))
}
