package resourcelimits

import (
	"context"
	"fmt"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/lirm/aeron-go/aeron"
	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/logbuffer"
	"github.com/stretchr/testify/require"
)

// lastTermChannel places a publication's first message termOffset bytes into the last term of the log
// (term count 2^31-1), so MAX_POSITION_EXCEEDED is reached after termLength-termOffset bytes. initTermID near
// MaxInt32 also makes the term id wrap negative.
func lastTermChannel(base string, termLength, initTermID, termOffset int32) string {
	termID := initTermID + math.MaxInt32 // wraps; termID - initTermID == 2^31-1
	return fmt.Sprintf("%s%sterm-length=%d|init-term-id=%d|term-id=%d|term-offset=%d", base, sep(base),
		termLength, initTermID, termID, termOffset)
}

func sep(base string) string {
	if strings.Contains(base, "?") {
		return "|"
	}
	return "?"
}

type peerResult struct {
	start, result, end string
}

// javaPeer runs testdata/RlPeer.java (Java Aeron 1.53.3) against the rl-driver and returns its output.
func javaPeer(t *testing.T, args ...string) peerResult {
	t.Helper()
	jar, err := filepath.Abs("../../driver/aeron-all-1.53.3.jar")
	require.NoError(t, err)
	peer, err := filepath.Abs("testdata")
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	cmdArgs := []string{"run", "--rm", "--network", "host", "--ipc", "host", "-v", "/dev/shm:/dev/shm",
		"-u", fmt.Sprintf("%d:%d", os.Getuid(), os.Getgid()),
		"-v", jar + ":/opt/aeron-all.jar:ro", "-v", peer + ":/peer:ro", "eclipse-temurin:21-jdk",
		"java", "--add-opens=java.base/jdk.internal.misc=ALL-UNNAMED", "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
		"-cp", "/opt/aeron-all.jar", "/peer/RlPeer.java", "dir=" + dir}
	out, err := exec.CommandContext(ctx, "docker", append(cmdArgs, args...)...).CombinedOutput()
	require.NoError(t, err, "java peer: %s", out)
	var r peerResult
	for _, l := range strings.Split(string(out), "\n") {
		switch {
		case strings.HasPrefix(l, "start "):
			r.start = l
		case strings.HasPrefix(l, "result "):
			r.result = l
		case strings.HasPrefix(l, "end "):
			r.end = l
		}
	}
	require.NotEmpty(t, r.result, "java peer output: %s", out)
	return r
}

type positionCase struct {
	name      string
	channel   string
	exclusive bool
	mode      string // offer | claim | block
	size      int
	attempts  int
}

// TestPositionEdgesMatchJava drives Go and Java publications through the identical deterministic sequence of
// offers/claims/blocks at the end of the log and across term-id wrap, and requires the same result codes
// (OK, ADMIN_ACTION, MAX_POSITION_EXCEEDED), the same start/end positions, and gap-free data at the subscriber.
func TestPositionEdgesMatchJava(t *testing.T) {
	requireStack(t, driverSvc)
	const term = 64 * 1024
	const nearMax = math.MaxInt32 - 3
	cases := []positionCase{
		{"concurrent offer aligned", lastTermChannel("aeron:ipc", term, 0, term-10*1024), false, "offer", 992, 15},
		{"concurrent offer padded", lastTermChannel("aeron:ipc", term, 7, term-10*1024), false, "offer", 1000, 15},
		{"exclusive offer aligned", lastTermChannel("aeron:ipc", term, -5, term-10*1024), true, "offer", 992, 15},
		{"exclusive offer padded", lastTermChannel("aeron:ipc", term, nearMax, term-10*1024), true, "offer", 1000, 15},
		{"concurrent claim", lastTermChannel("aeron:ipc", term, nearMax, term-4096), false, "claim", 500, 12},
		{"exclusive claim", lastTermChannel("aeron:ipc", term, 100, term-4096), true, "claim", 500, 12},
		// Exactly fills the last term. A further OfferBlock would rotate past the end of the log: Java 1.53.3
		// ExclusivePublication.offerBlock has no max-position check, the term count overflows and the Java driver
		// conductor then fails every duty cycle (IndexOutOfBoundsException). Go mirrors Java, so it is not exercised.
		{"exclusive block", lastTermChannel("aeron:ipc", term, nearMax, term-8*1024), true, "block", 1024, 8},
		{"concurrent fragmented", lastTermChannel("aeron:ipc", term, 3, term-20000), false, "offer", 6000, 6},
		{"exclusive fragmented", lastTermChannel("aeron:ipc", term, nearMax, term-20000), true, "offer", 6000, 6},
		{"udp exclusive offer", lastTermChannel("aeron:udp?endpoint=localhost:27901", term, nearMax, term-10*1024),
			true, "offer", 992, 15},
		{"udp concurrent claim", lastTermChannel("aeron:udp?endpoint=localhost:27902", term, 9, term-4096),
			false, "claim", 500, 12},
		{"max term length exclusive", lastTermChannel("aeron:ipc", 1<<30, nearMax, 1<<30-8192), true, "offer", 992, 12},
		{"term id wrap concurrent", fmt.Sprintf("aeron:ipc?term-length=%d|init-term-id=%d|term-id=%d|term-offset=0",
			term, nearMax, nearMax), false, "offer", 992, 64 * 6},
		{"term id wrap exclusive", fmt.Sprintf("aeron:ipc?term-length=%d|init-term-id=%d|term-id=%d|term-offset=0",
			term, nearMax, nearMax), true, "offer", 992, 64 * 6},
		{"term id wrap exclusive block", fmt.Sprintf("aeron:ipc?term-length=%d|init-term-id=%d|term-id=%d|term-offset=0",
			term, math.MaxInt32, math.MaxInt32), true, "block", 2048, 32 * 4},
	}
	for i, tc := range cases {
		stream := int32(9900 + i)
		t.Run(tc.name, func(t *testing.T) {
			java := javaPeer(t, "channel="+tc.channel, fmt.Sprint("stream=", stream), fmt.Sprint("exclusive=", tc.exclusive),
				"mode="+tc.mode, fmt.Sprint("size=", tc.size), fmt.Sprint("attempts=", tc.attempts))
			goRes := goPeer(t, tc, stream+50)
			require.Equal(t, java, goRes, "Go and Java Aeron 1.53.3 disagree")
			if tc.mode != "block" && !strings.Contains(tc.name, "wrap") {
				require.Contains(t, goRes.result, "MAX_POSITION_EXCEEDED")
			}
		})
	}
}

// goPeer is the Go twin of RlPeer.java; it also checks the subscriber sees every committed message in order.
func goPeer(t *testing.T, tc positionCase, stream int32) peerResult {
	t.Helper()
	base := baseline(t)
	eh := &errs{}
	a, err := aeron.Connect(newContext(dir, eh))
	require.NoError(t, err)
	t.Cleanup(func() { _ = a.Close() })
	driverErrs := driverErrors(a)
	require.Zero(t, driverErrs, "driver already has errors (e.g. from the Java peer)")
	sub, err := a.AddSubscription(tc.channel, stream)
	require.NoError(t, err)
	var pub aeron.Publisher
	var excl *aeron.ExclusivePublication
	if tc.exclusive {
		excl, err = a.AddExclusivePublication(tc.channel, stream)
		pub = excl
	} else {
		pub, err = a.AddPublication(tc.channel, stream)
	}
	require.NoError(t, err)
	awaitTrue(t, "connected", 20*time.Second, func() bool { return pub.IsConnected() && sub.ImageCount() > 0 })

	var r peerResult
	r.start = fmt.Sprintf("start position=%d maxPossiblePosition=%d", pub.Position(), pub.MaxPossiblePosition())
	buf := payload(tc.size)
	var claim logbuffer.Claim
	var sent, received int64
	nextSeq := int64(0)
	assembler := aeron.NewFragmentAssembler(func(b *atomic.Buffer, offset, length int32, h *logbuffer.Header) {
		if tc.mode != "block" {
			require.Equal(t, int32(tc.size), length)
			require.Equal(t, nextSeq, b.GetInt64(offset), "gap or duplicate")
		}
		nextSeq++
		received++
	}, 1024)
	var out []string
	last, run := "", 0
	deadline := time.Now().Add(20 * time.Second)
	for n := 0; n < tc.attempts; {
		var res int64
		switch tc.mode {
		case "claim":
			res = pub.TryClaim(int32(tc.size), &claim)
			if res > 0 {
				claim.Buffer().PutInt64(claim.Offset(), sent)
				claim.Commit()
			}
		case "block":
			res = blockOffer(excl, buf, tc.size)
		default:
			buf.PutInt64(0, sent)
			res = pub.Offer(buf, 0, int32(tc.size), nil)
		}
		sub.Poll(assembler.OnFragment, 100)
		if res == aeron.BackPressured || res == aeron.NotConnected {
			require.True(t, time.Now().Before(deadline), "stuck %s", aeron.PublicationErrorString(res))
			time.Sleep(time.Millisecond)
			continue
		}
		n++
		code := "OK"
		if res > 0 {
			sent++
		} else {
			code = aeron.PublicationErrorString(res)
		}
		if code != last {
			if last != "" {
				out = append(out, fmt.Sprintf("%s*%d", last, run))
			}
			last, run = code, 0
		}
		run++
	}
	out = append(out, fmt.Sprintf("%s*%d", last, run))
	r.result = "result " + strings.Join(out, " ")
	r.end = fmt.Sprintf("end position=%d", pub.Position())

	img := sub.ImageBySessionID(pub.SessionID())
	require.NotNil(t, img)
	// The image ends at the publication position, or past it at the padding an exclusive publication writes
	// without advancing its own position when the message does not fit.
	awaitTrue(t, "subscriber caught up", 10*time.Second, func() bool {
		sub.Poll(assembler.OnFragment, 100)
		return received == sent && img.Position() >= pub.Position()
	})
	r.end += fmt.Sprintf(" imagePosition=%d", img.Position())
	require.Empty(t, eh.all())
	require.Zero(t, driverErrors(a), "media driver errors")
	require.NoError(t, pub.Close())
	require.NoError(t, sub.Close())
	require.NoError(t, a.Close())
	base.check(t, dir)
	return r
}

func blockOffer(pub *aeron.ExclusivePublication, buf *atomic.Buffer, size int) int64 {
	// OfferBlock rotates first when the term is full, so the block must already carry the next term's coordinates.
	termOffset, termID := pub.TermOffset(), pub.TermID()
	if termOffset >= pub.TermBufferLength() {
		termOffset, termID = 0, termID+1
	}
	h := logbuffer.DataFrameHeader
	buf.PutInt32(h.FrameLengthFieldOffset, int32(size))
	buf.PutUInt8(h.VersionFieldOffset, 0)
	buf.PutUInt8(h.FlagsFieldOffset, 0xC0)
	buf.PutUInt16(h.TypeFieldOffset, h.TypeData)
	buf.PutInt32(h.TermOffsetFieldOffset, termOffset)
	buf.PutInt32(h.SessionIDFieldOffset, pub.SessionID())
	buf.PutInt32(h.StreamIDFieldOffset, pub.StreamID())
	buf.PutInt32(h.TermIDFieldOffset, termID)
	return pub.OfferBlock(buf, 0, int32(size))
}
