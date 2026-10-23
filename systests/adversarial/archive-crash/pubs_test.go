package archivecrash

import (
	"fmt"
	"os"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lirm/aeron-go/aeron"
	aatomic "github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/logbuffer"
	"github.com/lirm/aeron-go/aeron/logbuffer/term"
	"github.com/lirm/aeron-go/e2e"
	"github.com/stretchr/testify/require"
)

type livePub interface {
	Offer(buffer *aatomic.Buffer, offset int32, length int32, reservedValueSupplier term.ReservedValueSupplier) int64
	TryClaim(length int32, claim *logbuffer.Claim) int64
	Position() int64
	SessionID() int32
	IsConnected() bool
	Close() error
}

// runner publishes sequenced messages on a publication in the background, recording the stream position each
// message ends at.
type runner struct {
	mu     sync.Mutex
	endPos map[int64]int64
	next   int64
	stop   chan struct{}
	done   chan struct{}
	err    error
}

func runPublisher(pub livePub, from, count int64, size int, f format, claim bool, interval time.Duration) *runner {
	r := &runner{endPos: map[int64]int64{}, next: from, stop: make(chan struct{}), done: make(chan struct{})}
	go func() {
		defer close(r.done)
		msg := make([]byte, size)
		buf := aatomic.MakeBuffer(msg)
		var bc logbuffer.Claim
		for seq := from; count < 0 || seq < from+count; seq++ {
			if f == goFormat {
				e2e.EncodeMessage(msg, seq)
			} else {
				javaFill(msg, seq)
			}
			for {
				var ret int64
				if claim {
					if ret = pub.TryClaim(int32(size), &bc); ret >= 0 {
						bc.Buffer().PutBytesArray(bc.Offset(), &msg, 0, int32(size))
						bc.Commit()
					}
				} else {
					ret = pub.Offer(buf, 0, int32(size), nil)
				}
				if ret >= 0 {
					r.mu.Lock()
					r.endPos[seq] = ret
					r.next = seq + 1
					r.mu.Unlock()
					break
				}
				if ret != aeron.NotConnected && ret != aeron.BackPressured && ret != aeron.AdminAction {
					r.err = fmt.Errorf("offer seq %d: %d", seq, ret)
					return
				}
				select {
				case <-r.stop:
					return
				default:
				}
				time.Sleep(50 * time.Microsecond)
			}
			select {
			case <-r.stop:
				return
			default:
			}
			if interval > 0 {
				time.Sleep(interval)
			}
		}
	}()
	return r
}

// halt stops the runner and returns a copy of the published positions.
func (r *runner) halt() map[int64]int64 {
	select {
	case <-r.stop:
	default:
		close(r.stop)
	}
	<-r.done
	return r.positions()
}

func (r *runner) positions() map[int64]int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	m := make(map[int64]int64, len(r.endPos))
	for k, v := range r.endPos {
		m[k] = v
	}
	return m
}

func (r *runner) sent() int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.next
}

// pubKind is one publisher type whose stream is recorded by the archive.
type pubKind struct {
	name       string
	pubChannel string
	recChannel string // channel given to StartRecording
	fragment   string // channel fragment for ListRecordingsForUri
	local      bool   // isLocal (IPC / spy)
	exclusive  bool
	claim      bool
	size       int
	mdcDest    string // manual MDC destination
	liveSub    string // a live subscriber that must exist for the publication to connect (spy kind)
	java       bool
	multicast  bool
	block      bool // ExclusivePublication.OfferBlock of self-built frames; pubChannel must start at term 0 offset 0
}

func (k pubKind) format() format {
	if k.java {
		return javaFormat
	}
	return goFormat
}

var pubKinds = []pubKind{
	{name: "udp-concurrent-offer", pubChannel: "aeron:udp?endpoint=localhost:27330|term-length=65536",
		recChannel: "aeron:udp?endpoint=localhost:27330", fragment: "27330", size: 200},
	{name: "udp-exclusive-tryclaim", pubChannel: "aeron:udp?endpoint=localhost:27331|term-length=65536",
		recChannel: "aeron:udp?endpoint=localhost:27331", fragment: "27331", exclusive: true, claim: true, size: 300},
	{name: "ipc-concurrent-tryclaim", pubChannel: "aeron:ipc?term-length=65536", recChannel: "aeron:ipc",
		fragment: "aeron:ipc", local: true, claim: true, size: 128},
	{name: "udp-spy-exclusive", pubChannel: "aeron:udp?endpoint=localhost:27332|term-length=65536",
		recChannel: "aeron-spy:aeron:udp?endpoint=localhost:27332", fragment: "27332", local: true, exclusive: true,
		size: 256, liveSub: "aeron:udp?endpoint=localhost:27332"},
	{name: "mdc-dynamic", pubChannel: "aeron:udp?control=localhost:27333|control-mode=dynamic|term-length=65536",
		recChannel: "aeron:udp?endpoint=localhost:27334|control=localhost:27333", fragment: "27334", exclusive: true,
		size: 200},
	{name: "mdc-manual", pubChannel: "aeron:udp?control-mode=manual|term-length=65536",
		recChannel: "aeron:udp?endpoint=localhost:27335", fragment: "27335", size: 200,
		mdcDest: "aeron:udp?endpoint=localhost:27335"},
	{name: "fragmented-20k", pubChannel: "aeron:udp?endpoint=localhost:27336|term-length=1048576",
		recChannel: "aeron:udp?endpoint=localhost:27336", fragment: "27336", exclusive: true, size: 20000},
	{name: "multicast", pubChannel: "aeron:udp?endpoint=239.255.93.1:27337|interface=127.0.0.1|ttl=0|term-length=65536",
		recChannel: "aeron:udp?endpoint=239.255.93.1:27337|interface=127.0.0.1", fragment: "27337", exclusive: true,
		size: 200, multicast: true},
	{name: "udp-exclusive-offerblock",
		pubChannel: "aeron:udp?endpoint=localhost:27339|term-length=65536|init-term-id=0|term-id=0|term-offset=0",
		recChannel: "aeron:udp?endpoint=localhost:27339", fragment: "27339", exclusive: true, size: 333, block: true},
	{name: "java-exclusive", pubChannel: "aeron:udp?endpoint=localhost:27338|term-length=65536",
		recChannel: "aeron:udp?endpoint=localhost:27338", fragment: "27338", exclusive: true, size: 200, java: true},
}

func multicastLoopback() bool {
	out, err := exec.Command("ip", "link", "show", "lo").Output()
	return err == nil && strings.Contains(string(out), "MULTICAST")
}

func (k pubKind) skipIfUnsupported(t *testing.T) {
	t.Helper()
	if k.multicast && !multicastLoopback() {
		t.Skip("loopback is not multicast capable (sudo ip link set lo multicast on)")
	}
	if k.java {
		if err := exec.Command("docker", "image", "inspect", interopImage).Run(); err != nil {
			t.Skipf("interop image %s missing (docker compose -f docker/interop/docker-compose.yml --profile tools build)",
				interopImage)
		}
	}
}

// open creates the Go publication of the kind.
func (k pubKind) open(t *testing.T, a *aeron.Aeron, stream int32) livePub {
	t.Helper()
	var pub livePub
	var err error
	if k.exclusive {
		var p *aeron.ExclusivePublication
		p, err = a.AddExclusivePublication(k.pubChannel, stream)
		if err == nil && k.mdcDest != "" {
			err = p.AddDestination(k.mdcDest)
		}
		pub = p
		if k.block {
			pub = &blockPub{ExclusivePublication: p, stream: stream}
		}
	} else {
		var p *aeron.Publication
		p, err = a.AddPublication(k.pubChannel, stream)
		if err == nil && k.mdcDest != "" {
			err = p.AddDestination(k.mdcDest)
		}
		pub = p
	}
	require.NoError(t, err)
	t.Cleanup(func() { _ = pub.Close() })
	return pub
}

var javaSeq int32

// javaPub is a Java InteropTool publisher container.
type javaPub struct{ name string }

func startJavaPub(t *testing.T, channel string, stream int32, size int) *javaPub {
	t.Helper()
	j := &javaPub{name: fmt.Sprintf("adv-archive-crash-java-%d-%d", os.Getpid(), atomic.AddInt32(&javaSeq, 1))}
	docker(t, "run", "-d", "--rm", "--name", j.name, "--network", "host", "--ipc", "host", "-v", "/dev/shm:/dev/shm",
		"-u", fmt.Sprintf("%d:%d", os.Getuid(), os.Getgid()), "-e", "AERON_PROPS=-Daeron.dir="+aeronDir, interopImage,
		"pub", "channel="+channel, fmt.Sprintf("stream=%d", stream), "exclusive=true", "count=1000000000",
		fmt.Sprintf("sizes=%d", size), "delayUs=200", "timeoutMs=600000", "lingerMs=600000")
	t.Cleanup(j.kill)
	return j
}

func (j *javaPub) kill() {
	_ = exec.Command("docker", "kill", "-s", "KILL", j.name).Run()
	deadline := time.Now().Add(15 * time.Second)
	for running(j.name) && time.Now().Before(deadline) {
		time.Sleep(100 * time.Millisecond)
	}
}

// blockPub offers every message as a pre-formatted single-frame block through ExclusivePublication.OfferBlock.
// The publication starts at term id 0, offset 0 with a 64KiB term, so the term id and offset follow from the
// position. A frame that does not fit in the rest of the term goes through Offer, which pads the term.
type blockPub struct {
	*aeron.ExclusivePublication
	stream int32
	frame  []byte
}

const blockTermLength = 65536

func (b *blockPub) Offer(buffer *aatomic.Buffer, offset int32, length int32, rvs term.ReservedValueSupplier) int64 {
	hdr := &logbuffer.DataFrameHeader
	frameLength := hdr.Length + length
	aligned := (frameLength + 31) &^ 31
	pos := b.Position()
	termOffset := int32(pos & (blockTermLength - 1))
	if termOffset+aligned > blockTermLength {
		return b.ExclusivePublication.Offer(buffer, offset, length, rvs)
	}
	if len(b.frame) < int(aligned) {
		b.frame = make([]byte, aligned)
	}
	frame := aatomic.MakeBuffer(b.frame[:aligned])
	frame.SetMemory(0, aligned, 0)
	frame.PutInt32(hdr.FrameLengthFieldOffset, frameLength)
	frame.PutUInt8(hdr.VersionFieldOffset, 0)
	frame.PutUInt8(hdr.FlagsFieldOffset, hdr.BeginAndEndFlags)
	frame.PutUInt16(hdr.TypeFieldOffset, hdr.TypeData)
	frame.PutInt32(hdr.TermOffsetFieldOffset, termOffset)
	frame.PutInt32(hdr.SessionIDFieldOffset, b.SessionID())
	frame.PutInt32(hdr.StreamIDFieldOffset, b.stream)
	frame.PutInt32(hdr.TermIDFieldOffset, int32(pos/blockTermLength))
	frame.PutBytes(hdr.Length, buffer, offset, length)
	return b.OfferBlock(frame, 0, aligned)
}
