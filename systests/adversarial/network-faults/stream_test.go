package networkfaults

import (
	"fmt"
	"sync/atomic"
	"time"

	"github.com/lirm/aeron-go/aeron"
	ab "github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/logbuffer"
	"github.com/lirm/aeron-go/aeron/logbuffer/term"
	"github.com/lirm/aeron-go/aeron/util"
	"github.com/lirm/aeron-go/e2e"
)

type pubMode int

const (
	modeOffer pubMode = iota
	modeTryClaim
	modeOfferBlock // ExclusivePublication.OfferBlock of one pre-formatted frame, falling back to Offer at term ends
)

func smallSize(seq int64) int { return e2e.HeaderLength + int(seq%200) }

// mixedSize mixes small messages with ones fragmented at the default 1408 mtu.
func mixedSize(seq int64) int {
	if seq%10 == 0 {
		return 3000 + int(seq%5000)
	}
	return e2e.HeaderLength + int(seq%300)
}

// publisher offers sequenced e2e messages, retrying on back pressure / not connected until its deadline.
type publisher struct {
	pub      aeron.Publisher
	mode     pubMode
	size     func(int64) int
	next     atomic.Int64 // next sequence to publish
	lastOK   atomic.Int64 // unix nanos of the last successful offer
	maxStall atomic.Int64 // longest time between successful offers (ns)
	notConn  atomic.Int64 // offers that returned NotConnected
	backPres atomic.Int64 // offers that returned BackPressured
	stop     atomic.Bool
	results  map[int64]int
}

func newPublisher(pub aeron.Publisher, mode pubMode, size func(int64) int) *publisher {
	p := &publisher{pub: pub, mode: mode, size: size}
	p.lastOK.Store(time.Now().UnixNano())
	return p
}

// publish sends count more messages (until the deadline), sleeping interval after each.
func (p *publisher) publish(count int64, interval time.Duration, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	msg := make([]byte, 1<<20)
	buf := ab.MakeBuffer(msg)
	var claim logbuffer.Claim
	end := p.next.Load() + count
	for seq := p.next.Load(); seq < end && !p.stop.Load(); seq++ {
		n := p.size(seq)
		e2e.EncodeMessage(msg[:n], seq)
		for {
			var r int64
			if ex, ok := p.pub.(*aeron.ExclusivePublication); ok && p.mode == modeOfferBlock && blockFits(ex, n) {
				block := formatFrame(ex, msg[:n])
				r = ex.OfferBlock(ab.MakeBuffer(block), 0, int32(len(block)))
			} else if p.mode == modeTryClaim && int32(n) <= p.pub.MaxPayloadLength() {
				if r = p.pub.TryClaim(int32(n), &claim); r > 0 {
					claim.Buffer().PutBytesSlice(claim.Offset(), msg[:n])
					claim.Commit()
				}
			} else {
				r = p.pub.Offer(buf, 0, int32(n), nil)
			}
			if r > 0 {
				break
			}
			if p.stop.Load() {
				return nil
			}
			switch r {
			case aeron.NotConnected:
				p.notConn.Add(1)
			case aeron.BackPressured:
				p.backPres.Add(1)
			case aeron.AdminAction:
			default:
				return fmt.Errorf("offer seq %d: result %d", seq, r)
			}
			if time.Now().After(deadline) {
				return fmt.Errorf("offer seq %d timed out after %v (last result %d, connected %v)", seq, timeout, r, p.pub.IsConnected())
			}
			time.Sleep(50 * time.Microsecond)
		}
		now := time.Now().UnixNano()
		if stall := now - p.lastOK.Swap(now); stall > p.maxStall.Load() {
			p.maxStall.Store(stall)
		}
		p.next.Store(seq + 1)
		if interval > 0 {
			time.Sleep(interval)
		}
	}
	return nil
}

func (p *publisher) resetStall() {
	p.lastOK.Store(time.Now().UnixNano())
	p.maxStall.Store(0)
}

// publishAsync runs publish on a goroutine; the returned channel yields its error.
func (p *publisher) publishAsync(count int64, interval, timeout time.Duration) chan error {
	done := make(chan error, 1)
	go func() { done <- p.publish(count, interval, timeout) }()
	return done
}

// consumer polls a subscription through a fragment assembler into a sequence checker.
type consumer struct {
	sub        *aeron.Subscription
	checker    *e2e.SequenceChecker
	controlled bool
	aborts     int64
	poll       func() int
}

func newConsumer(sub *aeron.Subscription, from int64, controlled bool) *consumer {
	c := &consumer{sub: sub, checker: e2e.NewSequenceChecker(from), controlled: controlled}
	onMessage := c.checker.Handler()
	if controlled {
		// Abort the first delivery of every 7th message: the same message must be redelivered at the same position.
		lastAborted := int64(-1)
		var positions = map[int64]int64{}
		handler := func(buffer *ab.Buffer, offset, length int32, header *logbuffer.Header) term.ControlledPollAction {
			seq := int64(buffer.GetInt64(offset))
			if seq%7 == 3 && seq != lastAborted {
				lastAborted = seq
				positions[seq] = header.Position()
				c.aborts++
				return term.ControlledPollActionAbort
			}
			if pos, ok := positions[seq]; ok {
				delete(positions, seq)
				if pos != header.Position() {
					c.checker.FirstError = fmt.Errorf("seq %d redelivered at %d, aborted at %d", seq, header.Position(), pos)
					c.checker.Corrupt++
				}
			}
			onMessage(buffer, offset, length, header)
			if seq%5 == 0 {
				return term.ControlledPollActionCommit
			}
			return term.ControlledPollActionContinue
		}
		assembler := aeron.NewControlledFragmentAssembler(handler, aeron.DefaultFragmentAssemblyBufferLength)
		c.poll = func() int { return sub.ControlledPoll(assembler.OnFragment, 64) }
	} else {
		assembler := aeron.NewFragmentAssembler(onMessage, aeron.DefaultFragmentAssemblyBufferLength)
		c.poll = func() int { return sub.Poll(assembler.OnFragment, 64) }
	}
	return c
}

// consume polls until the checker expects until, a sequence error occurs, or timeout.
func (c *consumer) consume(until int64, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for c.checker.Next < until {
		if err := c.checker.Err(); err != nil {
			return err
		}
		if c.poll() == 0 {
			if time.Now().After(deadline) {
				return fmt.Errorf("%w waiting for seq %d: %s", e2e.ErrTimeout, until-1, c.checker)
			}
			time.Sleep(50 * time.Microsecond)
		}
	}
	return c.checker.Err()
}

// pollFor polls for d, returning the fragments seen.
func (c *consumer) pollFor(d time.Duration) int {
	n := 0
	for deadline := time.Now().Add(d); time.Now().Before(deadline); {
		if k := c.poll(); k > 0 {
			n += k
		} else {
			time.Sleep(100 * time.Microsecond)
		}
	}
	return n
}

func udp(host string, p int, params string) string {
	ch := fmt.Sprintf("aeron:udp?endpoint=%s:%d", host, p)
	if params != "" {
		ch += "|" + params
	}
	return ch
}

// blockFits reports whether a single unfragmented frame of n bytes fits in the rest of the current term, which
// OfferBlock requires (it does not pad and rotate like Offer).
func blockFits(pub *aeron.ExclusivePublication, n int) bool {
	frame := util.AlignInt32(int32(n)+logbuffer.DataFrameHeader.Length, logbuffer.FrameAlignment)
	return int32(n) <= pub.MaxPayloadLength() && pub.TermOffset() > 0 && pub.TermOffset()+frame <= pub.TermBufferLength()
}

// formatFrame lays msg out as one committed data frame at the publication's current position, as an archive
// replicator would for OfferBlock.
func formatFrame(pub *aeron.ExclusivePublication, msg []byte) []byte {
	hdr := &logbuffer.DataFrameHeader
	frame := make([]byte, util.AlignInt32(int32(len(msg))+hdr.Length, logbuffer.FrameAlignment))
	b := ab.MakeBuffer(frame)
	b.PutInt32(hdr.FrameLengthFieldOffset, int32(len(msg))+hdr.Length)
	b.PutUInt8(hdr.FlagsFieldOffset, 0xC0)
	b.PutUInt16(hdr.TypeFieldOffset, hdr.TypeData)
	b.PutInt32(hdr.TermOffsetFieldOffset, pub.TermOffset())
	b.PutInt32(hdr.SessionIDFieldOffset, pub.SessionID())
	b.PutInt32(hdr.StreamIDFieldOffset, pub.StreamID())
	b.PutInt32(hdr.TermIDFieldOffset, pub.TermID())
	copy(frame[hdr.Length:], msg)
	return frame
}
