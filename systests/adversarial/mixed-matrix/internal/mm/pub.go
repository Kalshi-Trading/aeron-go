package mm

import (
	"fmt"

	"github.com/lirm/aeron-go/aeron"
	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/logbuffer"
	"github.com/lirm/aeron-go/aeron/protocol"
	"github.com/lirm/aeron-go/aeron/util"
)

// Go publisher kinds. Messages longer than MaxPayloadLength cannot be claimed or written as a single block frame,
// so the tryclaim and offerblock kinds send those with Offer (fragmented) and everything else their own way.
const (
	PubConcurrent = "concurrent" // Publication.Offer
	PubExclusive  = "exclusive"  // ExclusivePublication.Offer
	PubTryClaim   = "tryclaim"   // Publication.TryClaim + Commit
	PubOfferBlock = "offerblock" // ExclusivePublication.OfferBlock of up to 4 pre-framed messages
)

// Pub is a Go publisher of one kind writing the mm wire format.
type Pub struct {
	Kind   string
	Writer uint8
	Sizes  []int
	Pub    aeron.Publisher
	excl   *aeron.ExclusivePublication
	conc   *aeron.Publication
	seq    uint32
	buf    *atomic.Buffer
	block  *atomic.Buffer
	claim  logbuffer.Claim
}

// AddPub creates a publication of the given kind.
func AddPub(a *aeron.Aeron, kind, channel string, stream int32, writer uint8, sizes []int) (*Pub, error) {
	p := &Pub{Kind: kind, Writer: writer, Sizes: sizes}
	var err error
	switch kind {
	case PubConcurrent, PubTryClaim:
		p.conc, err = a.AddPublication(channel, stream)
		p.Pub = p.conc
	case PubExclusive, PubOfferBlock:
		p.excl, err = a.AddExclusivePublication(channel, stream)
		p.Pub = p.excl
	default:
		return nil, fmt.Errorf("unknown publisher kind %q", kind)
	}
	if err != nil {
		return nil, err
	}
	max := 1
	for _, s := range sizes {
		if s > max {
			max = s
		}
	}
	p.buf = atomic.MakeBuffer(make([]byte, max))
	p.block = atomic.MakeBuffer(make([]byte, 4*(int(p.Pub.MaxPayloadLength())+int(protocol.DataHeaderLength))))
	return p, nil
}

// Next is the sequence of the next message to be sent.
func (p *Pub) Next() uint32 { return p.seq }

// Close closes the publication.
func (p *Pub) Close() error { return p.Pub.Close() }

func (p *Pub) size(seq uint32) int { return p.Sizes[int(seq)%len(p.Sizes)] }

// Retryable reports whether an offer result is a transient back pressure status.
func Retryable(result int64) bool {
	return result == aeron.NotConnected || result == aeron.BackPressured || result == aeron.AdminAction
}

// Step makes one send attempt. It returns the number of messages sent (0 on a transient status) and an error for
// PublicationClosed / MaxPositionExceeded.
func (p *Pub) Step() (int, error) {
	length := p.size(p.seq)
	maxPayload := int(p.Pub.MaxPayloadLength())
	var result int64
	sent := 1
	switch {
	case p.Kind == PubTryClaim && length <= maxPayload:
		result = p.conc.TryClaim(int32(length), &p.claim)
		if result > 0 {
			payload := make([]byte, length)
			Fill(payload, p.Writer, p.seq)
			p.claim.Buffer().PutBytesArray(p.claim.Offset(), &payload, 0, int32(length))
			p.claim.PutReservedValue(ReservedValue(payload, p.Writer, p.seq))
			p.claim.Commit()
		}
	case p.Kind == PubOfferBlock && length <= maxPayload:
		result, sent = p.offerBlock(maxPayload)
	default:
		payload := p.buf.GetBytesArray(0, int32(length))
		Fill(payload, p.Writer, p.seq)
		p.buf.PutBytesArray(0, &payload, 0, int32(length))
		rv := ReservedValue(payload, p.Writer, p.seq)
		result = p.Pub.Offer(p.buf, 0, int32(length), func(*atomic.Buffer, int32, int32) int64 { return rv })
	}
	if result > 0 {
		p.seq = (p.seq + uint32(sent)) & SeqMask
		return sent, nil
	}
	if Retryable(result) {
		return 0, nil
	}
	return 0, fmt.Errorf("%s publisher writer %d: offer result %d", p.Kind, p.Writer, result)
}

// offerBlock frames consecutive messages that fit in the rest of the term and offers them as one block; when not
// even the first one fits it falls back to Offer, which pads and rotates the term.
func (p *Pub) offerBlock(maxPayload int) (int64, int) {
	pub := p.excl
	termRemaining := int(pub.TermBufferLength() - pub.TermOffset())
	offset := 0
	n := 0
	for seq := p.seq; n < 4; seq = (seq + 1) & SeqMask {
		length := p.size(seq)
		aligned := int(util.AlignInt32(int32(length)+protocol.DataHeaderLength, logbuffer.FrameAlignment))
		if length > maxPayload || offset+aligned > termRemaining {
			break
		}
		hdr := int32(offset)
		p.block.PutInt32(hdr+protocol.FrameLengthFieldOffset, int32(length)+protocol.DataHeaderLength)
		p.block.PutUInt8(hdr+protocol.VersionFieldOffset, protocol.CurrentVersion)
		p.block.PutUInt8(hdr+protocol.FlagsFieldOffset, protocol.BeginAndEndFlags)
		p.block.PutUInt16(hdr+protocol.TypeFieldOffset, protocol.HdrTypeData)
		p.block.PutInt32(hdr+protocol.DataTermOffsetFieldOffset, pub.TermOffset()+int32(offset))
		p.block.PutInt32(hdr+protocol.DataSessionIDFieldOffset, pub.SessionID())
		p.block.PutInt32(hdr+protocol.DataStreamIDFieldOffset, pub.StreamID())
		p.block.PutInt32(hdr+protocol.DataTermIDFieldOffset, pub.TermID())
		payload := make([]byte, length)
		Fill(payload, p.Writer, seq)
		p.block.PutInt64(hdr+protocol.DataReservedValueOffset, ReservedValue(payload, p.Writer, seq))
		if length > 0 {
			p.block.PutBytesArray(hdr+protocol.DataHeaderLength, &payload, 0, int32(length))
		}
		// zero the alignment padding so the block is fully initialised
		for i := offset + int(protocol.DataHeaderLength) + length; i < offset+aligned; i++ {
			p.block.PutUInt8(int32(i), 0)
		}
		offset += aligned
		n++
	}
	if n == 0 {
		length := p.size(p.seq)
		payload := p.buf.GetBytesArray(0, int32(length))
		Fill(payload, p.Writer, p.seq)
		p.buf.PutBytesArray(0, &payload, 0, int32(length))
		rv := ReservedValue(payload, p.Writer, p.seq)
		return pub.Offer(p.buf, 0, int32(length), func(*atomic.Buffer, int32, int32) int64 { return rv }), 1
	}
	return pub.OfferBlock(p.block, 0, int32(offset)), n
}
