// Copyright 2026 Kalshi Trading.
//
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

package persistentsubscription

import (
	"testing"

	"github.com/lirm/aeron-go/aeron"
	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/logbuffer"
	"github.com/lirm/aeron-go/aeron/logbuffer/term"
	"github.com/lirm/aeron-go/archive"
)

const (
	testFrameHeaderLength = 32
	testPayloadLength     = 64
)

// framesImage replays a fixed set of data frames from a term buffer.
type framesImage struct {
	*fakeImage
	buffer *atomic.Buffer
	frames []int32
	next   int
}

func newFramesImage(sessionID int32, flags []uint8) *framesImage {
	frameLength := int32(testFrameHeaderLength + testPayloadLength)
	data := make([]byte, int(frameLength)*len(flags))
	buffer := atomic.MakeBuffer(data, len(data))
	img := &framesImage{fakeImage: &fakeImage{sessionID: sessionID}, buffer: buffer}
	for i, f := range flags {
		offset := int32(i) * frameLength
		buffer.PutInt32(offset, frameLength)
		buffer.PutUInt8(offset+4, 0)
		buffer.PutUInt8(offset+5, f)
		buffer.PutUInt16(offset+6, 1)
		buffer.PutInt32(offset+8, offset)
		buffer.PutInt32(offset+12, sessionID)
		buffer.PutInt32(offset+16, testStreamID)
		buffer.PutInt32(offset+20, 0)
		for j := int32(0); j < testPayloadLength; j++ {
			buffer.PutUInt8(offset+testFrameHeaderLength+j, uint8(i))
		}
		img.frames = append(img.frames, offset)
	}
	return img
}

func (i *framesImage) header(offset int32) *logbuffer.Header {
	h := new(logbuffer.Header)
	h.SetBuffer(i.buffer).SetOffset(offset).SetInitialTermID(0).SetPositionBitsToShift(16)
	return h
}

func (i *framesImage) ControlledPoll(handler term.ControlledFragmentHandler, limit int) int {
	n := 0
	for i.next < len(i.frames) && n < limit {
		offset := i.frames[i.next]
		action := handler(i.buffer, offset+testFrameHeaderLength, testPayloadLength, i.header(offset))
		if action == term.ControlledPollActionAbort {
			break
		}
		i.next++
		n++
		i.position = int64(offset + testFrameHeaderLength + testPayloadLength)
	}
	return n
}

func (i *framesImage) Poll(handler term.FragmentHandler, limit int) int {
	return i.ControlledPoll(func(b *atomic.Buffer, o, l int32, h *logbuffer.Header) term.ControlledPollAction {
		handler(b, o, l, h)
		return term.ControlledPollActionContinue
	}, limit)
}

type deliveredMessage struct {
	length      int32
	flags       uint8
	frameLength int32
	termOffset  int32
	position    int64
}

// Java's ImageFragmentAssembler/ImageControlledFragmentAssembler deliver a reassembled message with
// BufferBuilder.completeHeader: BEGIN|END flags, the frame length of the whole message and the term offset of the
// first fragment, while Position() is the position after the last fragment.
func TestFragmentedMessageDeliveredWithCompleteHeader(t *testing.T) {
	for _, controlled := range []bool{true, false} {
		h := newHarness(t, FromStart, "aeron:ipc")
		var got []deliveredMessage
		record := func(_ *atomic.Buffer, _ int32, length int32, header *logbuffer.Header) {
			got = append(got, deliveredMessage{length: length, flags: header.Flags(),
				frameLength: header.FrameLength(), termOffset: header.TermOffset(), position: header.Position()})
		}
		h.ps.uncontrolledFragmentHandler = record
		h.ps.controlledFragmentHandler = func(b *atomic.Buffer, o, l int32, hd *logbuffer.Header) term.ControlledPollAction {
			record(b, o, l, hd)
			return term.ControlledPollActionContinue
		}
		img := newFramesImage(5, []uint8{beginFragFlag, 0, endFragFlag})
		h.ps.doPoll(img, 10, controlled)
		if len(got) != 1 {
			t.Fatalf("controlled=%v: expected 1 message, got %+v", controlled, got)
		}
		m := got[0]
		if m.length != 3*testPayloadLength || m.flags != beginFragFlag|endFragFlag ||
			m.frameLength != testFrameHeaderLength+3*testPayloadLength || m.termOffset != 0 ||
			m.position != 3*(testFrameHeaderLength+testPayloadLength) {
			t.Fatalf("controlled=%v: unexpected header %+v", controlled, m)
		}
	}
}

const (
	beginFragFlag uint8 = 0x80
	endFragFlag   uint8 = 0x40
)

type notConnectedPublisher struct{ aeron.Publisher }

func (notConnectedPublisher) Offer(*atomic.Buffer, int32, int32, term.ReservedValueSupplier) int64 {
	return aeron.NotConnected
}

type stubRequestPublication struct{ pub aeron.Publisher }

func (p stubRequestPublication) isConnected() bool          { return false }
func (p stubRequestPublication) publisher() aeron.Publisher { return p.pub }

// Java AsyncAeronArchive.trySendReplayRequest(archiveProxy, ...) moves to DISCONNECTED when the response-channel
// request publication throws (NOT_CONNECTED / CLOSED / MAX_POSITION_EXCEEDED), triggering archive reconnect.
func TestResponseChannelRequestPublicationNotConnectedDisconnects(t *testing.T) {
	h := newHarness(t, FromStart, "aeron:ipc")
	a := newAsyncArchive(archive.DefaultOptions(), aeron.NewContext, h.ps, func() int64 { return 0 })
	a.archive = &archive.Archive{Options: archive.DefaultOptions(), SessionID: 1, Proxy: &archive.Proxy{}}
	a.state = asyncArchiveConnected
	if a.trySendReplayRequestOn(stubRequestPublication{notConnectedPublisher{}}, 1, testRecordingID, 0,
		replayAllAndFollow, "aeron:udp?control-mode=response", 2002, 3) {
		t.Fatal("expected send to fail")
	}
	if a.state != asyncArchiveDisconnected {
		t.Fatalf("expected disconnected, got %v", a.state)
	}
}

// Java Context.conclude sets aeronArchiveContext.aeron(aeron) so the archive control session shares the
// persistent subscription's Aeron client instead of connecting a new client for every archive connection.
func TestArchiveSharesAeronClient(t *testing.T) {
	ctx := NewContext()
	ctx.Aeron = &aeron.Aeron{}
	ctx.ArchiveOptions = archive.DefaultOptions()
	opts := ctx.archiveConnectOptions()
	if opts.Aeron != ctx.Aeron || opts.OwnsAeronClient {
		t.Fatalf("archive must share the persistent subscription aeron client: %+v", opts)
	}
	if ctx.ArchiveOptions.Aeron != nil {
		t.Fatal("caller supplied archive options must not be mutated")
	}

	ctx.ArchiveAeronContext = aeron.NewContext()
	if opts := ctx.archiveConnectOptions(); opts.Aeron != nil {
		t.Fatal("explicit ArchiveAeronContext must keep a dedicated archive aeron client")
	}

	own := &aeron.Aeron{}
	ctx.ArchiveAeronContext = nil
	ctx.ArchiveOptions.Aeron = own
	if opts := ctx.archiveConnectOptions(); opts.Aeron != own {
		t.Fatal("explicit ArchiveOptions.Aeron must be kept")
	}
}
