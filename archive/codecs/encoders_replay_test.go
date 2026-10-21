package codecs

import (
	"bytes"
	"testing"
)

func decodeHeader(t *testing.T, m *SbeGoMarshaller, packet []byte) (*bytes.Reader, MessageHeader) {
	t.Helper()
	r := bytes.NewReader(packet)
	var hdr MessageHeader
	if err := hdr.Decode(m, r, 0); err != nil {
		t.Fatalf("header decode: %v", err)
	}
	return r, hdr
}

func TestReplayRequestWithParamsPacket(t *testing.T) {
	m := NewSbeGoMarshaller()
	packet, err := ReplayRequestWithParamsPacket(m, true, 1, 2, 3, 4, 5, 6, 4096, 77, "aeron:udp?endpoint=localhost:1")
	if err != nil {
		t.Fatal(err)
	}
	r, hdr := decodeHeader(t, m, packet)
	var req ReplayRequest
	if hdr.TemplateId != req.SbeTemplateId() || hdr.SchemaId != req.SbeSchemaId() {
		t.Fatalf("unexpected header %+v", hdr)
	}
	if err := req.Decode(m, r, hdr.Version, hdr.BlockLength, true); err != nil {
		t.Fatal(err)
	}
	if req.ControlSessionId != 1 || req.CorrelationId != 2 || req.RecordingId != 3 || req.Position != 4 ||
		req.Length != 5 || req.ReplayStreamId != 6 || req.FileIoMaxLength != 4096 || req.ReplayToken != 77 ||
		string(req.ReplayChannel) != "aeron:udp?endpoint=localhost:1" {
		t.Fatalf("unexpected decode %+v", req)
	}
}

func TestLegacyReplayPacketsUseNullOptionals(t *testing.T) {
	m := NewSbeGoMarshaller()
	packet, err := ReplayRequestPacket(m, true, 1, 2, 3, 4, 5, 6, "aeron:ipc")
	if err != nil {
		t.Fatal(err)
	}
	r, hdr := decodeHeader(t, m, packet)
	var req ReplayRequest
	if err := req.Decode(m, r, hdr.Version, hdr.BlockLength, true); err != nil {
		t.Fatal(err)
	}
	if req.FileIoMaxLength != NullValue || req.ReplayToken != NullValue {
		t.Fatalf("expected null optionals, got %+v", req)
	}

	packet, err = BoundedReplayPacket(m, true, 1, 2, 3, 4, 5, 9, 6, "aeron:ipc")
	if err != nil {
		t.Fatal(err)
	}
	r, hdr = decodeHeader(t, m, packet)
	var breq BoundedReplayRequest
	if hdr.TemplateId != breq.SbeTemplateId() {
		t.Fatalf("unexpected template %d", hdr.TemplateId)
	}
	if err := breq.Decode(m, r, hdr.Version, hdr.BlockLength, true); err != nil {
		t.Fatal(err)
	}
	if breq.LimitCounterId != 9 || breq.FileIoMaxLength != NullValue || breq.ReplayToken != NullValue {
		t.Fatalf("unexpected decode %+v", breq)
	}
}

func TestBoundedReplayWithParamsPacket(t *testing.T) {
	m := NewSbeGoMarshaller()
	packet, err := BoundedReplayWithParamsPacket(m, true, 1, 2, 3, 4, 5, 9, 6, 8192, 42, "aeron:ipc")
	if err != nil {
		t.Fatal(err)
	}
	r, hdr := decodeHeader(t, m, packet)
	var req BoundedReplayRequest
	if err := req.Decode(m, r, hdr.Version, hdr.BlockLength, true); err != nil {
		t.Fatal(err)
	}
	if req.RecordingId != 3 || req.LimitCounterId != 9 || req.ReplayStreamId != 6 || req.FileIoMaxLength != 8192 ||
		req.ReplayToken != 42 || string(req.ReplayChannel) != "aeron:ipc" {
		t.Fatalf("unexpected decode %+v", req)
	}
}

func TestSimpleRequestPackets(t *testing.T) {
	m := NewSbeGoMarshaller()

	packet, err := ReplayTokenRequestPacket(m, true, 11, 12, 13)
	if err != nil {
		t.Fatal(err)
	}
	r, hdr := decodeHeader(t, m, packet)
	var token ReplayTokenRequest
	if hdr.TemplateId != token.SbeTemplateId() {
		t.Fatalf("unexpected template %d", hdr.TemplateId)
	}
	if err := token.Decode(m, r, hdr.Version, hdr.BlockLength, true); err != nil || token.ControlSessionId != 11 || token.CorrelationId != 12 || token.RecordingId != 13 {
		t.Fatalf("ReplayTokenRequest %+v %v", token, err)
	}

	packet, err = MaxRecordedPositionRequestPacket(m, true, 21, 22, 23)
	if err != nil {
		t.Fatal(err)
	}
	r, hdr = decodeHeader(t, m, packet)
	var maxPos MaxRecordedPositionRequest
	if hdr.TemplateId != maxPos.SbeTemplateId() {
		t.Fatalf("unexpected template %d", hdr.TemplateId)
	}
	if err := maxPos.Decode(m, r, hdr.Version, hdr.BlockLength, true); err != nil || maxPos.ControlSessionId != 21 || maxPos.CorrelationId != 22 || maxPos.RecordingId != 23 {
		t.Fatalf("MaxRecordedPositionRequest %+v %v", maxPos, err)
	}

	packet, err = ArchiveIdRequestPacket(m, true, 31, 32)
	if err != nil {
		t.Fatal(err)
	}
	r, hdr = decodeHeader(t, m, packet)
	var archiveID ArchiveIdRequest
	if hdr.TemplateId != archiveID.SbeTemplateId() {
		t.Fatalf("unexpected template %d", hdr.TemplateId)
	}
	if err := archiveID.Decode(m, r, hdr.Version, hdr.BlockLength, true); err != nil || archiveID.ControlSessionId != 31 || archiveID.CorrelationId != 32 {
		t.Fatalf("ArchiveIdRequest %+v %v", archiveID, err)
	}
}
