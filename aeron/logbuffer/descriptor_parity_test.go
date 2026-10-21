// Copyright 2026 Kalshi Trading
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

package logbuffer

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/protocol"
	"github.com/lirm/aeron-go/aeron/util/memmap"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newMeta() *LogBufferMetaData {
	var m LogBufferMetaData
	m.Wrap(atomic.MakeBuffer(make([]byte, LogMetaDataLength)), 0)
	return &m
}

// Offsets as computed by io.aeron.logbuffer.LogBufferDescriptor 1.53.3.
func TestMetaDataOffsetsMatchJava(t *testing.T) {
	expected := map[string][2]int32{
		"TERM_TAIL_COUNTERS":                 {TermTailCountersOffset, 0},
		"ACTIVE_TERM_COUNT":                  {LogActiveTermCountOffset, 24},
		"END_OF_STREAM_POSITION":             {LogEndOfStreamPositionOffset, 128},
		"IS_CONNECTED":                       {LogIsConnectedOffset, 136},
		"ACTIVE_TRANSPORT_COUNT":             {LogActiveTransportCount, 140},
		"CORRELATION_ID":                     {LogCorrelationIDOffset, 256},
		"INITIAL_TERM_ID":                    {LogInitialTermIDOffset, 264},
		"DEFAULT_FRAME_HEADER_LENGTH":        {LogDefaultFrameHeaderLengthOffset, 268},
		"MTU_LENGTH":                         {LogMtuLengthOffset, 272},
		"TERM_LENGTH":                        {LogTermLengthOffset, 276},
		"PAGE_SIZE":                          {LogPageSizeOffset, 280},
		"PUBLICATION_WINDOW_LENGTH":          {LogPublicationWindowLengthOffset, 284},
		"RECEIVER_WINDOW_LENGTH":             {LogReceiverWindowLengthOffset, 288},
		"SOCKET_SNDBUF_LENGTH":               {LogSocketSndbufLengthOffset, 292},
		"OS_DEFAULT_SOCKET_SNDBUF_LENGTH":    {LogOsDefaultSocketSndbufLengthOffset, 296},
		"OS_MAX_SOCKET_SNDBUF_LENGTH":        {LogOsMaxSocketSndbufLengthOffset, 300},
		"SOCKET_RCVBUF_LENGTH":               {LogSocketRcvbufLengthOffset, 304},
		"OS_DEFAULT_SOCKET_RCVBUF_LENGTH":    {LogOsDefaultSocketRcvbufLengthOffset, 308},
		"OS_MAX_SOCKET_RCVBUF_LENGTH":        {LogOsMaxSocketRcvbufLengthOffset, 312},
		"MAX_RESEND":                         {LogMaxResendOffset, 316},
		"DEFAULT_FRAME_HEADER":               {LogDefaultFrameHeaderOffset, 320},
		"ENTITY_TAG":                         {LogEntityTagOffset, 448},
		"RESPONSE_CORRELATION_ID":            {LogResponseCorrelationIDOffset, 456},
		"LINGER_TIMEOUT_NS":                  {LogLingerTimeoutNsOffset, 464},
		"UNTETHERED_WINDOW_LIMIT_TIMEOUT_NS": {LogUntetheredWindowLimitTimeoutNsOffset, 472},
		"UNTETHERED_RESTING_TIMEOUT_NS":      {LogUntetheredRestingTimeoutNsOffset, 480},
		"GROUP":                              {LogGroupOffset, 488},
		"IS_RESPONSE":                        {LogIsResponseOffset, 489},
		"REJOIN":                             {LogRejoinOffset, 490},
		"RELIABLE":                           {LogReliableOffset, 491},
		"SPARSE":                             {LogSparseOffset, 492},
		"SIGNAL_EOS":                         {LogSignalEosOffset, 493},
		"SPIES_SIMULATE_CONNECTION":          {LogSpiesSimulateConnectionOffset, 494},
		"TETHER":                             {LogTetherOffset, 495},
		"IS_PUBLICATION_REVOKED":             {LogIsPublicationRevokedOffset, 496},
		"TYPE":                               {LogTypeOffset, 497},
		"UNTETHERED_LINGER_TIMEOUT_NS":       {LogUntetheredLingerTimeoutNsOffset, 500},
	}
	for name, v := range expected {
		assert.Equalf(t, v[1], v[0], "LOG_%s_OFFSET", name)
	}
	assert.Equal(t, int32(4096), LogMetaDataLength)
	assert.Equal(t, int32(128), LogDefaultFrameHeaderMaxLength)
}

func TestLegacyFlyweightFieldsAliasJavaOffsets(t *testing.T) {
	m := newMeta()
	m.SetCorrelationID(42)
	m.SetInitialTermID(-7)
	m.SetMtuLength(1408)
	m.SetTermLength(TermMinLength)
	m.SetPageSize(PageMinSize)
	m.SetIsConnected(true)
	m.SetActiveTransportCount(2)
	m.SetEndOfStreamPosition(99)
	m.SetRawTail(1, PackTail(3, 64))
	assert.True(t, m.CasActiveTermCount(0, 5))

	assert.Equal(t, int64(42), m.CorrelationId.Get())
	assert.Equal(t, int32(-7), m.InitTermID.Get())
	assert.Equal(t, int32(1408), m.MTULen.Get())
	assert.Equal(t, TermMinLength, m.TermLen.Get())
	assert.Equal(t, PageMinSize, m.PageSizeFld.Get())
	assert.Equal(t, int32(1), m.IsConnectedFld.Get())
	assert.Equal(t, int32(2), m.ActiveTransportCount())
	assert.Equal(t, int64(99), m.EndOfStreamPosOff.Get())
	assert.Equal(t, PackTail(3, 64), m.TailCounter[1].Get())
	assert.Equal(t, int32(5), m.ActiveTermCountOff.Get())
	assert.Equal(t, int(LogMetaDataLength), m.Size())
}

func TestMetaDataAccessorsRoundTrip(t *testing.T) {
	m := newMeta()
	m.SetPublicationWindowLength(1)
	m.SetReceiverWindowLength(2)
	m.SetSocketSndbufLength(3)
	m.SetOsDefaultSocketSndbufLength(4)
	m.SetOsMaxSocketSndbufLength(5)
	m.SetSocketRcvbufLength(6)
	m.SetOsDefaultSocketRcvbufLength(7)
	m.SetOsMaxSocketRcvbufLength(8)
	m.SetMaxResend(9)
	m.SetEntityTag(10)
	m.SetResponseCorrelationID(11)
	m.SetLingerTimeoutNs(12)
	m.SetUntetheredWindowLimitTimeoutNs(13)
	m.SetUntetheredRestingTimeoutNs(14)
	m.SetUntetheredLingerTimeoutNs(-15)
	m.SetGroup(true)
	m.SetRejoin(true)
	m.SetSparse(true)
	m.SetTether(true)
	m.SetIsPublicationRevoked(true)
	m.SetLogType(LogBufferTypePublicationImage)

	assert.Equal(t, int32(1), m.PublicationWindowLength())
	assert.Equal(t, int32(2), m.ReceiverWindowLength())
	assert.Equal(t, int32(3), m.SocketSndbufLength())
	assert.Equal(t, int32(4), m.OsDefaultSocketSndbufLength())
	assert.Equal(t, int32(5), m.OsMaxSocketSndbufLength())
	assert.Equal(t, int32(6), m.SocketRcvbufLength())
	assert.Equal(t, int32(7), m.OsDefaultSocketRcvbufLength())
	assert.Equal(t, int32(8), m.OsMaxSocketRcvbufLength())
	assert.Equal(t, int32(9), m.MaxResend())
	assert.Equal(t, int64(10), m.EntityTag())
	assert.Equal(t, int64(11), m.ResponseCorrelationID())
	assert.Equal(t, int64(12), m.LingerTimeoutNs())
	assert.Equal(t, int64(13), m.UntetheredWindowLimitTimeoutNs())
	assert.Equal(t, int64(14), m.UntetheredRestingTimeoutNs())
	assert.Equal(t, int64(-15), m.UntetheredLingerTimeoutNs())
	assert.True(t, m.Group())
	assert.False(t, m.IsResponse())
	assert.True(t, m.Rejoin())
	assert.False(t, m.Reliable())
	assert.True(t, m.Sparse())
	assert.False(t, m.SignalEos())
	assert.False(t, m.SpiesSimulateConnection())
	assert.True(t, m.Tether())
	assert.True(t, m.IsPublicationRevoked())
	assert.Equal(t, LogBufferTypePublicationImage, m.LogType())
	// neighbouring byte fields must not be clobbered
	assert.Equal(t, uint8(1), m.Buffer().GetUInt8(LogGroupOffset))
	assert.Equal(t, uint8(0), m.Buffer().GetUInt8(LogIsResponseOffset))
}

func TestDefaultFrameHeaderStoreAndApply(t *testing.T) {
	m := newMeta()
	assert.Error(t, m.StoreDefaultFrameHeader(atomic.MakeBuffer(make([]byte, 16))))
	hdr := protocol.CreateDefaultHeader(7, 1001, 33)
	require.NoError(t, m.StoreDefaultFrameHeader(hdr))
	assert.Equal(t, DataFrameHeader.Length, m.DefaultFrameHeaderLength())
	assert.Equal(t, hdr.GetBytesArray(0, 32), m.DefaultFrameHeaderBuffer().GetBytesArray(0, 32))

	term := atomic.MakeBuffer(make([]byte, 256))
	m.ApplyDefaultHeader(term, 64)
	var d protocol.DataHeaderFlyweight
	d.Wrap(term, 64)
	assert.Equal(t, int32(7), d.SessionID())
	assert.Equal(t, int32(1001), d.StreamID())
	assert.Equal(t, int32(33), d.TermID())
	assert.Equal(t, protocol.HdrTypeData, d.HeaderType())
	assert.Equal(t, protocol.BeginAndEndFlags, d.Flags())
}

// Ported from LogBufferDescriptorTest.
func TestRotateLogShouldCasActiveTermCountEvenWhenTermIdDoesNotMatch(t *testing.T) {
	m := newMeta()
	termID, termCount := int32(5), int32(1)
	m.SetRawTail(2, PackTail(termID, 1024))
	m.SetRawTail(0, PackTail(termID+1, 2048))
	m.SetRawTail(1, PackTail(termID+2, 4096))
	m.SetActiveTermCountOrdered(termCount)

	assert.True(t, RotateLog(m, termCount, termID))
	assert.Equal(t, PackTail(termID, 1024), m.RawTail(2))
	assert.Equal(t, PackTail(termID+1, 2048), m.RawTail(0))
	assert.Equal(t, PackTail(termID+2, 4096), m.RawTail(1))
	assert.Equal(t, termCount+1, m.ActiveTermCount())
}

func TestRotateLogShouldCasActiveTermCountAfterSettingTailForTheNextTerm(t *testing.T) {
	m := newMeta()
	termID, termCount := int32(51), int32(19)
	m.SetRawTail(1, PackTail(termID, 1024))
	m.SetRawTail(2, PackTail(termID+1-PartitionCount, 2048))
	m.SetRawTail(0, PackTail(termID+2-PartitionCount, 4096))
	m.SetActiveTermCountOrdered(termCount)

	assert.True(t, RotateLog(m, termCount, termID))
	assert.Equal(t, PackTail(termID, 1024), m.RawTail(1))
	assert.Equal(t, PackTail(termID+1, 0), m.RawTail(2))
	assert.Equal(t, PackTail(termID+2-PartitionCount, 4096), m.RawTail(0))
	assert.Equal(t, termCount+1, m.ActiveTermCount())
}

func TestRotateLogIsANoOpIfNeitherTailNorActiveTermCountCanBeChanged(t *testing.T) {
	m := newMeta()
	termID, termCount := int32(23), int32(42)
	m.SetRawTail(0, PackTail(termID, 1024))
	m.SetRawTail(1, PackTail(termID+18, 2048))
	m.SetRawTail(2, PackTail(termID-19, 4096))
	m.SetActiveTermCountOrdered(termCount)

	assert.False(t, RotateLog(m, 3, termID))
	assert.Equal(t, PackTail(termID, 1024), m.RawTail(0))
	assert.Equal(t, PackTail(termID+18, 2048), m.RawTail(1))
	assert.Equal(t, PackTail(termID-19, 4096), m.RawTail(2))
	assert.Equal(t, termCount, m.ActiveTermCount())
}

func TestComputeFragmentedFrameLength(t *testing.T) {
	for _, c := range [][3]int32{{0, 1376, 0}, {10, 1024, 64}, {2048, 2048, 2080}, {4096, 1024, 4224}, {7997, 992, 8288}} {
		assert.Equalf(t, c[2], ComputeFragmentedFrameLength(c[0], c[1]), "%v", c)
	}
	assert.Equal(t, int32(32+7997), ComputeAssembledFrameLength(7997, 992))
}

func TestPositionHelpers(t *testing.T) {
	shift := PositionBitsToShift(TermMinLength)
	assert.Equal(t, int32(16), shift)
	assert.Equal(t, int32(30), PositionBitsToShift(TermMaxLength))
	assert.Panics(t, func() { PositionBitsToShift(TermMinLength + 1) })

	initialTermID := int32(-5)
	pos := ComputePosition(initialTermID+3, 128, shift, initialTermID)
	assert.Equal(t, int64(3)<<16+128, pos)
	assert.Equal(t, int64(3)<<16, ComputeTermBeginPosition(initialTermID+3, shift, initialTermID))
	assert.Equal(t, initialTermID+3, ComputeTermIDFromPosition(pos, shift, initialTermID))
	assert.Equal(t, int32(128), ComputeTermOffsetFromPosition(pos, shift))
	assert.Equal(t, int32(0), IndexByPosition(pos, shift))
	assert.Equal(t, int32(0), IndexByTerm(initialTermID, initialTermID+3))
	assert.Equal(t, int32(1), IndexByTermCount(4))
	assert.Equal(t, int32(0), NextPartitionIndex(2))
	assert.Equal(t, int32(2), PreviousPartitionIndex(0))

	// term id wrap around
	wrapInitial := int32(2147483647)
	assert.Equal(t, int64(1)<<16, ComputeTermBeginPosition(wrapInitial+1, shift, wrapInitial))

	assert.Equal(t, int32(7), TermID(PackTail(7, 99)))
	assert.Equal(t, int32(99), TermOffset(PackTail(7, 99), 1024))
	assert.Equal(t, int32(1024), TermOffset(PackTail(7, 5000), 1024))

	assert.Equal(t, int64(3*65536+4096), ComputeLogLength(TermMinLength, PageMinSize))
	assert.Equal(t, int64(2*1024*1024), ComputeLogLength(TermMinLength, 2*1024*1024))
	assert.Equal(t, int64(3)*1024*1024*1024+4096, ComputeLogLength(TermMaxLength, PageMinSize))
	assert.Equal(t, int32(1024*1024), ComputeMaxMessageLength(8*1024*1024))
	assert.Equal(t, MaxMessageLength, ComputeMaxMessageLength(TermMaxLength))
}

func TestCheckTermLengthAndPageSize(t *testing.T) {
	assert.NoError(t, CheckTermLength(TermMinLength))
	assert.Error(t, CheckTermLength(TermMinLength/2))
	assert.Error(t, CheckTermLength(-TermMinLength))
	assert.Error(t, CheckTermLength(TermMinLength+32))
	assert.NoError(t, CheckPageSize(PageMinSize))
	assert.Error(t, CheckPageSize(1024))
	assert.Error(t, CheckPageSize(PageMinSize+1))
}

func writeLogFile(t *testing.T, length int64, termLength, pageSize int32) string {
	name := filepath.Join(t.TempDir(), "test.logbuffer")
	mmap, err := memmap.NewFile(name, 0, int(length))
	require.NoError(t, err)
	if length >= int64(LogMetaDataLength) {
		buf := atomic.MakeBuffer(mmap.GetMemoryPtr(), int32(length))
		var m LogBufferMetaData
		m.Wrap(buf, int(length-int64(LogMetaDataLength)))
		m.SetTermLength(termLength)
		m.SetPageSize(pageSize)
		m.SetInitialTermID(77)
	}
	require.NoError(t, mmap.Close())
	return name
}

func TestNewLogBuffersValidation(t *testing.T) {
	good := ComputeLogLength(TermMinLength, PageMinSize)
	lb, err := NewLogBuffers(writeLogFile(t, good, TermMinLength, PageMinSize))
	require.NoError(t, err)
	assert.Equal(t, TermMinLength, lb.TermLength())
	assert.Equal(t, int32(77), lb.Meta().InitialTermID())
	assert.Len(t, lb.TermBuffers(), PartitionCount)
	for _, tb := range lb.TermBuffers() {
		assert.Equal(t, TermMinLength, tb.Capacity())
	}
	assert.Equal(t, LogMetaDataLength, lb.MetaDataBuffer().Capacity())
	lb.PreTouch()
	require.NoError(t, lb.Close())

	_, err = NewLogBuffers(writeLogFile(t, good+4096, TermMinLength, PageMinSize))
	assert.ErrorContains(t, err, "does not match expected")
	_, err = NewLogBuffers(writeLogFile(t, good, 1024, PageMinSize))
	assert.ErrorContains(t, err, "term length")
	_, err = NewLogBuffers(writeLogFile(t, good, TermMinLength, 1000))
	assert.ErrorContains(t, err, "page size")
	_, err = NewLogBuffers(writeLogFile(t, 1024, TermMinLength, PageMinSize))
	assert.ErrorContains(t, err, "less than min length")
	_, err = NewLogBuffers(filepath.Join(t.TempDir(), "missing"))
	assert.Error(t, err)
	assert.Panics(t, func() { Wrap(writeLogFile(t, good+4096, TermMinLength, PageMinSize)) })
	_ = os.Remove
}
