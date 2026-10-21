package cnc

import (
	"fmt"
	"path/filepath"

	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/util"
	"github.com/lirm/aeron-go/aeron/util/memmap"
)

// LossReportFileName is the driver's loss report file inside the Aeron directory.
const LossReportFileName = "loss-report.dat"

// LossReportCSVHeader matches LossReportReader.LOSS_REPORT_CSV_HEADER.
const LossReportCSVHeader = "#OBSERVATION_COUNT,TOTAL_BYTES_LOST,FIRST_OBSERVATION,LAST_OBSERVATION,SESSION_ID,STREAM_ID,CHANNEL,SOURCE"

// Loss report entry layout (io.aeron.driver.reports.LossReport).
const (
	lossObservationCountOffset = 0
	lossTotalBytesLostOffset   = 8
	lossFirstObservationOffset = 16
	lossLastObservationOffset  = 24
	lossSessionIDOffset        = 32
	lossStreamIDOffset         = 36
	lossChannelOffset          = 40
	lossEntryAlignment         = util.CacheLineLength
)

// LossEntry is one (session, stream, channel, source) loss record.
type LossEntry struct {
	ObservationCount   int64
	TotalBytesLost     int64
	FirstObservationMs int64
	LastObservationMs  int64
	SessionID          int32
	StreamID           int32
	Channel            string
	Source             string
}

// ReadLossReport calls fn for every entry in the loss report buffer and returns the entry count.
func ReadLossReport(buffer *atomic.Buffer, fn func(LossEntry)) int {
	count := 0
	capacity := buffer.Capacity()
	for offset := int32(0); offset+lossChannelOffset+4 <= capacity; {
		observations := buffer.GetInt64Volatile(offset + lossObservationCountOffset)
		if observations <= 0 {
			break
		}
		channelLength := buffer.GetInt32(offset + lossChannelOffset)
		sourceOffset := offset + lossChannelOffset + util.AlignInt32(4+channelLength, 4)
		sourceLength := buffer.GetInt32(sourceOffset)
		if channelLength < 0 || sourceLength < 0 || sourceOffset+4+sourceLength > capacity {
			break
		}
		count++
		fn(LossEntry{
			ObservationCount:   observations,
			TotalBytesLost:     buffer.GetInt64Volatile(offset + lossTotalBytesLostOffset),
			FirstObservationMs: buffer.GetInt64(offset + lossFirstObservationOffset),
			LastObservationMs:  buffer.GetInt64Volatile(offset + lossLastObservationOffset),
			SessionID:          buffer.GetInt32(offset + lossSessionIDOffset),
			StreamID:           buffer.GetInt32(offset + lossStreamIDOffset),
			Channel:            string(buffer.GetBytesArray(offset+lossChannelOffset+4, channelLength)),
			Source:             string(buffer.GetBytesArray(sourceOffset+4, sourceLength)),
		})
		offset += util.AlignInt32(sourceOffset+4+sourceLength-offset, lossEntryAlignment)
	}
	return count
}

// LossReport is a mapped loss report file.
type LossReport struct {
	Buffer  *atomic.Buffer
	mapping *memmap.File
}

// OpenLossReport maps <aeronDir>/loss-report.dat (or path if it names a file).
func OpenLossReport(path string) (*LossReport, error) {
	if filepath.Base(path) != LossReportFileName {
		path = filepath.Join(path, LossReportFileName)
	}
	mapping, err := memmap.MapExisting(path, 0, 0)
	if err != nil {
		return nil, fmt.Errorf("mapping %s: %w", path, err)
	}
	return &LossReport{Buffer: atomic.MakeBuffer(mapping.GetMemoryPtr(), mapping.GetMemorySize()), mapping: mapping}, nil
}

// Close unmaps the file.
func (l *LossReport) Close() error { return l.mapping.Close() }
