// Package cnc reads the driver's CnC file, distinct error log, loss report and stream counters for the
// stat sample tools (Go equivalents of io.aeron.samples.CncFileReader, ErrorStat, LossStat,
// BacklogStat and AeronStat). It only reads; it never writes driver files.
package cnc

import (
	"fmt"
	"path/filepath"
	"time"

	"github.com/lirm/aeron-go/aeron/counters"
	rb "github.com/lirm/aeron-go/aeron/ringbuffer"
	"github.com/lirm/aeron-go/aeron/util/memmap"
)

// File is a mapped CnC file.
type File struct {
	Meta     *counters.MetaDataFlyweight
	Counters *counters.Reader
	mapping  *memmap.File
	toDriver rb.ManyToOne
}

// FileName returns the CnC file path inside an Aeron directory.
func FileName(aeronDir string) string { return filepath.Join(aeronDir, "cnc.dat") }

// Open maps <aeronDir>/cnc.dat.
func Open(aeronDir string) (*File, error) {
	meta, mapping, err := counters.MapFile(FileName(aeronDir))
	if err != nil {
		return nil, fmt.Errorf("mapping %s: %w", FileName(aeronDir), err)
	}
	f := &File{Meta: meta, mapping: mapping,
		Counters: counters.NewReader(meta.ValuesBuf.Get(), meta.MetaDataBuf.Get())}
	f.toDriver.Init(meta.ToDriverBuf.Get())
	return f, nil
}

// Close unmaps the file.
func (f *File) Close() error { return f.mapping.Close() }

// SemanticVersion returns the CnC version as major.minor.patch.
func (f *File) SemanticVersion() string { return SemanticVersionString(f.Meta.CncVersion.Get()) }

// DriverPid returns the media driver's process id.
func (f *File) DriverPid() int64 { return f.Meta.DriverPid.Get() }

// DriverHeartbeatAgeMs returns how long ago the driver last consumed its command buffer.
func (f *File) DriverHeartbeatAgeMs(now time.Time) int64 {
	return now.UnixMilli() - f.toDriver.ConsumerHeartbeatTime()
}

// SemanticVersionString formats an Aeron semantic version int (major<<16 | minor<<8 | patch).
func SemanticVersionString(v int32) string {
	return fmt.Sprintf("%d.%d.%d", (v>>16)&0xFF, (v>>8)&0xFF, v&0xFF)
}
