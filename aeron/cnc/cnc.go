/*
Copyright 2026 Kalshi Trading

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Package cnc maps and decodes the media driver command-and-control file (cnc.dat), mirroring
// io.aeron.CncFileDescriptor.
//
// File layout:
//
//	+-----------------------------+
//	|          Meta Data          |  (MetaDataLength bytes)
//	+-----------------------------+
//	|      to-driver Buffer       |
//	+-----------------------------+
//	|      to-clients Buffer      |
//	+-----------------------------+
//	|   Counters Metadata Buffer  |
//	+-----------------------------+
//	|    Counters Values Buffer   |
//	+-----------------------------+
//	|          Error Log          |
//	+-----------------------------+
package cnc

import (
	"errors"
	"fmt"
	"path/filepath"
	"unsafe"

	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/counters"
	"github.com/lirm/aeron-go/aeron/util"
	"github.com/lirm/aeron-go/aeron/util/memmap"
)

// FileName is the name of the CnC file within the Aeron directory.
const FileName = "cnc.dat"

// Version is the CnC version this client understands (0.2.0).
var Version = int32(util.SemanticVersionCompose(0, 2, 0))

// Field offsets within the metadata section, identical to CncFileDescriptor.
const (
	VersionFieldOffset                      int32 = 0
	ToDriverBufferLengthFieldOffset               = VersionFieldOffset + util.SizeOfInt32
	ToClientsBufferLengthFieldOffset              = ToDriverBufferLengthFieldOffset + util.SizeOfInt32
	CountersMetaDataBufferLengthFieldOffset       = ToClientsBufferLengthFieldOffset + util.SizeOfInt32
	CountersValuesBufferLengthFieldOffset         = CountersMetaDataBufferLengthFieldOffset + util.SizeOfInt32
	ErrorLogBufferLengthFieldOffset               = CountersValuesBufferLengthFieldOffset + util.SizeOfInt32
	ClientLivenessTimeoutFieldOffset              = ErrorLogBufferLengthFieldOffset + util.SizeOfInt32
	StartTimestampFieldOffset                     = ClientLivenessTimeoutFieldOffset + util.SizeOfInt64
	PidFieldOffset                                = StartTimestampFieldOffset + util.SizeOfInt64
	FilePageSizeFieldOffset                       = PidFieldOffset + util.SizeOfInt64
)

// MetaDataLength is the length of the metadata section; the to-driver buffer starts at this offset.
const MetaDataLength int32 = util.CacheLineLength * 2

// EndOfMetaDataOffset is the offset of the first buffer after the metadata section.
const EndOfMetaDataOffset = MetaDataLength

// ErrIncompatibleVersion is wrapped by errors from CheckVersion.
var ErrIncompatibleVersion = errors.New("CnC version not compatible")

// ErrNotReady is returned when the CnC file exists but the driver has not yet signalled it is ready (version 0).
var ErrNotReady = errors.New("CnC file not ready")

// CheckVersion verifies the major version of cncVersion matches Version, as CncFileDescriptor.checkVersion does.
func CheckVersion(cncVersion int32) error {
	if util.SemanticVersionMajor(uint32(Version)) != util.SemanticVersionMajor(uint32(cncVersion)) {
		return fmt.Errorf("%w: app=%s file=%s", ErrIncompatibleVersion,
			util.SemanticVersionToString(uint32(Version)), util.SemanticVersionToString(uint32(cncVersion)))
	}
	return nil
}

// ComputeFileLength returns the CnC file length for the given total buffer length and alignment.
func ComputeFileLength(totalLengthOfBuffers, alignment int32) int32 {
	return util.AlignInt32(MetaDataLength+totalLengthOfBuffers, alignment)
}

// File is a decoded view over a mapped CnC file. The accessors return views into the mapped memory and remain valid
// until Close is called.
type File struct {
	mmap   *memmap.File
	buffer *atomic.Buffer
	meta   *atomic.Buffer
}

// Path returns the path of the CnC file inside aeronDir.
func Path(aeronDir string) string {
	return filepath.Join(aeronDir, FileName)
}

// Open maps the CnC file in aeronDir and validates its version and length.
func Open(aeronDir string) (*File, error) {
	return MapFile(Path(aeronDir))
}

// MapFile maps an existing CnC file at path and validates its version and length.
func MapFile(path string) (*File, error) {
	m, err := memmap.MapExisting(path, 0, 0)
	if err != nil {
		return nil, err
	}
	f, err := Wrap(atomic.MakeBuffer(m.GetMemoryPtr(), m.GetMemorySize()))
	if err != nil {
		_ = m.Close()
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	f.mmap = m
	return f, nil
}

// Wrap decodes a CnC file already present in buffer. The caller retains ownership of the memory.
func Wrap(buffer *atomic.Buffer) (*File, error) {
	if buffer.Capacity() < MetaDataLength {
		return nil, fmt.Errorf("CnC file too short: length=%d < %d", buffer.Capacity(), MetaDataLength)
	}
	f := &File{buffer: buffer, meta: atomic.MakeBuffer(buffer.Ptr(), MetaDataLength)}
	version := f.Version()
	if version == 0 {
		return nil, ErrNotReady
	}
	if err := CheckVersion(version); err != nil {
		return nil, err
	}
	if !f.IsLengthSufficient() {
		return nil, fmt.Errorf("CnC file length insufficient: length=%d required=%d", buffer.Capacity(), f.requiredLength())
	}
	return f, nil
}

// Close unmaps the file if it was mapped by Open/MapFile.
func (f *File) Close() error {
	if f.mmap == nil {
		return nil
	}
	err := f.mmap.Close()
	f.mmap = nil
	return err
}

// Buffer returns the whole CnC file buffer.
func (f *File) Buffer() *atomic.Buffer { return f.buffer }

// MetaDataBuffer returns the metadata section.
func (f *File) MetaDataBuffer() *atomic.Buffer { return f.meta }

// Version returns the CnC version (volatile read; 0 until the driver has finished initialising the file).
func (f *File) Version() int32 { return f.meta.GetInt32Volatile(VersionFieldOffset) }

// ToDriverBufferLength returns the length of the to-driver ring buffer (including trailer).
func (f *File) ToDriverBufferLength() int32 { return f.meta.GetInt32(ToDriverBufferLengthFieldOffset) }

// ToClientsBufferLength returns the length of the to-clients broadcast buffer (including trailer).
func (f *File) ToClientsBufferLength() int32 {
	return f.meta.GetInt32(ToClientsBufferLengthFieldOffset)
}

// CountersMetaDataBufferLength returns the length of the counters metadata buffer.
func (f *File) CountersMetaDataBufferLength() int32 {
	return f.meta.GetInt32(CountersMetaDataBufferLengthFieldOffset)
}

// CountersValuesBufferLength returns the length of the counters values buffer.
func (f *File) CountersValuesBufferLength() int32 {
	return f.meta.GetInt32(CountersValuesBufferLengthFieldOffset)
}

// ErrorLogBufferLength returns the length of the driver's distinct error log.
func (f *File) ErrorLogBufferLength() int32 { return f.meta.GetInt32(ErrorLogBufferLengthFieldOffset) }

// ClientLivenessTimeoutNs returns the driver's client liveness timeout in nanoseconds.
func (f *File) ClientLivenessTimeoutNs() int64 {
	return f.meta.GetInt64(ClientLivenessTimeoutFieldOffset)
}

// StartTimestampMs returns the epoch time in ms at which the driver started.
func (f *File) StartTimestampMs() int64 { return f.meta.GetInt64(StartTimestampFieldOffset) }

// Pid returns the process id of the driver.
func (f *File) Pid() int64 { return f.meta.GetInt64(PidFieldOffset) }

// FilePageSize returns the page size the driver uses for its files.
func (f *File) FilePageSize() int32 { return f.meta.GetInt32(FilePageSizeFieldOffset) }

func (f *File) toDriverOffset() int32  { return MetaDataLength }
func (f *File) toClientsOffset() int32 { return f.toDriverOffset() + f.ToDriverBufferLength() }
func (f *File) countersMetaDataOffset() int32 {
	return f.toClientsOffset() + f.ToClientsBufferLength()
}
func (f *File) countersValuesOffset() int32 {
	return f.countersMetaDataOffset() + f.CountersMetaDataBufferLength()
}
func (f *File) errorLogOffset() int32 {
	return f.countersValuesOffset() + f.CountersValuesBufferLength()
}
func (f *File) requiredLength() int32 { return f.errorLogOffset() + f.ErrorLogBufferLength() }

// IsLengthSufficient reports whether the mapped length covers all buffers described in the metadata.
func (f *File) IsLengthSufficient() bool { return f.buffer.Capacity() >= f.requiredLength() }

func (f *File) slice(offset, length int32) *atomic.Buffer {
	return atomic.MakeBuffer(unsafe.Add(f.buffer.Ptr(), offset), length)
}

// ToDriverBuffer returns the to-driver many-to-one ring buffer region.
func (f *File) ToDriverBuffer() *atomic.Buffer {
	return f.slice(f.toDriverOffset(), f.ToDriverBufferLength())
}

// ToClientsBuffer returns the to-clients broadcast buffer region.
func (f *File) ToClientsBuffer() *atomic.Buffer {
	return f.slice(f.toClientsOffset(), f.ToClientsBufferLength())
}

// CountersMetaDataBuffer returns the counters metadata region.
func (f *File) CountersMetaDataBuffer() *atomic.Buffer {
	return f.slice(f.countersMetaDataOffset(), f.CountersMetaDataBufferLength())
}

// CountersValuesBuffer returns the counters values region.
func (f *File) CountersValuesBuffer() *atomic.Buffer {
	return f.slice(f.countersValuesOffset(), f.CountersValuesBufferLength())
}

// ErrorLogBuffer returns the driver's distinct error log region (see package errorlog).
func (f *File) ErrorLogBuffer() *atomic.Buffer {
	return f.slice(f.errorLogOffset(), f.ErrorLogBufferLength())
}

// CountersReader returns a counters reader over this file's counters.
func (f *File) CountersReader() *counters.Reader {
	return counters.NewReader(f.CountersValuesBuffer(), f.CountersMetaDataBuffer())
}
