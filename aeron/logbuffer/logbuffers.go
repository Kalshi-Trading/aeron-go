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
	"errors"
	"fmt"
	"unsafe"

	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/logging"
	"github.com/lirm/aeron-go/aeron/util/memmap"
)

var logger = logging.MustGetLogger("logbuffers")

// LogBuffers provides access to the memory mapped log file containing the term partitions and the metadata section.
type LogBuffers struct {
	mmapFiles  []*memmap.File
	buffers    [PartitionCount + 1]atomic.Buffer
	meta       LogBufferMetaData
	refCount   int
	termLength int32
	fileName   string
}

// Wrap maps the log file and panics if it cannot be mapped or fails validation. Prefer NewLogBuffers.
func Wrap(fileName string) *LogBuffers {
	buffers, err := NewLogBuffers(fileName)
	if err != nil {
		panic(err)
	}
	return buffers
}

// NewLogBuffers maps an existing log file, validating the term length and page size stored in the metadata and
// that the file length matches ComputeLogLength(termLength, pageSize).
func NewLogBuffers(fileName string) (*LogBuffers, error) {
	logLength := memmap.GetFileSize(fileName)
	if logLength < 0 {
		return nil, fmt.Errorf("log buffer file not found: %s", fileName)
	}
	minLength := int64(TermMinLength)*PartitionCount + int64(LogMetaDataLength)
	if logLength < minLength {
		return nil, fmt.Errorf("log buffer length less than min length of %d: length=%d file=%s",
			minLength, logLength, fileName)
	}

	mmap, err := memmap.MapExisting(fileName, 0, 0)
	if err != nil {
		return nil, fmt.Errorf("failed to map log buffer %s: %w", fileName, err)
	}

	buffers := &LogBuffers{mmapFiles: []*memmap.File{mmap}, fileName: fileName}
	base := mmap.GetMemoryPtr()
	metaPtr := unsafe.Add(base, logLength-int64(LogMetaDataLength))
	buffers.buffers[LogMetaDataSectionIndex].Wrap(metaPtr, LogMetaDataLength)
	buffers.meta.Wrap(&buffers.buffers[LogMetaDataSectionIndex], 0)

	termLength := buffers.meta.TermLength()
	pageSize := buffers.meta.PageSize()
	if err := CheckTermLength(termLength); err != nil {
		mmap.Close()
		return nil, fmt.Errorf("%w: file=%s", err, fileName)
	}
	if err := CheckPageSize(pageSize); err != nil {
		mmap.Close()
		return nil, fmt.Errorf("%w: file=%s", err, fileName)
	}
	if expected := ComputeLogLength(termLength, pageSize); expected != logLength {
		mmap.Close()
		return nil, fmt.Errorf("log buffer length %d does not match expected %d for termLength=%d pageSize=%d: file=%s",
			logLength, expected, termLength, pageSize, fileName)
	}

	buffers.termLength = termLength
	for i := 0; i < PartitionCount; i++ {
		ptr := unsafe.Add(base, int64(i)*int64(termLength))
		buffers.buffers[i].Wrap(ptr, termLength)
	}
	return buffers, nil
}

// Meta return log buffer meta data flyweight
func (logBuffers *LogBuffers) Meta() *LogBufferMetaData {
	return &logBuffers.meta
}

// MetaDataBuffer returns the buffer over the log metadata section.
func (logBuffers *LogBuffers) MetaDataBuffer() *atomic.Buffer {
	return &logBuffers.buffers[LogMetaDataSectionIndex]
}

// TermBuffers returns the term partition buffers.
func (logBuffers *LogBuffers) TermBuffers() []*atomic.Buffer {
	terms := make([]*atomic.Buffer, PartitionCount)
	for i := range terms {
		terms[i] = &logBuffers.buffers[i]
	}
	return terms
}

// TermLength returns the length of each term partition.
func (logBuffers *LogBuffers) TermLength() int32 {
	return logBuffers.termLength
}

// FileName returns the name of the mapped log file.
func (logBuffers *LogBuffers) FileName() string {
	return logBuffers.fileName
}

// PreTouch touches every page of the term buffers so they are faulted in before use.
func (logBuffers *LogBuffers) PreTouch() {
	pageSize := logBuffers.meta.PageSize()
	for i := 0; i < PartitionCount; i++ {
		term := &logBuffers.buffers[i]
		for offset := int32(0); offset < logBuffers.termLength; offset += pageSize {
			term.CompareAndSetInt32(offset, 0, 0)
		}
	}
}

// Buffer returns a buffer backing a specific term based on index. PartitionLength+1 is the size of the buffer array,
// and the last buffer is the metadata buffer, which can be accessed through a convenience wrapped via Meta() method.
func (logBuffers *LogBuffers) Buffer(index int) *atomic.Buffer {
	return &logBuffers.buffers[index]
}

// Close unmaps the backing memory maps, returning all errors encountered.
func (logBuffers *LogBuffers) Close() error {
	logger.Debug("Closing logBuffers")
	var errs []error
	for _, mmap := range logBuffers.mmapFiles {
		if err := mmap.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	logBuffers.mmapFiles = nil
	return errors.Join(errs...)
}

// IncRef increments the reference count. Returns the current reference count after increment.
func (logBuffers *LogBuffers) IncRef() int {
	logBuffers.refCount++
	return logBuffers.refCount
}

// DecRef decrements the reference count. Returns the current reference counter after decrement.
func (logBuffers *LogBuffers) DecRef() int {
	logBuffers.refCount--
	return logBuffers.refCount
}
