// Copyright 2016 Stanislav Liberman
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

package memmap

import (
	"fmt"
	"testing"

	"github.com/lirm/aeron-go/aeron/atomic"
)

const (
	MMAP = "mmap.bin"
)

func TestMmapBasics(t *testing.T) {
	t.Log("Beginning test")
	mmap, err := NewFile(MMAP, 0, 8000)
	defer mmap.Close()
	// t.Logf("mmap: %v, len:%d, err:%v", mmap.GetMemoryPtr(), mmap.GetMemorySize(), err)
	fmt.Printf("mmap: %v, len:%d, err:%v\n", mmap.GetMemoryPtr(), mmap.GetMemorySize(), err)

	buf := atomic.MakeBuffer(mmap.GetMemoryPtr(), mmap.GetMemorySize())
	// t.Logf("created atomic buffer: %v", buf)
	fmt.Printf("created atomic buffer: %v\n", buf)

	//    buf.Fill(0)

	num := buf.GetInt64Volatile(0)
	// t.Logf("1: Got %d stuff from file", num)
	fmt.Printf("1: Got %d stuff from file\n", num)

	var expected int64 = 42
	buf.PutInt64Ordered(0, expected)
	num = buf.GetInt64Volatile(0)
	t.Logf("2: Got %d stuff from file\n", num)
	if num != expected {
		t.Fail()
	}
}

func TestNewFileInvalidLengthReturnsError(t *testing.T) {
	name := t.TempDir() + "/zero.bin"
	if _, err := NewFile(name, 0, 0); err == nil {
		t.Fatal("expected error for zero length")
	}
}

func TestMapExistingRegionBeyondFileReturnsError(t *testing.T) {
	name := t.TempDir() + "/small.bin"
	m, err := NewFile(name, 0, 4096)
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := MapExisting(name, 0, 8192); err == nil {
		t.Fatal("expected error mapping past end of file")
	}
	if _, err := MapExisting(name, 4096, 0); err == nil {
		t.Fatal("expected error for offset at end of file")
	}
	m2, err := MapExisting(name, 0, 0)
	if err != nil || m2.GetMemorySize() != 4096 {
		t.Fatalf("m2=%v err=%v", m2, err)
	}
	if err := m2.Close(); err != nil {
		t.Fatal(err)
	}
	if err := m2.Close(); err != nil {
		t.Fatalf("second close: %v", err)
	}
}
