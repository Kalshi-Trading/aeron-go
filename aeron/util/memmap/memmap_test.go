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
	"os"
	"strings"
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

func isMapped(t *testing.T, name string) bool {
	maps, err := os.ReadFile("/proc/self/maps")
	if err != nil {
		t.Skip("no /proc/self/maps")
	}
	return strings.Contains(string(maps), name)
}

// A second Close of an already unmapped File must not unmap whatever was later mapped at the same address.
func TestCloseTwiceDoesNotUnmapReusedAddress(t *testing.T) {
	dir := t.TempDir()
	a, err := NewFile(dir+"/a.dat", 0, 1<<16)
	if err != nil {
		t.Fatal(err)
	}
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	b, err := NewFile(dir+"/b.dat", 0, 1<<16)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	if b.GetMemoryPtr() != a.GetMemoryPtr() {
		t.Skip("kernel did not reuse the address")
	}
	_ = a.Close()
	if !isMapped(t, dir+"/b.dat") {
		t.Fatal("closing a stale File unmapped a newer mapping at the same address")
	}
}

func TestCloseIsIdempotent(t *testing.T) {
	f, err := NewFile(t.TempDir()+"/c.dat", 0, 1<<12)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}
