package memmap

import (
	"path/filepath"
	"testing"
)

func TestNewFileInvalidLengthReturnsError(t *testing.T) {
	name := filepath.Join(t.TempDir(), "zero.bin")
	for _, length := range []int{0, -1} {
		if f, err := NewFile(name, 0, length); err == nil {
			_ = f.Close()
			t.Fatalf("NewFile(length=%d) returned no error", length)
		}
	}
}

func TestNewFileUncreatableReturnsError(t *testing.T) {
	if _, err := NewFile(filepath.Join(t.TempDir(), "missing", "x.bin"), 0, 4096); err == nil {
		t.Fatal("NewFile in a missing directory returned no error")
	}
}

func TestNewFileWithOffsetSizesFile(t *testing.T) {
	name := filepath.Join(t.TempDir(), "offset.bin")
	f, err := NewFile(name, 4096, 4096)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if got := GetFileSize(name); got != 8192 {
		t.Fatalf("file size = %d, want 8192", got)
	}
}

func TestCloseIsIdempotent(t *testing.T) {
	dir := t.TempDir()
	a, err := NewFile(filepath.Join(dir, "a.bin"), 0, 4096)
	if err != nil {
		t.Fatal(err)
	}
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	// A new mapping may reuse a's address; a second Close of a must not unmap it.
	b, err := NewFile(filepath.Join(dir, "b.bin"), 0, 4096)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	if err := a.Close(); err != nil {
		t.Fatalf("second Close returned %v", err)
	}
	*(*int64)(b.GetMemoryPtr()) = 42
	if got := *(*int64)(b.GetMemoryPtr()); got != 42 {
		t.Fatalf("b mapping corrupted: %d", got)
	}
}

func TestMapExistingMissingFile(t *testing.T) {
	if _, err := MapExisting(filepath.Join(t.TempDir(), "nope"), 0, 0); err == nil {
		t.Fatal("MapExisting on a missing file returned no error")
	}
}
