// Package advproto is the wire format, replicated state and cluster client driver shared by the adversarial cluster
// suite (systests/adversarial/cluster) and its helper binaries advsvc and advclient.
package advproto

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"os"
	"path/filepath"
	"sort"
)

// Message kinds.
const (
	KindData int32 = 1
	KindDump int32 = 2
)

// HeaderLength is the fixed prefix of every message: kind, length, tag, seq.
const HeaderLength = 24

// Encode writes a message of HeaderLength+payloadLen bytes into buf and returns its length. The payload bytes are
// derived from (tag, seq) so every receiver can detect corruption.
func Encode(buf []byte, kind int32, tag, seq int64, payloadLen int) int {
	n := HeaderLength + payloadLen
	binary.LittleEndian.PutUint32(buf[0:], uint32(kind))
	binary.LittleEndian.PutUint32(buf[4:], uint32(n))
	binary.LittleEndian.PutUint64(buf[8:], uint64(tag))
	binary.LittleEndian.PutUint64(buf[16:], uint64(seq))
	for i := 0; i < payloadLen; i++ {
		buf[HeaderLength+i] = fill(tag, seq, i)
	}
	return n
}

func fill(tag, seq int64, i int) byte {
	return byte(int64(i)*31 + seq*7 + tag*13)
}

// Decode validates a message and returns its header fields.
func Decode(b []byte) (kind int32, tag, seq int64, err error) {
	if len(b) < HeaderLength {
		return 0, 0, 0, fmt.Errorf("short message: %d", len(b))
	}
	kind = int32(binary.LittleEndian.Uint32(b[0:]))
	n := int(binary.LittleEndian.Uint32(b[4:]))
	tag = int64(binary.LittleEndian.Uint64(b[8:]))
	seq = int64(binary.LittleEndian.Uint64(b[16:]))
	if n != len(b) {
		return kind, tag, seq, fmt.Errorf("length mismatch: header=%d actual=%d tag=%d seq=%d", n, len(b), tag, seq)
	}
	for i := HeaderLength; i < n; i++ {
		if b[i] != fill(tag, seq, i-HeaderLength) {
			return kind, tag, seq, fmt.Errorf("corrupt payload at %d: tag=%d seq=%d", i, tag, seq)
		}
	}
	return kind, tag, seq, nil
}

// Ranges is a sorted, merged set of closed int64 intervals.
type Ranges [][2]int64

// Add inserts v and reports false if it was already present.
func (r *Ranges) Add(v int64) bool {
	s := *r
	i := sort.Search(len(s), func(i int) bool { return s[i][1] >= v-1 })
	if i < len(s) && s[i][0] <= v && v <= s[i][1] {
		return false
	}
	switch {
	case i < len(s) && s[i][1] == v-1:
		s[i][1] = v
		if i+1 < len(s) && s[i+1][0] == v+1 {
			s[i][1] = s[i+1][1]
			s = append(s[:i+1], s[i+2:]...)
		}
	case i < len(s) && s[i][0] == v+1:
		s[i][0] = v
	default:
		s = append(s, [2]int64{})
		copy(s[i+1:], s[i:])
		s[i] = [2]int64{v, v}
	}
	*r = s
	return true
}

// Contains reports whether v is in the set.
func (r Ranges) Contains(v int64) bool {
	i := sort.Search(len(r), func(i int) bool { return r[i][1] >= v })
	return i < len(r) && r[i][0] <= v
}

// Count is the number of values in the set.
func (r Ranges) Count() int64 {
	var n int64
	for _, x := range r {
		n += x[1] - x[0] + 1
	}
	return n
}

// Replicated is the deterministic service state: it must be identical on every member at the same log position.
type Replicated struct {
	Messages int64
	Bytes    int64
	Digest   uint64
	Dups     int64
	Corrupt  int64
	Applied  map[int64]Ranges
	Opens    int64
	Closes   map[string]int64
	// ClosedSessions maps a closed cluster session id to its close reason.
	ClosedSessions map[int64]string
	Timers         int64
}

// NewReplicated returns empty state.
func NewReplicated() *Replicated {
	return &Replicated{Applied: map[int64]Ranges{}, Closes: map[string]int64{}, ClosedSessions: map[int64]string{}}
}

// Mix folds v into the running digest.
func (s *Replicated) Mix(vs ...int64) {
	h := fnv.New64a()
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], s.Digest)
	_, _ = h.Write(b[:])
	for _, v := range vs {
		binary.LittleEndian.PutUint64(b[:], uint64(v))
		_, _ = h.Write(b[:])
	}
	s.Digest = h.Sum64()
}

// Dump is what a service member writes when it applies a KindDump message.
type Dump struct {
	Node           int32
	DumpID         int64
	LogPosition    int64
	State          *Replicated
	SnapshotLoaded bool
	Recovered      bool
}

// DumpPath is where member node writes dump id.
func DumpPath(dir string, node int32, id int64) string {
	return filepath.Join(dir, fmt.Sprintf("node%d-dump%d.json", node, id))
}

// WriteJSON writes v atomically.
func WriteJSON(path string, v interface{}) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// ReadDump reads a dump file.
func ReadDump(path string) (*Dump, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	d := &Dump{}
	return d, json.Unmarshal(b, d)
}

func errorf(format string, args ...interface{}) error { return fmt.Errorf(format, args...) }
