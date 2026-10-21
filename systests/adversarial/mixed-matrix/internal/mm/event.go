package mm

import (
	"encoding/json"
	"io"
	"sync"
)

// Event is one JSON line exchanged with child peers (Go mmpeer and Java MatrixPeer).
type Event map[string]interface{}

// Str returns a string field.
func (e Event) Str(k string) string {
	s, _ := e[k].(string)
	return s
}

// Int returns a numeric field.
func (e Event) Int(k string) int64 {
	switch v := e[k].(type) {
	case float64:
		return int64(v)
	case json.Number:
		n, _ := v.Int64()
		return n
	}
	return 0
}

// Bool returns a boolean field.
func (e Event) Bool(k string) bool {
	b, _ := e[k].(bool)
	return b
}

// Emitter writes events as JSON lines.
type Emitter struct {
	mu sync.Mutex
	w  io.Writer
}

// NewEmitter writes to w.
func NewEmitter(w io.Writer) *Emitter { return &Emitter{w: w} }

// Emit writes event name with key/value pairs.
func (e *Emitter) Emit(name string, kv ...interface{}) {
	ev := Event{"event": name}
	for i := 0; i+1 < len(kv); i += 2 {
		ev[kv[i].(string)] = kv[i+1]
	}
	b, _ := json.Marshal(ev)
	e.mu.Lock()
	_, _ = e.w.Write(append(b, '\n'))
	e.mu.Unlock()
}
