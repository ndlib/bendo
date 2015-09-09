package store

import (
	"fmt"
	"io"
	"strings"
	"sync"
)

// Memory implements a simple in-memory version of a store. It is intended
// mainly for testing.
type Memory struct {
	m     sync.RWMutex
	store map[string]*buf
}

var (
	// ensure Memory satisfies the Store interface
	_ Store = &Memory{}
)

func NewMemory() *Memory {
	return &Memory{store: make(map[string]*buf)}
}

// The listing goroutine keeps a read lock on the underlying store for its
// duration. This may cause deadlocks.
func (ms *Memory) List() <-chan string {
	c := make(chan string)
	go func() {
		ms.m.RLock()
		for k, _ := range ms.store {
			c <- k
		}
		ms.m.RUnlock()
		close(c)
	}()
	return c
}

func (ms *Memory) ListPrefix(prefix string) ([]string, error) {
	var result []string
	ms.m.RLock()
	for k, _ := range ms.store {
		if strings.HasPrefix(k, prefix) {
			result = append(result, k)
		}
	}
	ms.m.RUnlock()
	return result, nil
}

func (ms *Memory) Open(key string) (ReadAtCloser, int64, error) {
	ms.m.RLock()
	v, ok := ms.store[key]
	ms.m.RUnlock()
	if !ok {
		return nil, 0, fmt.Errorf("No item %s", key)
	}
	v.m.RLock()
	return v, int64(len(v.b)), nil
}

// Need to support a RWMutex instead of a Mutex, since some code path
// in reading a bundle opens a buf twice for reading.
// Because the same Close() is used in both cases, we need a flag to
// remember which unlock method to use.
type buf struct {
	m       sync.RWMutex
	iswrite bool
	b       []byte
}

func (r *buf) Close() error {
	if r.iswrite {
		r.iswrite = false
		r.m.Unlock()
	} else {
		r.m.RUnlock()
	}
	return nil
}

func (r *buf) ReadAt(p []byte, off int64) (int, error) {
	if int(off) >= len(r.b) {
		return 0, io.EOF
	}
	n := copy(p, r.b[off:])
	return n, nil
}

func (r *buf) Write(p []byte) (int, error) {
	r.b = append(r.b, p...)
	return len(p), nil
}

func (ms *Memory) Create(key string) (io.WriteCloser, error) {
	r := &buf{}
	r.m.Lock()
	r.iswrite = true
	ms.m.Lock()
	ms.store[key] = r
	ms.m.Unlock()
	return r, nil
}

func (ms *Memory) Delete(key string) error {
	ms.m.Lock()
	delete(ms.store, key)
	ms.m.Unlock()
	return nil
}

// for testing and debugging
func (ms *Memory) Dump(w io.Writer) {
	ms.m.RLock()
	for k, v := range ms.store {
		s := v.b
		if len(s) > 300 {
			s = s[:50]
		}
		fmt.Fprintf(w, "%s: %s\n", k, string(s))
	}
	ms.m.RUnlock()
}
