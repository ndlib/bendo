package bendo

import (
	"fmt"
	"io"
	"strings"
	"testing"
)

type memoryStore struct {
	m map[string]*buf
}

func NewMemoryStore() BundleStore {
	return &memoryStore{make(map[string]*buf)}
}

func (ms *memoryStore) List() <-chan string {
	c := make(chan string)
	go func() {
		for k, _ := range ms.m {
			c <- k
		}
		close(c)
	}()
	return c
}

func (ms *memoryStore) ListPrefix(prefix string) ([]string, error) {
	var result []string
	for k, _ := range ms.m {
		if strings.HasPrefix(k, prefix) {
			result = append(result, k)
		}
	}
	return result, nil
}

func (ms *memoryStore) Open(key, id string) (ReadAtCloser, int64, error) {
	v, ok := ms.m[key]
	if !ok {
		return nil, 0, fmt.Errorf("No item %s", key)
	}
	return v, int64(len(v.b)), nil
}

type buf struct {
	b []byte
}

func (r *buf) Close() error { return nil }
func (r *buf) ReadAt(p []byte, off int64) (int, error) {
	if int(off) > len(r.b) {
		return 0, io.EOF
	}
	n := copy(p, r.b[off:])
	return n, nil
}

func (r *buf) Write(p []byte) (int, error) {
	r.b = append(r.b, p...)
	return len(p), nil
}

func (ms *memoryStore) Create(key, id string) (io.WriteCloser, error) {
	r := &buf{}
	ms.m[key] = r
	return r, nil
}

func (ms *memoryStore) Delete(key, id string) error {
	delete(ms.m, key)
	return nil
}

func TestBundleWriter(t *testing.T) {
	ms := NewMemoryStore().(*memoryStore)
	item := &Item{ID: "12345"}
	bw := NewBundler(ms, item)
	if bw.CurrentBundle() != 1 {
		t.Fatalf("CurrentBundle() == %d, expected 1", bw.CurrentBundle())
	}
	blob := &Blob{ID: 1}
	item.Blobs = append(item.Blobs, blob)
	err := bw.WriteBlob(blob, strings.NewReader("Hello There"))
	if err != nil {
		t.Fatalf("WriteBlob() == %s, expected nil", err.Error())
	}
	err = bw.Close()
	if err != nil {
		t.Fatalf("Close() == %s, expected nil", err.Error())
	}
	_, ok := ms.m["12345-0001"]
	if !ok {
		t.Fatalf("Expected bundle 12345-0001 to exist")
	}
	_, ok = ms.m["12345-0002"]
	if ok {
		t.Fatalf("Expected bundle 12345-0002 to not exist.")
	}
	helloR, err := OpenBundleStream(ms, "12345-0001", "blob/1")
	if err != nil {
		t.Fatalf("OpenBundleStream() == %s, expected nil", err.Error())
	}
	var hello = make([]byte, 20)
	n, _ := helloR.Read(hello)
	if n != 11 {
		t.Fatalf("Blob/1 length = %d, expected 11", n)
	}
	if string(hello)[:n] != "Hello There" {
		t.Fatalf("Blob/1 = %#v, expected \"Hello There\"", string(hello))
	}
}
