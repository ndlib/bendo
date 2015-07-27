// A store provides a simple, goroutine safe, key-value interface. Instead of
// values being an opaque array of bytes, though, they are a stream. This lets
// large items to be stored easily.
//
// Probably the most important type is the FileSystem. The others are useful
// for testing or wrapping a File.
package store

import (
	"io"
)

type ReadAtCloser interface {
	io.ReaderAt
	io.Closer
}

// Store defines the basic stream based key-value store.
// Items are immutable once stored, but they may be deleted and then replaced
// with a new value.
//
// Since the FileSystem store uses the key as file names, keys should not
// contain forbidden filesystem characters, such as '/'.
//
// Open() returns a ReadAtCloser instead of a ReadCloser to make it easier to
// wrap it by a ZipWriter.
//
// TODO: is a Close() method needed?
type Store interface {
	List() <-chan string
	ListPrefix(prefix string) ([]string, error)
	Open(key string) (ReadAtCloser, int64, error)
	Create(key string) (io.WriteCloser, error)
	Delete(key string) error
}

type reader struct {
	r   io.ReaderAt
	off int64
}

// Turns a ReaderAt into a io.Reader. It is here as a utility to help work with
// the ReadAtCloser returned by Open.
func NewReader(r io.ReaderAt) io.Reader {
	return &reader{r: r}
}

func (r *reader) Read(p []byte) (n int, err error) {
	n, err = r.r.ReadAt(p, r.off)
	r.off += int64(n)
	if err == io.EOF && n > 0 {
		// reading less than a full buffer is not an error for
		// an io.Reader
		err = nil
	}
	return
}
