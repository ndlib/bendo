package items

import (
	"errors"
	"io"
	"strings"

	"github.com/ndlib/bendo/bagit"
	"github.com/ndlib/bendo/store"
)

// TODO(dbrower): This file could use some cleanup/rethinking after replacing
// the zip file code with the bagit code. Can the code be reduced or removed?

// A BagreaderCloser is a bagit.Reader which will also close the underlying file.
type BagreaderCloser struct {
	f             io.Closer // the underlying file
	*bagit.Reader           // the zip reader
}

// Close flushes the reader and closes the underlying io.Closer.
func (bg *BagreaderCloser) Close() error {
	return bg.f.Close()
}

// OpenBundle opens the provided key in the given store, and wraps it in a
// bagit reader.
func OpenBundle(s store.Store, key string) (*BagreaderCloser, error) {
	stream, size, err := s.Open(key)
	if err != nil {
		return nil, err
	}
	var result *BagreaderCloser
	r, err := bagit.NewReader(stream, size)
	if err == nil {
		result = &BagreaderCloser{
			Reader: r,
			f:      stream,
		}
	} else {
		stream.Close()
	}
	return result, err
}

type parentReadCloser struct {
	parent io.Closer
	io.ReadCloser
}

func (r *parentReadCloser) Close() error {
	r.ReadCloser.Close()
	return r.parent.Close()
}

var (
	// ErrNotFound means a stream inside a zip file with the given name
	// could not be found.
	ErrNotFound = errors.New("stream not found")
)

// OpenBundleStream returns an io.ReadCloser containing the contents of the
// stream sname inside the bundle having the given key in the given store.
func OpenBundleStream(s store.Store, key, sname string) (io.ReadCloser, error) {
	r, err := OpenBundle(s, key)
	if err != nil {
		return nil, err
	}
	var result *parentReadCloser
	rc, err := r.Open(sname)
	if err == nil {
		result = &parentReadCloser{
			parent:     r,
			ReadCloser: rc,
		}
	} else if err == bagit.ErrNotFound {
		err = ErrNotFound
	}
	return result, err
}

// A Zipwriter wraps the zip.Writer object to track the underlying file stream
// holding the zip file's complete contents.
// Some utility methods are added to make our life easier.
type Zipwriter struct {
	f             io.WriteCloser // the underlying bundle file, nil if no file is currently open
	*bagit.Writer                // the zip interface over the bundle file
}

// OpenZipWriter creates a new bundle in the given store using the given id and
// bundle number. It returns a zip writer which is then saved into the store.
func OpenZipWriter(s store.Store, id string, n int) (*Zipwriter, error) {
	f, err := s.Create(sugar(id, n))
	if err != nil {
		return nil, err
	}
	return &Zipwriter{
		f:      f,
		Writer: bagit.NewWriter(f, strings.TrimSuffix(id, ".zip")),
	}, nil
}

// Close writes out the zip directory information and then closes the underlying
// file descriptor for this bundle file.
func (zw *Zipwriter) Close() error {
	err := zw.Writer.Close()
	if err == nil {
		err = zw.f.Close()
	}
	return err
}

// MakeStream returns a writer which saves a file with the given name
// inside this zip file. The writer does not need to be closed when finished.
// Only one stream can be active at a time, and call MakeStream again to start
// the next stream.
func (zw *Zipwriter) MakeStream(name string) (io.Writer, error) {
	return zw.Create(name)
}
