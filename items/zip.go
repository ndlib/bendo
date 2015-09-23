package items

import (
	"archive/zip"
	"errors"
	"io"

	"github.com/ndlib/bendo/store"
)

// A ZipreaderCloser is a zip.Reader which will also close the underlying file.
type ZipreaderCloser struct {
	f io.Closer
	*zip.Reader
}

// Close this ZipreaderCloser.
func (z *ZipreaderCloser) Close() error {
	return z.f.Close()
}

// OpenBundle opens the given key in the given store, and wraps a zip reader
// around the resulting ReadAtCloser.
func OpenBundle(s store.Store, key string) (*ZipreaderCloser, error) {
	stream, size, err := s.Open(key)
	if err != nil {
		return nil, err
	}
	var result *ZipreaderCloser
	r, err := zip.NewReader(stream, size)
	if err == nil {
		result = &ZipreaderCloser{
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
	err = ErrNotFound
	for _, f := range r.File {
		if f.Name != sname {
			continue
		}
		var rc io.ReadCloser
		rc, err = f.Open()
		if err == nil {
			result = &parentReadCloser{
				parent:     r,
				ReadCloser: rc,
			}
		}
		break
	}
	return result, err
}

// A Zipwriter wraps the zip.Writer object to track the underlying file stream
// holding the zip file's complete contents.
// Some utility methods are added to make our life easier.
type Zipwriter struct {
	f           io.WriteCloser // the underlying bundle file, nil if no file is currently open
	*zip.Writer                // the zip interface over the bundle file
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
		Writer: zip.NewWriter(f),
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

// MakeStream returns a writer which saves into a file with the given name
// inside this zip file. The writer does not need to be closed when finished.
// Only one stream can be active at a time, and call MakeStream again to start
// the next stream.
func (zw *Zipwriter) MakeStream(name string) (io.Writer, error) {
	header := zip.FileHeader{
		Name:   name,
		Method: zip.Store,
	}
	return zw.CreateHeader(&header)
}
