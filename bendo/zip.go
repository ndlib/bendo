package bendo

import (
	"archive/zip"
	"errors"
	"io"
)

type ZipreaderCloser struct {
	f io.Closer
	*zip.Reader
}

func (z *ZipreaderCloser) Close() error {
	return z.f.Close()
}

func OpenBundle(store BundleStore, key string) (*ZipreaderCloser, error) {
	stream, size, err := store.Open(key, key)
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
	ErrNotFound = errors.New("stream not found")
)

// Return a io.ReadCloser which contains the contents of the file `sname` in the bundle `key`.
//
// this doesn't really need a Directory, just a BundleStore...
func OpenBundleStream(store BundleStore, key, sname string) (io.ReadCloser, error) {
	r, err := OpenBundle(store, key)
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

/* Wrapper around the zip.Writer object, which also
tracks the underlying file stream we are writing to.
Some utility methods are added to make our life easier.
*/
type zipwriter struct {
	f           io.WriteCloser // the underlying bundle file, nil if no file is currently open
	*zip.Writer                // the zip interface over the bundle file
}

func openZipWriter(s BundleStore, id string, n int) (*zipwriter, error) {
	f, err := s.Create(sugar(id, n), id)
	if err != nil {
		return nil, err
	}
	return &zipwriter{
		f:      f,
		Writer: zip.NewWriter(f),
	}, nil
}

func (zw *zipwriter) Close() error {
	err := zw.Writer.Close()
	if err == nil {
		err = zw.f.Close()
	}
	return err
}

func (zw *zipwriter) makeStream(name string) (io.Writer, error) {
	header := zip.FileHeader{
		Name:   name,
		Method: zip.Store,
	}
	return zw.CreateHeader(&header)
}
