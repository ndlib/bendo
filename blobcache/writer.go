package blobcache

import (
	"io"
)

type Writer struct {
	parent        *T
	id            string
	w             io.WriteCloser
	size          int64
	deleteOnClose bool
}

func (w *Writer) Close() error {
	err := w.w.Close()
	if err != nil || w.deleteOnClose {
		// TODO: handle errors better here?
		w.parent.s.Delete(w.id)
		w.parent.reserve(-w.size) // give space back to cache
		return nil                // what should this be?
	}
	w.parent.linkEntry(entry{
		id:   w.id,
		size: w.size,
	})
	return nil
}

func (w *Writer) Write(p []byte) (int, error) {
	// do the write after evicting so we never have more than maxSize in cache
	n := len(p)
	err := w.parent.reserve(int64(n))
	if err != nil {
		if err == ErrCacheFull {
			w.deleteOnClose = true
		}
		return 0, err
	}
	w.size += int64(n) // w.size will be >= the actual item size
	return w.w.Write(p)
}
