package blobcache

import (
	"io"
)

// writer provides a way to write a new item into the cache.
type writer struct {
	parent        *StoreLRU
	id            string
	w             io.WriteCloser
	size          int64
	deleteOnClose bool
}

func (w *writer) Close() error {
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

func (w *writer) Write(p []byte) (int, error) {
	// do the write after evicting so we never have more than maxSize in cache
	n := len(p)
	err := w.parent.reserve(int64(n))
	if err != nil {
		if err == ErrCacheFull {
			w.deleteOnClose = true
		}
		return 0, err
	}
	// w.size will be >= the actual item size since we don't use the actual
	// amount written. Perhaps that does need to be tracked.
	w.size += int64(n)
	return w.w.Write(p)
}
