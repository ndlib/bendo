package blobcache

import (
	"io"
)

// saver is what a writer expects to communicate with as a new item is
// copied into the cache.
type saver interface {
	save(w *writer)      // new item has been successfully copied
	reserve(int64) error // gets more space on each call to Write
	discard(w *writer)   // new item had an error while being copied
}

// writer provides a way to write a new item into the cache.
type writer struct {
	parent        saver
	key           string
	w             io.WriteCloser
	size          int64
	deleteOnClose bool
}

func (w *writer) Close() error {
	err := w.w.Close()
	if err != nil || w.deleteOnClose {
		w.parent.discard(w)
		return err // what should this be?
	}
	w.parent.save(w)
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
