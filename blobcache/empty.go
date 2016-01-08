package blobcache

import (
	"io"
	"io/ioutil"

	"github.com/ndlib/bendo/store"
)

// An EmptyCache always misses. It contains nothing and saves nothing.
type EmptyCache struct{}

// Contains always returns false.
func (EmptyCache) Contains(id string) bool {
	return false
}

// Get always returns a cache miss.
func (EmptyCache) Get(id string) (store.ReadAtCloser, int64, error) {
	return nil, 0, nil
}

// Put returns a valid WriteCloser which discards its input.
// The item being added will ultimately not be added to the cache.
func (EmptyCache) Put(id string) (io.WriteCloser, error) {
	return nopCloser{ioutil.Discard}, nil
}

type nopCloser struct {
	io.Writer
}

func (nopCloser) Close() error { return nil }
