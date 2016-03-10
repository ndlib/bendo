package blobcache

import (
	"io"
	"io/ioutil"

	"github.com/ndlib/bendo/store"
)

// An EmptyCache always misses. It contains nothing and saves nothing.
type EmptyCache struct{}

// Contains always returns false.
func (EmptyCache) Contains(key string) bool {
	return false
}

// Get always returns a cache miss.
func (EmptyCache) Get(key string) (store.ReadAtCloser, int64, error) {
	return nil, 0, nil
}

// Put returns a valid WriteCloser which discards its input.
// The item being added will ultimately not be added to the cache.
func (EmptyCache) Put(key string) (io.WriteCloser, error) {
	return nopCloser{ioutil.Discard}, nil
}

// Delete removes an item from the cache. (In this case it's always a nop).
func (EmptyCache) Delete(key string) error {
	return nil
}

// Size returns 0.
func (EmptyCache) Size() int64 {
	return 0
}

type nopCloser struct {
	io.Writer
}

func (nopCloser) Close() error { return nil }
