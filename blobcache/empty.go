package blobcache

import (
	"io"
	"io/ioutil"

	"github.com/ndlib/bendo/store"
)

// An EmptyCache always misses. It contains nothing and saves nothing.
type EmptyCache struct{}

func (EmptyCache) Contains(id string) bool {
	return false
}

func (EmptyCache) Get(id string) (store.ReadAtCloser, int64, error) {
	return nil, 0, nil
}

func (EmptyCache) Put(id string) (io.WriteCloser, error) {
	return nopCloser{ioutil.Discard}, nil
}

type nopCloser struct {
	io.Writer
}

func (nopCloser) Close() error { return nil }
