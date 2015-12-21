// Package blobcache implements a simple cache. It is backed by a store, so it
// can be entirely in memory or disk-backed.
//
// While the cached contents are kept in the store, the list recording usage
// information is kept only in memory. On startup the items in the store are
// enumerated and taken to populate the cache list in an undetermined order.
//
// The cache uses an LRU item replacement policy. It would be nice for it to
// use an ARC or 2Q policy instead.
package blobcache

import (
	"container/list"
	"errors"
	"io"
	"sync"

	"github.com/ndlib/bendo/store"
)

type T struct {
	// this is the place where cached items are stored
	s store.Store

	m sync.RWMutex // protects everything below

	// total size used to store items in cache. If 0 then
	// we have not calculated the total size of the cache yet.
	size int64

	maxSize int64 // The maximum amount of space we may use

	// front of list is MRU, tail is LRU.
	lru *list.List // list of cache contents
}

type entry struct {
	id   string
	size int64
}

// New creates and initializes a new cache structure. The given store
// may already have items in it. Call Scan() either inline or in a goroutine
// to scan the store and add the items inside it to the LRU list.
func New(s store.Store, maxSize int64) *T {
	return &T{s: s, maxSize: maxSize, lru: list.New()}
}

// Scan enumerates the items in the given store and returns their total
// size. Blocks until it is completely finished. Mainly used to initialize the
// size member of the cache structure.
func (t *T) Scan() {
	for key := range t.s.List() {
		if t.Contains(key) {
			continue
		}
		rc, size, err := t.s.Open(key)
		if err != nil {
			continue
		}
		rc.Close()
		err = t.reserve(size)
		if err != nil {
			// this item is too big for the cache.
			t.s.Delete(key)
			continue
		}
		t.linkEntry(entry{id: key, size: size})
	}
}

// Contains returns true if the given item is in the cache. It does not
// update the LRU status, and does not guarantee the item will be in the
// cache when Get() is called.
func (t *T) Contains(id string) bool {
	e := t.find(id)
	return e != nil
}

// Return a reader for the given item. It will block until the data is ready to
// be read. The LRU list is updated. If the item is not in the cache nil is
// returned for the ReadAtCloser. (NOTE: it is not an error for an item to not
// be in the cache. Check the ReadAtCloser to see.)
func (t *T) Get(id string) (store.ReadAtCloser, int64, error) {
	e := t.find(id)
	if e == nil {
		return nil, 0, nil
	}
	t.m.Lock()
	t.lru.MoveToFront(e)
	t.m.Unlock()
	return t.s.Open(id)
}

func (t *T) find(id string) *list.Element {
	t.m.RLock()
	defer t.m.RUnlock()
	for e := t.lru.Front(); e != nil; e = e.Next() {
		entry := e.Value.(entry)
		if entry.id == id {
			return e
		}
	}
	return nil
}

// Put returns a WriteCloser which saves writes to it in the cache under the
// provided id key. Items are evicted from the cache as content is written to
// the Writer. The item is not formally added to the cache until the Writer is
// closed.
//
// Only one writer to a given id can be active at a time. Subsusquient Puts
// will return an error. Also, once an item is in the cache, Puts for it will
// return an error (until the item is evicted.)
func (t *T) Put(id string) (io.WriteCloser, error) {
	w, err := t.s.Create(id)
	if err != nil {
		return nil, err
	}
	return &Writer{parent: t, id: id, w: w}, nil
}

// linkEntry adds the given entry into our LRU list.
func (t *T) linkEntry(entry entry) {
	t.m.Lock()
	defer t.m.Unlock()

	t.lru.PushFront(entry)
}

var (
	ErrCacheFull = errors.New("Cache is full and no more items can be removed")
)

// reserve space for the passed in size, evicting items if necessary to stay
// under maxSize. Size can be negative to cancel a previous reservation.
// Nothing is reserved if there is an error.
func (t *T) reserve(size int64) error {
	t.m.Lock()
	defer t.m.Unlock()

	t.size += size
	for t.size > t.maxSize {
		// LRU eviction
		e := t.lru.Back()
		if e == nil {
			t.size -= size
			return ErrCacheFull
		}
		entry := t.lru.Remove(e).(entry)
		err := t.s.Delete(entry.id)
		if err != nil {
			t.size -= size
			return err
		}
		t.size -= entry.size
	}
	return nil
}
