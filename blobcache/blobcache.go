// Package blobcache implements a simple cache. It is backed by a store, so it
// can be entirely in memory or disk-backed.
//
// While the cached contents are kept in the store, the list recording usage
// information is kept only in memory. On startup the items in the store are
// enumerated and taken to populate the cache list in an undetermined order.
//
// The only cache implemented in here uses an LRU item replacement policy. It
// would be nice for it to use an ARC or 2Q policy instead.
package blobcache

import (
	"container/list"
	"errors"
	"io"
	"sync"

	"github.com/ndlib/bendo/store"
)

// T is the basic blob cache interface.
type T interface {
	// Is the given key in the cache?
	Contains(key string) bool
	// Get content associated with a key from the cache.
	Get(key string) (store.ReadAtCloser, int64, error)
	// Add content into the cache
	Put(key string) (io.WriteCloser, error)
	// Remove an entry from the cache
	Delete(key string) error
	// The maximum size of the cache
	Size() int64
}

// A StoreLRU implements a cache using the least recently used (LRU) eviction
// policy and using a store as the storage backend.
type StoreLRU struct {
	// this is the place where cached items are stored
	s store.Store

	m sync.RWMutex // protects everything below

	// total size in bytes used by the cache. If 0 then
	// we have not calculated the total size of the cache yet.
	size int64

	maxSize int64 // The maximum amount of space we may use

	// list of cache contents
	// front of list is MRU, tail is LRU.
	lru *list.List

	// set of keys that have a Put() started on them, but not closed yet.
	pending map[string]struct{}
}

type entry struct {
	key  string
	size int64
}

// NewLRU creates and initializes a new cache structure using the least
// recently used eviction policy. The given store may already have items in it.
// You must call Scan() (either inline or in another goroutine) to add the
// preexisting items in the store to the LRU list.
func NewLRU(s store.Store, maxSize int64) *StoreLRU {
	return &StoreLRU{
		s:       s,
		maxSize: maxSize,
		lru:     list.New(),
		pending: make(map[string]struct{}),
	}
}

// Scan enumerates the items in the given store and enters them into the LRU
// cache (if they aren't in it already).
func (t *StoreLRU) Scan() {
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
		t.linkEntry(entry{key: key, size: size})
	}
}

// Contains returns true if the given item is in the cache. It does not
// update the LRU status, and does not guarantee the item will be in the
// cache when Get() is called.
func (t *StoreLRU) Contains(key string) bool {
	e := t.find(key)
	return e != nil
}

// Get returns a reader for the given item. It will block until the data is
// ready to be read. The LRU list is updated. If the item is not in the cache
// nil is returned for the ReadAtCloser. (NOTE: it is not an error for an item
// to not be in the cache. Check the ReadAtCloser to see.)
func (t *StoreLRU) Get(key string) (store.ReadAtCloser, int64, error) {
	e := t.find(key)
	if e == nil {
		return nil, 0, nil
	}
	t.m.Lock()
	t.lru.MoveToFront(e)
	t.m.Unlock()
	rac, size, err := t.s.Open(key)
	if err != nil {
		// Something happened, so unlink this item from the lru list
		// and unreserve its space.
		// We assume Open will always return at least one of rac and err as nil.
		err = t.Delete(key)
	}
	return rac, size, err
}

func (t *StoreLRU) find(key string) *list.Element {
	t.m.RLock()
	defer t.m.RUnlock()
	for e := t.lru.Front(); e != nil; e = e.Next() {
		entry := e.Value.(entry)
		if entry.key == key {
			return e
		}
	}
	return nil
}

// Put returns a WriteCloser which saves writes to it in the cache under the
// provided key. Items are evicted from the cache as content is written to
// the Writer. The item is not formally added to the cache until the Writer is
// closed.
//
// Only one writer to a given key can be active at a time. Subsequent Puts
// will return an error. Also, once an item is in the cache, Puts for it will
// return an error (until the item is evicted.)
func (t *StoreLRU) Put(key string) (io.WriteCloser, error) {
	// is there currently a Put pending on that key?
	t.m.Lock()
	_, exists := t.pending[key]
	t.pending[key] = struct{}{} // dosn't hurt since we already know exists
	t.m.Unlock()
	if exists {
		return nil, ErrPutPending
	}
	w, err := t.s.Create(key)
	// special case situation where the key already exists to try again after
	// deleting the key.
	// since we passed the pending check, there are no open Puts on that key
	if err == store.ErrKeyExists {
		t.s.Delete(key)
		w, err = t.s.Create(key)
	}
	if err != nil {
		t.unpending(key)
		return nil, err
	}
	return &writer{parent: t, key: key, w: w}, nil
}

// unpending removes the given key from the pending set.
func (t *StoreLRU) unpending(key string) {
	t.m.Lock()
	defer t.m.Unlock()

	delete(t.pending, key)
}

// Delete removed an item from the cache. It is not an error to remove
// a key which is not present.
func (t *StoreLRU) Delete(key string) error {
	e := t.find(key)
	if e == nil {
		return nil
	}
	t.m.Lock()
	entry := t.lru.Remove(e).(entry)
	t.m.Unlock()
	err := t.s.Delete(entry.key)
	err2 := t.reserve(-entry.size) // give the space back
	if err != nil {
		return err
	}
	return err2
}

// Size returns the maximum size of this cache in bytes.
// (Not the current size of the cache.)
func (t *StoreLRU) Size() int64 {
	return t.maxSize
}

// linkEntry adds the given entry into our LRU list.
func (t *StoreLRU) linkEntry(entry entry) {
	t.m.Lock()
	defer t.m.Unlock()

	t.lru.PushFront(entry)
}

var (
	// ErrCacheFull means the item being added to the cache is too big
	// for the cache.
	ErrCacheFull  = errors.New("Cache is full and no more items can be removed")
	ErrPutPending = errors.New("Key is already being added to cache")
)

// reserve space for the passed in size, evicting items if necessary to stay
// under maxSize. Size can be negative to cancel a previous reservation.
// Nothing is reserved if there is an error.
func (t *StoreLRU) reserve(size int64) error {
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
		err := t.s.Delete(entry.key)
		if err != nil {
			t.size -= size
			return err
		}
		t.size -= entry.size
	}
	return nil
}
