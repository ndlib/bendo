package store

// Both the S3 and BlackPearl interfaces have a need to cache remote data and
// state in memory. This file implements a few kinds of caches.

import (
	"sync"
	"time"
)

// head is the structure stored in a sizecache.
type head struct {
	expire time.Time
	size   int64 // size of item. 0 = ?, -1 = doesn't exist. see constant below
}

// A sizecache is used to remember the size or non-size of a remote object.
// The size is either a positive int64, 0 = we don't know, -1 = item doesn't
// exist. Entries will expire after some amount of time. Items not existing
// will expire quicker than items with a positive size.
type sizecache struct {
	m         sync.RWMutex    // protects everything below
	cache     map[string]head // cache for item sizes
	sweeptime time.Time       // next time to age everything
}

const (
	// constants for head.size. Indicates that the given key is deleted.
	sizeDeleted int64 = -1 // any negative number will work

	defaultMissTTL = 3 * time.Hour   // 3 hours
	defaultHitTTL  = 240 * time.Hour // 10 days
)

func newSizeCache() *sizecache {
	return &sizecache{
		cache: make(map[string]head),
	}
}

// Get returns the size associated with key. If key is not in the cache
// it will call the fill function to figure out what the size is.
// If a size is negative the error ErrNotExist is returned.
func (s *sizecache) Get(key string, fill func(key string) (int64, error)) (int64, error) {
	s.m.Lock()
	defer s.m.Unlock()
	if time.Now().After(s.sweeptime) {
		go s.age()
	}
	entry := s.cache[key]
	if entry.size > 0 {
		return entry.size, nil
	}
	if entry.size < 0 {
		// we have previously determined this key does not exist
		return 0, ErrNotExist
	}
	if fill == nil {
		return 0, nil
	}
	// key is not cached, so try to fill it
	// TODO(dbrower): we hold the lock m during this call.
	// Is there a way to release it? need to take into account
	// what if the key is deleted while fill() is executing.
	size, err := fill(key)
	s.set0(key, size)
	return size, err
}

// Set caches a size to use for the given key.
// Use sizeDeleted to mark the key as missing.
func (s *sizecache) Set(key string, size int64) {
	s.m.Lock()
	s.set0(key, size)
	s.m.Unlock()
}

// set0 is just like set but assumes caller already has a lock on s.m
func (s *sizecache) set0(key string, size int64) {
	ttl := defaultHitTTL
	switch {
	case size < 0:
		ttl = defaultMissTTL
	case size == 0:
		ttl = 0
	}
	s.cache[key] = head{expire: time.Now().Add(ttl), size: size}
}

// age will age all the cache entries, and remove the ones that have
// become too old. It holds m the entire time.
func (s *sizecache) age() {
	s.m.Lock()
	defer s.m.Unlock()
	now := time.Now()
	s.sweeptime = now.Add(time.Hour) // next sweep in an hour
	for k, v := range s.cache {
		if now.After(v.expire) {
			delete(s.cache, k) // remove aged entries
		}
	}
}
