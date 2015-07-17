package bendo

import (
	"fmt"
	"io"
	"sync"

	"github.com/golang/groupcache/singleflight"
)

type server struct {
	cache   ItemCache
	s       ItemStore
	table   singleflight.Group  // keyed by item id
	m       sync.Mutex          // protects updates
	updates map[string]struct{} // lazily created
}

func NewServer(s BundleStore, cache ItemCache) ItemServer {
	return &server{s: New(s), cache: cache}
}

func (s *server) List() <-chan string {
	return make(chan string)
}

func (s *server) Item(id string) (*Item, error) {
	result := s.cache.Lookup(id)
	if result != nil && len(result.versions) > 0 {
		// we have a complete item record
		return result, nil
	}
	val, err := s.table.Do(id, func() (interface{}, error) {
		v, err := s.s.Item(id)
		if err == nil {
			s.cache.Set(id, v)
		}
		return v, err
	})
	if val != nil {
		result = val.(*Item)
	}
	return result, err
}

func (s *server) Blob(id string, bid BlobID) (io.ReadCloser, error) {
	// this looks like itemstore•Blob, but we call Item on a Server, so it
	// will only call the underlying blobstore once
	item, err := s.Item(id)
	if err != nil {
		return nil, err
	}
	b := item.blobByID(bid)
	if b == nil {
		return nil, fmt.Errorf("No blob (%s, %s)", id, bid)
	}
	sname := fmt.Sprintf("blob/%d", bid)
	return OpenBundleStream(s.BundleStore(), sugar(id, b.Bundle), sname)
}

func (s *server) Update(id string) Transaction {
	s.m.Lock()
	if s.updates == nil {
		s.updates = make(map[string]struct{})
	}
	_, ok := s.updates[id]
	if ok {
		// there is already an update open....
		s.m.Unlock()
		return nil
	}
	s.updates[id] = struct{}{}
	s.m.Unlock()
	return nil
}

type serverTx struct {
	tx
}

func (s *server) Validate(id string) (int64, []string, error) {
	return 0, nil, nil
}

func (s *server) BundleStore() BundleStore {
	return s.s.BundleStore()
}

// memcache implements a simple thread-safe cache
type memcache struct {
	m  sync.RWMutex
	kv map[string]*Item
}

func NewMemoryCache() ItemCache {
	return &memcache{
		kv: make(map[string]*Item),
	}
}

func (mc *memcache) Lookup(id string) *Item {
	mc.m.RLock()
	v := mc.kv[id]
	mc.m.RUnlock()
	return v
}

func (mc *memcache) Set(id string, item *Item) {
	mc.m.Lock()
	mc.kv[id] = item
	mc.m.Unlock()
}
