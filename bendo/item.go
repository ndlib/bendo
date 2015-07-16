package bendo

import (
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"
)

//// handle interface to loading and saving items

// We have two levels of item knowledge:
//   1. We know an item exists and the extent of its bundle numbers
//   2. We have read a bundle and know the full item metadata
// We can get (1) by just stating files. So that is what we use to begin with.
// Knowledge of type (2) requires reading from tape. So we will fill it in
// on demand and selectively in the background.

// We keep a goroutine in charge of the metadata registry.
// to get an item record, one should:
// 1) first check the cache
// 2) if not in the cache, ask the registry via the channel
// 3) you will get either a) the record, b) an error to try again, c) an error of
//    no such item.
//
// To modify an item, first get it.
// Or try to create it (ask the registry)
// Then ask to lock it for updating
// Then either submit back the updated item record or a cancel request (we assume
// you have saved the item already if you are updating it. the registry is only
// concerned with updating the cache)

// A Directory is our item metadata registry
type Directory struct {
	// the metadata cache store...the authoritative source is the bundles
	cache ItemCache

	// the underlying bundle store
	s BundleStore
}

func NewDirectory(s BundleStore) T {
	dty := &Directory{
		cache: NewMemoryCache(),
		s:     s,
	}
	return dty
}

// this is not thread safe on dty
func (dty *Directory) ItemList() <-chan string {
	out := make(chan string)
	go func() {
		// need to do something here
	}()
	return out
}

// turn an item id and a bundle number n into a string key
func sugar(id string, n int) string {
	return fmt.Sprintf("%s-%04d", id, n)
}

// extract an item id and a bundle number from a string key
// return an id of "" if the key could not be decoded
func desugar(s string) (id string, n int) {
	z := strings.Split(s, "-")
	if len(z) != 2 {
		return "", 0
	}
	id = z[0]
	n64, err := strconv.ParseInt(z[1], 10, 0)
	if err != nil {
		return "", 0
	}
	n = int(n64)
	return
}

var (
	ErrTryAgain = errors.New("scanning directories, try again")
	ErrNoItem   = errors.New("no item, bad item id")
)

func (dty *Directory) Item(id string) (*Item, error) {
	item := dty.cache.Lookup(id)
	if item != nil {
		return item, nil
	}
	c := make(chan int)
	//dty.maxBundle <- scan{id: id, c: c}
	n := <-c
	if n == 0 {
		return nil, ErrTryAgain
	} else if n == -1 {
		return nil, ErrNoItem
	}
	rc, err := OpenBundleStream(dty.s, sugar(id, n), "item-info.json")
	if err != nil {
		return nil, err
	}
	result, err := readItemInfo(rc)
	rc.Close()
	if err == nil {
		dty.cache.Set(id, result)
	}
	return result, err
}

func (dty *Directory) Blob(id string, bid BlobID) (io.ReadCloser, error) {
	item, err := dty.Item(id)
	if err != nil {
		return nil, err
	}
	b := item.blobByID(bid)
	if b == nil {
		return nil, fmt.Errorf("No blob (%s, %s)", id, bid)
	}
	sname := fmt.Sprintf("blob/%d", bid)
	return OpenBundleStream(dty.s, sugar(id, b.Bundle), sname)
}

func (dty *Directory) Validate(id string) (int64, []string, error) {
	return 0, nil, nil
}

func (item *Item) blobByID(id BlobID) *Blob {
	for _, b := range item.blobs {
		if b.ID == id {
			return b
		}
	}
	return nil
}

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
