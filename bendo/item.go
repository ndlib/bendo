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

// A Directory is our item metadata registry
type Directory struct {
	// the metadata cache store...the authoritative source is the bundles
	cache ItemCache

	maxBundle map[string]int
	scanned   bool

	// the underlying bundle store
	s BundleStore
}

func NewRomp(s BundleStore) T {
	return &Directory{
		cache:     NewMemoryCache(),
		maxBundle: make(map[string]int),
		s:         s,
	}
}

// this is not thread safe on dty
func (dty *Directory) ItemList() <-chan string {
	out := make(chan string)
	go func() {
		dty.updateMaxBundle()
		for k, _ := range dty.maxBundle {
			out <- k
		}
		close(out)
	}()
	return out
}

func (dty *Directory) updateMaxBundle() {
	var maxes = make(map[string]int)
	c := dty.s.List()
	for key := range c {
		id, n := desugar(key)
		if id == "" {
			continue
		}
		mx := maxes[id]
		if n > mx {
			maxes[id] = n
		}
	}
	dty.maxBundle = maxes // not thread safe because of this
	dty.scanned = true    // and this
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
	if item == nil {
		return item, nil
	}
	if !dty.scanned {
		return nil, ErrTryAgain
	}
	// get the highest version number somehow
	n := dty.maxBundle[id]
	if n == 0 {
		return nil, ErrNoItem
	}
	rc, err := dty.openZipStream(sugar(id, n), "item-info.json")
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

func (dty *Directory) Blob(id string, b BlobID) (io.ReadCloser, error) {
	var n = 1
	// which bundle is this blob in?

	sname := fmt.Sprintf("blob/%d", b)
	return dty.openZipStream(sugar(id, n), sname)
}

func (dty *Directory) Validate(id string) (int64, []string, error) {
	return 0, nil, nil
}

type byID []*Blob

func (p byID) Len() int           { return len(p) }
func (p byID) Less(i, j int) bool { return p[i].ID < p[j].ID }
func (p byID) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

func (item *Item) blobByID(id BlobID) *Blob {
	for _, b := range item.blobs {
		if b.ID == id {
			item.m.Unlock()
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
