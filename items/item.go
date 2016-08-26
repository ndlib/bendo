package items

import (
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"

	"github.com/ndlib/bendo/store"
)

// A Store holds a collection of items
type Store struct {
	cache    ItemCache
	S        store.Store // the underlying bundle store
	useStore bool        // true - use bundlestore: false - use only itemCache
}

// New creates a new item store which writes its bundles to the given store.Store.
func New(s store.Store) *Store {
	return &Store{S: s, cache: Nullcache, useStore: true}
}

// NewWithCache creates a new item store which caches the item metadata in the
// given cache. (Should be deprecated??)
func NewWithCache(s store.Store, cache ItemCache) *Store {
	return &Store{S: s, cache: cache, useStore: true}
}

// SetCache will set the metadata cache used. It is intended to be used during
// initialization. It will cause a race condition if used while others are
// accessing this item store.
func (s *Store) SetCache(cache ItemCache) {
	s.cache = cache
}

// SetUseStore enables or disables access to the underlying store. true- on/ false-off 
func (s *Store) SetUseStore(value bool) {
	s.useStore = value
}

// List returns a channel which will contain all of the item ids in the current
// store.
func (s *Store) List() <-chan string {
	out := make(chan string)
	go func() {
		items := make(map[string]struct{})
		c := s.S.List()
		for key := range c {
			id, _ := desugar(key)
			if id == "" {
				continue
			}
			_, ok := items[id]
			if !ok {
				items[id] = struct{}{}
				out <- id
			}
		}
		close(out)
	}()
	return out
}

// Turn an item id and a bundle number n into a string key.
func sugar(id string, n int) string {
	return fmt.Sprintf("%s-%04d.zip", id, n)
}

// Extract an item id and a bundle number from a string key.
// Returns an id of "" if the key could not be decoded.
func desugar(s string) (id string, n int) {
	s = strings.TrimSuffix(s, ".zip")
	j := strings.LastIndex(s, "-")
	if j == -1 {
		return "", 0
	}
	id = s[0:j]
	n64, err := strconv.ParseInt(s[j+1:], 10, 0)
	if err != nil {
		return "", 0
	}
	n = int(n64)
	return
}

var (
	// ErrNoItem occurs when an item is requested for which no bundle
	// files could be found in the backing store.
	ErrNoItem = errors.New("no item, bad item id")
	// ErrNoStore occurs when useStore has been set to false-
	// backing store is unavailable.
	ErrNoStore = errors.New("no item, item store unavailable")
)

// Item loads and return an item's metadata info. This will block until the
// item is loaded.
func (s *Store) Item(id string) (*Item, error) {
	result := s.cache.Lookup(id)
	if result != nil && len(result.Versions) > 0 {
		return result, nil // a complete item record
	}
	// if item not in cache, and useStore disabled, return ErrNoStore Error error
	if  s.useStore == false {
		return result, ErrNoStore
	}

	result, err := s.itemload(id)
	if err == nil {
		s.cache.Set(id, result)
	}
	return result, err
}

// load an item into memory from the store
func (s *Store) itemload(id string) (*Item, error) {
	n := s.findMaxBundle(id)
	if n == 0 {
		return nil, ErrNoItem
	}
	rc, err := OpenBundleStream(s.S, sugar(id, n), "item-info.json")
	if err != nil {
		return nil, err
	}
	defer rc.Close()
	item, err := readItemInfo(rc)
	if err == nil {
		item.MaxBundle = n
	}
	return item, err
}

// Find the maximum bundle for the given id.
// Returns 0 if the item does not exist in the store.
func (s *Store) findMaxBundle(id string) int {
	bundles, err := s.S.ListPrefix(id)
	if err != nil {
		// TODO(dbrower): log the error or just lose it?
		return 0
	}
	var max int
	for _, b := range bundles {
		slug, n := desugar(b)
		if slug == id && n > max {
			max = n
		}
	}
	return max
}

// Blob returns an io.ReadCloser containing the given blob's contents and
// the blob's size.
// It will block until the item and blob are loaded from the backing store.
//
// TODO: perhaps this should be moved to be a method on an Item*
func (s *Store) Blob(id string, bid BlobID) (io.ReadCloser, int64, error) {
	b, err := s.BlobInfo(id, bid)
	if err != nil {
		return nil, 0, err
	}
	if b.Bundle == 0 {
		// blob has been deleted
		return nil, 0, fmt.Errorf("Blob has been deleted")
	}
	sname := fmt.Sprintf("blob/%d", bid)
	stream, err := OpenBundleStream(s.S, sugar(id, b.Bundle), sname)
	return stream, b.Size, err
}

// BlobInfo returns a pointer to a Blob structure containing information
// on the given blob. It is like Blob() but doesn't recall the content
// from tape. Unlike Blob(), though, it will not return an error if the blob is
// deleted.
func (s *Store) BlobInfo(id string, bid BlobID) (*Blob, error) {
	item, err := s.Item(id)
	if err != nil {
		return nil, err
	}
	b := item.blobByID(bid)
	if b == nil {
		return nil, fmt.Errorf("No blob (%s, %d)", id, bid)
	}
	return b, nil
}

func (item Item) blobByID(id BlobID) *Blob {
	for _, b := range item.Blobs {
		if b.ID == id {
			return b
		}
	}
	return nil
}

// BlobByVersionSlot returns the blob corresponding to the given version
// identifier and slot name. It returns 0 if the (version id, slot) pair do
// not resolve to anything.
func (item Item) BlobByVersionSlot(vid VersionID, slot string) BlobID {
	var ver *Version
	for _, v := range item.Versions {
		if v.ID == vid {
			ver = v
			break
		}
	}
	if ver == nil {
		return 0
	}
	return ver.Slots[slot]
}

// BlobByExtendedSlot return the blob idenfifer for the given extended slot
// name. An extended slot name is a slot name with an optional "@nnn/" prefix,
// where nnn is the version number of the item to use (in decimal). If a
// version prefix is not present, the most recent version of the item is used.
// Like BlobByVersionSlot, 0 is returned if the slot path does not
// resolve to anything.
func (item Item) BlobByExtendedSlot(slot string) BlobID {
	var vid VersionID
	var vmax = item.Versions[len(item.Versions)-1].ID
	// is this a special slot name?
	if len(slot) >= 1 && slot[0] == '@' {
		// handle "@blob/nnn" path
		if strings.HasPrefix(slot, "@blob/") {
			// try to parse the blob number
			b, err := strconv.ParseInt(slot[6:], 10, 0)
			if err != nil || b <= 0 || b > int64(len(item.Blobs)) {
				return 0
			}
			return BlobID(b)
		}
		// handle "@nnn/path/to/file" paths
		var err error
		j := strings.Index(slot, "/")
		if j >= 1 {
			var v int64
			// start from index 1 to skip initial "@"
			v, err = strconv.ParseInt(slot[1:j], 10, 0)
			vid = VersionID(v)
		}
		// if j was invalid, then vid == 0, so following will catch it
		if err != nil || vid <= 0 || vid > vmax {
			return 0
		}
		slot = slot[j+1:]
	} else {
		vid = vmax
	}
	return item.BlobByVersionSlot(vid, slot)
}

// used to implement a no-op cache
type cache struct{}

// The Nullcache is an ItemCache which does not store anything.
var Nullcache cache

func (c cache) Lookup(id string) *Item    { return nil }
func (c cache) Set(id string, item *Item) {}

// NewMemoryCache returns an empty ItemCache that keeps everything in memory
// and never evicts anything. It is probably only useful in tests.
func NewMemoryCache() ItemCache {
	return &memoryCache{}
}

type memoryCache struct {
	m      sync.RWMutex
	memory map[string]*Item
}

func (c *memoryCache) Lookup(id string) *Item {
	var result *Item
	c.m.RLock()
	if c.memory != nil {
		result = c.memory[id]
	}
	c.m.RUnlock()
	return result
}

func (c *memoryCache) Set(id string, item *Item) {
	c.m.Lock()
	if c.memory == nil {
		// lazily create the cache store
		c.memory = make(map[string]*Item)
	}
	c.memory[id] = item
	c.m.Unlock()
}
