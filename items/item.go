package items

import (
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/golang/groupcache/singleflight"
)

// We have two levels of item knowledge:
//   1. We know an item exists and the extent of its bundle numbers
//   2. We have read a bundle and know the full item metadata
// We can get (1) by just stating files. So that is what we use to begin with.
// Knowledge of type (2) requires reading from tape. So we will fill it in
// on demand and selectively in the background.

type Store struct {
	cache ItemCache
	S     BundleStore        // the underlying bundle store
	table singleflight.Group // for metadata lookups. keyed by item id
}

func New(s BundleStore) *Store {
	return &Store{S: s, cache: nullcache}
}

func NewWithCache(s BundleStore, cache ItemCache) *Store {
	return &Store{S: s, cache: cache}
}

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

// Load and return an item's metadata info. This will block until the
// item is loaded.
func (s *Store) Item(id string) (*Item, error) {
	result := s.cache.Lookup(id)
	if result != nil && len(result.Versions) > 0 {
		return result, nil // a complete item record
	}
	val, err := s.table.Do(id, func() (interface{}, error) {
		v, err := s.itemload(id)
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
// Returns 0 if the item does not exist in the BundleStore.
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

// Return an io.ReadCloser containing the given blob's contents.
// Will block until the item and blob are loaded from tape.
func (s *Store) Blob(id string, bid BlobID) (io.ReadCloser, error) {
	item, err := s.Item(id)
	if err != nil {
		return nil, err
	}
	b := item.blobByID(bid)
	if b == nil {
		return nil, fmt.Errorf("No blob (%s, %s)", id, bid)
	}
	sname := fmt.Sprintf("blob/%d", bid)
	return OpenBundleStream(s.S, sugar(id, b.Bundle), sname)
}

func (s *Store) Validate(id string) (int64, []string, error) {
	return 0, nil, nil
}

func (item Item) blobByID(id BlobID) *Blob {
	for _, b := range item.Blobs {
		if b.ID == id {
			return b
		}
	}
	return nil
}

type cache struct{}

var nullcache cache

func (_ cache) Lookup(id string) *Item    { return nil }
func (_ cache) Set(id string, item *Item) {}
