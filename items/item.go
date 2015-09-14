package items

import (
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/ndlib/bendo/store"
)

type Store struct {
	cache ItemCache
	S     store.Store // the underlying bundle store
}

func New(s store.Store) *Store {
	return &Store{S: s, cache: nullcache}
}

func NewWithCache(s store.Store, cache ItemCache) *Store {
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

// Turn an item id and a bundle number n into a string key.
func sugar(id string, n int) string {
	return fmt.Sprintf("%s-%04d.zip", id, n)
}

// Extract an item id and a bundle number from a string key.
// Returns an id of "" if the key could not be decoded.
func desugar(s string) (id string, n int) {
	s = strings.TrimSuffix(s, ".zip")
	pieces := strings.Split(s, "-")
	if len(pieces) != 2 {
		return "", 0
	}
	id = pieces[0]
	n64, err := strconv.ParseInt(pieces[1], 10, 0)
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

// Return an io.ReadCloser containing the given blob's contents.
// Will block until the item and blob are loaded from tape.
func (s *Store) Blob(id string, bid BlobID) (io.ReadCloser, error) {
	item, err := s.Item(id)
	if err != nil {
		return nil, err
	}
	b := item.blobByID(bid)
	if b == nil {
		return nil, fmt.Errorf("No blob (%s, %d)", id, bid)
	}
	if b.Bundle == 0 {
		// blob has been deleted
		return nil, fmt.Errorf("Blob has been deleted")
	}
	sname := fmt.Sprintf("blob/%d", bid)
	return OpenBundleStream(s.S, sugar(id, b.Bundle), sname)
}

func (item Item) blobByID(id BlobID) *Blob {
	for _, b := range item.Blobs {
		if b.ID == id {
			return b
		}
	}
	return nil
}

// Given a version identifier and a slot name, returns the corresponding blob
// identifier. Returns 0 if the vid, slot pair does not resolve to anything.
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
	for name, bid := range ver.Slots {
		if name == slot {
			return bid
		}
	}
	return 0
}

// used to implement a no-op cache
type cache struct{}

var nullcache cache

func (_ cache) Lookup(id string) *Item    { return nil }
func (_ cache) Set(id string, item *Item) {}
