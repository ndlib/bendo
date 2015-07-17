package bendo

import (
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
)

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

type itemstore struct {
	// the underlying bundle store
	s BundleStore
}

func New(s BundleStore) ItemStore {
	return &itemstore{
		s: s,
	}
}

func (is *itemstore) List() <-chan string {
	out := make(chan string)
	go func() {
		items := make(map[string]struct{})
		c := is.s.List()
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
func (is *itemstore) Item(id string) (*Item, error) {
	n := is.findMaxBundle(id)
	if n == 0 {
		return nil, ErrNoItem
	}
	rc, err := OpenBundleStream(is.s, sugar(id, n), "item-info.json")
	if err != nil {
		return nil, err
	}
	defer rc.Close()
	return readItemInfo(rc)
}

// Find the maximum bundle for the given id.
// Returns 0 if the item does not exist in the BundleStore.
func (is *itemstore) findMaxBundle(id string) int {
	bundles, err := is.s.ListPrefix(id)
	if err != nil {
		// TODO(dbrower): log the error or just lose it?
		return 0
	}
	var max int
	for _, b := range bundles {
		s, n := desugar(b)
		if s == id && n > max {
			max = n
		}
	}
	return max
}

// Return an io.ReadCloser containing the given blob's contents.
// Will block until the item and blob are loaded from tape.
func (is *itemstore) Blob(id string, bid BlobID) (io.ReadCloser, error) {
	item, err := is.Item(id)
	if err != nil {
		return nil, err
	}
	b := item.blobByID(bid)
	if b == nil {
		return nil, fmt.Errorf("No blob (%s, %s)", id, bid)
	}
	sname := fmt.Sprintf("blob/%d", bid)
	return OpenBundleStream(is.s, sugar(id, b.Bundle), sname)
}

func (is *itemstore) Validate(id string) (int64, []string, error) {
	return 0, nil, nil
}

func (is *itemstore) BundleStore() BundleStore {
	return is.s
}

func (item Item) blobByID(id BlobID) *Blob {
	for _, b := range item.blobs {
		if b.ID == id {
			return b
		}
	}
	return nil
}
