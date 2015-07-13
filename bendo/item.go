package bendo

import (
	"fmt"
	"io"
	"strconv"
	"strings"
)

//// handle interface to loading and saving items

// We have two levels of item knowledge:
//   1. We know an item exists and the extent of its bundle numbers
//   2. We have read a bundle and know the full item metadata
// We can get (1) by just stating files. So that is what we use to begin with.
// Knowledge of type (2) requires reading from tape. So we will fill it in
// on demand and selectively in the background.

// A romp is our item metadata registry
type romp struct {
	// the metadata cache store...the authoritative source is the bundles
	items map[string]*Item

	maxBundle map[string]int

	// the underlying bundle store
	s BundleStore
}

func NewRomp(s BundleStore) T {
	return &romp{
		items:     make(map[string]*Item),
		maxBundle: make(map[string]int),
		s:         s,
	}
}

// this is not thread safe on rmp
func (rmp *romp) ItemList() <-chan string {
	out := make(chan string)
	go func() {
		rmp.updateMaxBundle()
		for k, _ := range rmp.maxBundle {
			out <- k
		}
		close(out)
	}()
	return out
}

func (rmp *romp) updateMaxBundle() {
	var maxes = make(map[string]int)
	c := rmp.s.List()
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
	rmp.maxBundle = maxes // not thread safe because of this
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

func (rmp *romp) Item(id string) (*Item, error) {
	item, ok := rmp.items[id]
	if ok {
		return item, nil
	}
	// get the highest version number somehow
	var n int
	rc, err := rmp.openZipStream(sugar(id, n), "item-info.json")
	if err != nil {
		return nil, err
	}
	result, err := readItemInfo(rc)
	rc.Close()
	return result, err
}

//func (rmp *romp) BlobContent(id string, n int, b BlobID) (io.ReadCloser, error) {
func (rmp *romp) Blob(id string, b BlobID) (io.ReadCloser, error) {
	var n = 1
	// which bundle is this blob in?

	sname := fmt.Sprintf("blob/%d", b)
	return rmp.openZipStream(sugar(id, n), sname)
}

func (rmp *romp) Validate(id string) (int64, []string, error) {
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
