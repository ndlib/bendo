package bendo

import (
	"errors"
	"fmt"
	"io"
	"log"
	"net/url"
	"os"
	"path/filepath"
	"strings"
)

// store is used to implement both the TapeStore and the FSStore.
// The difference is whether there is a URL to send requests to the
// Dternity API.
type store struct {
	root string
	rest url.URL
}

func NewTapeStore(root string, api url.URL) BundleStore {
	return &store{root: root, rest: api}
}

func NewFSStore(root string) BundleStore {
	return &store{root: root}
}

func (s *store) ItemList() <-chan string {
	c := make(chan string)
	go walkTree(c, s.root, true)
	return c
}

// Perform depth first walk of file tree at root, emitting all unique item
// identifiers on channel out. Be careful to only open directories and stat
// files. Otherwise we might trigger a blocking request on the tape system.
//
// If toplevel is true, the channel is closed when the function exits.
func walkTree(out chan<- string, root string, toplevel bool) {
	if toplevel {
		defer close(out)
	}
	seen := make(map[string]struct{})
	f, err := os.Open(root)
	if err != nil {
		log.Println(err)
		return
	}
	defer f.Close()
	for {
		entries, err := f.Readdir(1000)
		if err == io.EOF {
			return
		} else if err != nil {
			// we have no other way of passing this error back
			log.Println(err)
			return
		}
		for _, e := range entries {
			if e.IsDir() {
				p := filepath.Join(root, e.Name())
				walkTree(out, p, false)
				continue
			}
			id := decodeID(e.Name())
			_, ok := seen[id]
			if !ok {
				seen[id] = struct{}{}
				out <- id
			}
		}
	}
}

// extract the id from the stored zip file names.
// e.g. "xyz-0001-1.zip" is mapped to "xyz"
func decodeID(s string) string {
	i := strings.Index(s, "-")
	if i == -1 {
		return ""
	}
	return s[:i]
}

// Return the metadata for the specified item
func (s *store) Item(id string) (*Item, error) {
	// find the most recent zip for this item
	return nil, nil
}

const (
	maxSaveIterations = 100
	fileTemplate      = "%s-%04d-%d.zip"
)

var (
	ErrNoItem = errors.New("No such item")
)

// find the name of the zip file with the largest version number for item id.
func (s *store) mostRecent(id string) (string, error) {
	p := filepath.Join(s.root, itemSubdir(id), id)
	var version int
	var iteration int
	for version = 1; ; version++ {
		i := findIteration(p, version)
		if i == -1 {
			break
		}
		iteration = i
	}
	version--
	if version == 0 {
		// there are no matching zip files
		return "", ErrNoItem
	}
	return fmt.Sprintf(fileTemplate, p, version, iteration), nil
}

// return -1 if no iteration could be found.
func findIteration(p string, version int) int {
	for i := 1; i <= maxSaveIterations; i++ {
		fname := fmt.Sprintf(fileTemplate, p, version, i)
		_, err := os.Stat(fname)
		if err == nil {
			return i
		}
	}
	return -1
}

// Given an item id, return the subdirectory the item's file are stored in
// e.g. "abcdd123" returns "ab/cd/"
func itemSubdir(id string) string {
	var result = make([]byte, 0, 8)
	var count int
	for j := range id {
		result = append(result, id[j])
		count++
		if count == 2 {
			result = append(result, '/')
		}
		if count >= 4 {
			break
		}
	}
	if count != 2 {
		result = append(result, '/')
	}
	return string(result)
}

func (s *store) BlobContent(bid XBid) (io.Reader, error) {
	return nil, nil
}

func (s *store) SaveItem(item *Item, bd []BlobData) error {
	return nil
}
