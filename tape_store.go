package bendo

import (
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

func NewTapeStore() BundleStore {
	return nil
}

func NewFSStore() BundleStore {
	return nil
}

func (s *store) ItemList() <-chan string {
	c := make(chan string)
	go walkTree(c, s.root, true)
	return c
}

// walk tree at root, emiting all unique item identifiers on channel out.
// if toplevel is true, the channel is closed when the function exits.
func walkTree(out chan<- string, root string, toplevel bool) {
	defer func() {
		if toplevel {
			close(out)
		}
	}()
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
			// log the error since we have no way of otherwise
			// passing it back
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

func (s *store) Item(id string) (*Item, error) {
	return nil, nil
}

func (s *store) BlobContent(bid XBid) (io.Reader, error) {
	return nil, nil
}
