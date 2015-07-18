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

func NewTapeStore(root string, api url.URL) BundleStore {
	return &store{root: root, rest: api}
}

func NewFSStore(root string) BundleStore {
	return &store{root: root}
}

func (s *store) List() <-chan string {
	c := make(chan string)
	go walkTree(c, s.root, true)
	return c
}

func (s *store) ListPrefix(prefix string) ([]string, error) {
	if len(prefix) < 4 {
		// this item is stored across more than one directory
	} else {
		return s.getprefix(prefix)
	}
	return nil, nil
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
			out <- strings.TrimSuffix(e.Name(), ".zip")
		}
	}
}

func (s *store) Open(key, id string) (ReadAtCloser, int64, error) {
	fname := filepath.Join(s.root, itemSubdir(id), key+".zip")
	f, err := os.Open(fname)
	if err != nil {
		return nil, 0, err
	}
	fi, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, 0, err
	}
	return f, fi.Size(), nil
}

func (s *store) Create(key, id string) (io.WriteCloser, error) {
	var w io.WriteCloser
	dir := filepath.Join(s.root, itemSubdir(id))
	err := os.MkdirAll(dir, 0775)
	if err == nil {
		fname := filepath.Join(dir, key+".zip")
		w, err = os.Create(fname)
	}
	return w, err
}

func (s *store) Delete(key, id string) error {
	fname := filepath.Join(s.root, itemSubdir(id), key+".zip")
	return os.Remove(fname)
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

func (s *store) getprefix(prefix string) ([]string, error) {
	dir := filepath.Join(s.root, itemSubdir(prefix)) + "/"
	glob := dir + prefix + "*"
	result, err := filepath.Glob(glob)
	if err == nil {
		for i := range result {
			r := strings.TrimPrefix(result[i], dir)
			result[i] = strings.TrimSuffix(r, ".zip")
		}
	}
	return result, err
}
