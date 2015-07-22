package items

import (
	"io"
	"log"
	"os"
	"path"
	"path/filepath"
	"strings"
)

// fileStore implements the simple file system based bundle store. It is
// used by the dternity store, so make sure it doesn't open files unless
// necessary.
type fileStore struct {
	root string
}

func NewFileStore(root string) BundleStore {
	return &fileStore{root}
}

func (s *fileStore) List() <-chan string {
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

func (s *fileStore) ListPrefix(prefix string) ([]string, error) {
	var glob string
	switch len(prefix) {
	case 0:
		glob = "*/*"
	case 1:
		glob = prefix + "*/*"
	case 2:
		glob = prefix[0:2] + "/*"
	case 3:
		glob = prefix[0:2] + "/" + prefix[2:3] + "*"
	default:
		glob = prefix[0:2] + "/" + prefix[2:4]
	}
	glob = filepath.Join(s.root, glob, prefix+"*")
	result, err := filepath.Glob(glob)
	if err == nil {
		for i := range result {
			r := path.Base(result[i])
			result[i] = strings.TrimSuffix(r, ".zip")
		}
	}
	return result, err
}

func (s *fileStore) Open(key, id string) (ReadAtCloser, int64, error) {
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

func (s *fileStore) Create(key, id string) (io.WriteCloser, error) {
	var w io.WriteCloser
	dir := filepath.Join(s.root, itemSubdir(id))
	err := os.MkdirAll(dir, 0775)
	if err == nil {
		fname := filepath.Join(dir, key+".zip")
		// pass the O_EXCL flag explicitly to prevent overwriting
		// already existing files
		w, err = os.OpenFile(fname, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0666)
	}
	return w, err
}

func (s *fileStore) Delete(key, id string) error {
	fname := filepath.Join(s.root, itemSubdir(id), key+".zip")
	return os.Remove(fname)
}

// Given an item id, return the subdirectory the item's file are stored in
// e.g. "abcdd123" returns "ab/cd/"
func itemSubdir(id string) string {
	var result string
	switch len(id) {
	case 0:
		result = "./"
	case 1:
		result = id + "/"
	case 2:
		result = id + "/"
	case 3:
		result = id[0:2] + "/" + id[2:3] + "/"
	default:
		result = id[0:2] + "/" + id[2:4] + "/"
	}
	return result
}
