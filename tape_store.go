package bendo

import (
	"archive/zip"
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
	fname, err := s.mostRecent(id)
	if err != nil {
		return nil, err
	}
	// read the zip file and extract the item record
	return readZipFile(fname)
}

const (
	maxSaveIterations = 100

	// A given item named abcdefg will be stored as a group of key-value
	// pairs. Each key will have a name in the form:
	//     abcdefg-VVVV-I
	// where VVVV is the `version number`, and `I` is the iteration number.
	//
	// The FS store will group keys into a directory hierarchy which has
	// the form
	//     ab/cd/abcdefg-VVVV-I
	fileTemplate = "%s-%04d-%d.zip"
)

var (
	ErrNoItem        = errors.New("No such item")
	ErrNoBlob        = errors.New("No such blob")
	ErrVersionExists = errors.New("Given Version already exists")
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
	version-- // because we stopped upon not finding a version
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

// open and read the json metadata file from fname.
func readZipFile(fname string) (*Item, error) {
	var result *Item
	z, err := zip.OpenReader(fname)
	if err != nil {
		return nil, err
	}
	defer z.Close()

	// find info file
	for _, f := range z.File {
		if f.Name != "item-info.json" {
			continue
		}
		var rc io.ReadCloser
		rc, err = f.Open()
		if err == nil {
			result, err = readItemInfo(rc)
			rc.Close()
		}
		break
	}
	return result, err
}

// Track the open archive file so it can be closed.
// the embedded ReadCloser is for the actual blob content.
type archiveStream struct {
	zipFd *zip.ReadCloser
	io.ReadCloser
}

func (as *archiveStream) Close() error {
	as.ReadCloser.Close()
	return as.zipFd.Close()
}

func (s *store) BlobContent(id string, version VersionID, bid BlobID) (io.ReadCloser, error) {
	// find zip file
	iteration := findIteration(id, int(version))
	if iteration == -1 {
		return nil, ErrNoBlob
	}
	fname := fmt.Sprintf(fileTemplate, id, version, iteration)
	return s.readZipStream(fname, bid)
}

func (s *store) readZipStream(fname string, blob BlobID) (*archiveStream, error) {
	z, err := zip.OpenReader(fname)
	if err != nil {
		return nil, err
	}
	// don't leak the open z. Close it if we did not make
	// an archiveStream to hold it.
	var result *archiveStream
	defer func() {
		if result == nil {
			z.Close()
		}
	}()

	streamName := fmt.Sprintf("blob/%d", blob)
	for _, f := range z.File {
		if f.Name != streamName {
			continue
		}
		var rc io.ReadCloser
		rc, err = f.Open()
		if err == nil {
			result = &archiveStream{
				zipFd:      z,
				ReadCloser: rc,
			}
		}
		break
	}
	return result, err
}

func (s *store) SaveItem(item *Item, v VersionID, bd []BlobData) error {
	// make sure there is not already a version there
	iteration := findIteration(item.ID, int(v))
	if iteration != -1 {
		// this version already exists on tape
		return ErrVersionExists
	}
	// Then open a new zip file
	fname := ""
	f, err := os.Create(fname)
	if err != nil {
		return err
	}
	defer f.Close()
	z := zip.NewWriter(f)
	defer z.Close()
	// write out the item data
	header := zip.FileHeader{
		Name:   "item-info.json",
		Method: zip.Store,
	}
	w, err := z.CreateHeader(&header)
	if err != nil {
		return err
	}
	err = writeItemInfo(w, item)
	if err != nil {
		return err
	}
	// write out all the blobs
	for _, blob := range bd {
		header := zip.FileHeader{
			Name:   "blob/%d", // blob.id
			Method: zip.Store,
		}
		w, err := z.CreateHeader(&header)
		if err != nil {
			return err
		}
		_, err = io.Copy(w, blob.r)
		if err != nil {
			return err
		}
	}
	return nil
}
