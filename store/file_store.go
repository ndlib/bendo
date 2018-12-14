package store

import (
	"errors"
	"io"
	"log"
	"os"
	"path"
	"path/filepath"
	"strings"
	"unicode"
	"unicode/utf8"

	raven "github.com/getsentry/raven-go"
)

// FileSystem implements the simple file system based store. It tries to
// only open files when necessary, so it could be backed by a tape system,
// for example.
// The keys are used as file names. This means keys should not contain a
// forward slash character '/'. Also, if you want the files to have a
// specific file extension, you need to add it to your key.
type FileSystem struct {
	root string
}

const (
	// the subdir to store files while they are being written to.
	scratchdir = "scratch"
)

var (
	// make sure it implements the Store interface
	_ Store = &FileSystem{}

	// ErrKeyExists indicates an attempt to create a key which already exists
	ErrKeyExists = errors.New("Key already exists")

	// ErrKeyContainsSlash means the key provided contains a forward slash '/'
	ErrKeyContainsSlash = errors.New("Key contains forward slash")

	// ErrKeyContainsNonUnicode means the key provided contains a Non Unicode Rune
	ErrKeyContainsNonUnicode = errors.New("Key contains Non-Unicode character")

	// ErrKeyContainsWhiteSpace  means the key provided contains WhiteSpace
	ErrKeyContainsWhiteSpace = errors.New("Key contains White Space")

	// ErrKeyContainsControlChar  means the key provided contains Control Characters
	ErrKeyContainsControlChar = errors.New("Key contains Control  Characters")
)

// NewFileSystem creates a new FileSystem store based at the given root path.
func NewFileSystem(root string) *FileSystem {
	return &FileSystem{root}
}

// List returns a channel listing all the keys in this store.
func (s *FileSystem) List() <-chan string {
	c := make(chan string)
	go walkTree(c, s.root, 0)
	return c
}

// Perform depth first walk of file tree at root, emitting all unique item
// keys on channel out. Be careful to only open directories and stat
// files. Otherwise we might trigger a blocking request on the tape system.
//
// If level is 0, the channel is closed when the function exits.
func walkTree(out chan<- string, root string, level int) {
	if level == 0 {
		defer close(out)
	}
	f, err := os.Open(root)
	if err != nil {
		log.Println(err)
		raven.CaptureError(err, nil)
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
			raven.CaptureError(err, nil)
			return
		}
		for _, e := range entries {
			// only decend at most two directories down, and only
			// list files in the second level. 0/1/2
			if e.IsDir() {
				if level < 2 {
					p := filepath.Join(root, e.Name())
					walkTree(out, p, level+1)
				}
				continue
			}
			if level != 2 {
				continue
			}
			out <- e.Name()
		}
	}
}

// ListPrefix returns a list of all the keys beginning with the given prefix.
func (s *FileSystem) ListPrefix(prefix string) ([]string, error) {
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
			result[i] = path.Base(result[i])
		}
	}
	return result, err
}

// Open returns a reader for the given object along with its size.
func (s *FileSystem) Open(key string) (ReadAtCloser, int64, error) {
	if strings.Contains(key, "/") {
		return nil, 0, ErrKeyContainsSlash
	}
	fname := filepath.Join(s.root, itemSubdir(key), key)
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

// Create creates a new item with the given key, and a writer to allow for
// saving data into the new item.
func (s *FileSystem) Create(key string) (io.WriteCloser, error) {

	// Perform Key Name Validation
	err := isKeyValid(key)

	if err != nil {
		return nil, err
	}
	var w io.WriteCloser
	// first set up the eventual home dir of this file
	target, err := s.setupSubDir(itemSubdir(key), key)
	if err != nil {
		return nil, err
	}
	_, err = os.Stat(target)
	if !os.IsNotExist(err) {
		return nil, ErrKeyExists
	}
	// now set up the scratch location we will temporially save the file to
	temp, err := s.setupSubDir(scratchdir, key)
	if err != nil {
		return nil, err
	}
	// pass the O_EXCL flag explicitly to prevent overwriting
	// already existing files
	w, err = os.OpenFile(temp, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0666)
	if err != nil {
		return nil, err
	}
	return &moveCloser{w, temp, target}, nil
}

// setupSubDir makes sure the given subdirectory exists under the root, and
// then returns the absolute path to the keyed file, and an optional error.
func (s *FileSystem) setupSubDir(subdir, key string) (string, error) {
	dir := filepath.Join(s.root, subdir)
	err := os.MkdirAll(dir, 0775)
	return filepath.Join(dir, key), err
}

// track the file so when it is closed, we can move it into the correct place
type moveCloser struct {
	io.WriteCloser
	source string
	target string
}

func (w *moveCloser) Close() error {
	err := w.WriteCloser.Close()
	if err != nil {
		return err
	}
	_, err = os.Stat(w.target)
	if !os.IsNotExist(err) {
		return ErrKeyExists
	}
	return os.Rename(w.source, w.target)
}

// Delete the given key from the store. It is not an error if the key doesn't
// exist.
func (s *FileSystem) Delete(key string) error {
	if strings.Contains(key, "/") {
		return ErrKeyContainsSlash
	}
	fname1 := filepath.Join(s.root, itemSubdir(key), key)
	err := os.Remove(fname1)
	// don't report a missing file as an error
	if err != nil && os.IsNotExist(err) {
		err = nil
	}
	return err
}

// Given an item key, return the subdirectory the item's file are stored in
// e.g. "abcdd123" returns "ab/cd/"
func itemSubdir(key string) string {
	var result string
	switch len(key) {
	case 0:
		result = "./"
	case 1:
		result = key + "/"
	case 2:
		result = key + "/"
	case 3:
		result = key[0:2] + "/" + key[2:3] + "/"
	default:
		result = key[0:2] + "/" + key[2:4] + "/"
	}
	return result
}

// Some Simple Item Key Validations
func isKeyValid(key string) error {

	// Valid Unicode

	if !utf8.ValidString(key) {
		return ErrKeyContainsNonUnicode
	}

	// No Slashes
	if strings.Contains(key, "/") {
		return ErrKeyContainsSlash
	}

	for _, rune := range key {
		// No White Space
		if unicode.IsSpace(rune) {
			return ErrKeyContainsWhiteSpace
		}

		// No Control Characters
		if unicode.IsControl(rune) {
			return ErrKeyContainsControlChar
		}
	}

	// return an empty error on success
	return nil
}
