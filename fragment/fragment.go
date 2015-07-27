/*
Fragment manages the fragment cache used to upload files to the server.
The fragment cache lets files be uploaded in pieces, and then be copied
to tape as a single unit. Files are intended to be uploaded as consecutive
pieces, of arbitrary size. If a fragment upload does not complete or has
and error, that fragment is deleted, and the upload can try again.
*/
package fragment

import (
	"sync"

	"github.com/ndlib/bendo/store"
)

type Store struct {
	s     store.Store
	m     sync.RWMutex // protects files
	files map[string]*File
}

// we store two kinds of information in the store: file metadata and
// file fragments. They are distinguished by the prefix of their keys:
// metadata keys start with "md" and file fragments start with "f".

const (
	fileKeyPrefix     = "md"
	fragmentKeyPrefix = "f"
)

// we keep the metadata info in the store to allow resumption after server restarts.
// but the source of record will be the in memory version of the data.

type File struct {
	ID   string
	Size int64
}

// Open the given store.Store and return a fragment store wrapping it. It will
// load its metadata from the store before returning, possibly with an error.
func New(s store.Store) (*Store, error) {
	news := &Store{
		s:     s,
		files: make(map[string]*File),
	}
	err := news.load()
	return news, err
}

// initialize our in memory data from the store.
func (s *Store) load() error {
	s.m.Lock()
	defer s.m.Unlock()
	metadata, err := s.s.ListPrefix(fileKeyPrefix)
	if err != nil {
		return err
	}
	// we could deconstruct the ID from the key, but
	// it is easier to just unmarshal it from the json
	for _, key := range metadata {
		r, _, err := s.s.Open(key)
		if err != nil {
			return err
		}

		r.Close()
		s.files[f.ID] = f
	}

}

// creates a new file, and returns a pointer to the file object.
// the new file will have a unique name to be used for further interactions.
// the file is not persisted until its first fragment has been written.
func (fs *Store) New() *File { return nil }

// Lookup a file
func (fs *Store) Lookup() *File { return nil }

// Delete a file
func (fs *Store) Delete() {}

func (f *File) Append()   {}
func (f *File) Open()     {}
func (f *File) Info()     {}
func (f *File) Rollback() {}

func load(r io.ReaderAt) (*File, error) {

}

func (f *File) save() error {
	err := f.store.Delete(frag.ID)
	if err != nil {
		return err
	}
	w, err := f.store.Create(frag.ID)
	if err != nil {
		return err
	}
	defer w.Close()
	enc := json.NewEncoder(w)
	err = enc.Encode(f)
	return err
}

// defer opening the given key until Read is called.
// close the stream when EOF is reached
