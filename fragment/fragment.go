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

// we keep the metadata info in the store to allow resumption after server restarts.
// but the source of record will be the in memory version of the data.

type File struct {
	ID   string
	Size int64
}

// Open the given store.Store and return a fragment store wrapping it
func Open(s store.Store) *Store {
	return nil
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
