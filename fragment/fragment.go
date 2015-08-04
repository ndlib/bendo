/*
Fragment manages the fragment cache used to upload files to the server.
The fragment cache lets files be uploaded in pieces, and then be copied
to tape as a single unit. Files are intended to be uploaded as consecutive
pieces, of arbitrary size. If a fragment upload does not complete or has
and error, that fragment is deleted, and the upload can try again.
*/
package fragment

import (
	"fmt"
	"io"
	"sync"

	"github.com/ndlib/bendo/store"
)

// Store wraps a store.Store and provides a fragment cache. This allows files
// to be uploaded in pieces, "fragments", and then read back as a single
// unit.
type Store struct {
	meta   JSONStore    // for the metadata
	fstore store.Store  // for the file fragments
	m      sync.RWMutex // protects files
	files  map[string]*File
}

const (
	// There are two kinds of information in the store: file metadata and
	// file fragments. They are distinguished by the prefix of their keys:
	// metadata keys start with "md" and file fragments start with "f".
	//
	// The metadata info is in the store to allow reloading after
	// server restarts.
	fileKeyPrefix     = "md"
	fragmentKeyPrefix = "f"
)

type File struct {
	// what kind of locking is needed on a File?
	meta     JSONStore    // for metadata
	fstore   store.Store  // for fragments
	m        sync.RWMutex // protects everything below
	ID       string       // name in the fstore
	Size     int64        // sum of all the children sizes
	N        int          // the id number to use for the next fragment
	Children []*Fragment  // Children ids, in the order to read them.
}

type Fragment struct {
	ID   string
	Size int64
}

// Create a new fragment store wrapping a store.Store. The metadata will be
// loaded from the store before returning.
func New(s store.Store) *Store {
	return &Store{
		meta:   NewJSON(store.NewWithPrefix(s, fileKeyPrefix)),
		fstore: store.NewWithPrefix(s, fragmentKeyPrefix),
		files:  make(map[string]*File),
	}
}

// Initialize our in-memory data from the store. This must be called before
// using the structure.
func (s *Store) Load() error {
	metadata, err := s.meta.ListPrefix("")
	if err != nil {
		return err
	}
	s.m.Lock()
	defer s.m.Unlock()
	for _, key := range metadata {
		f := new(File)
		err := s.meta.Open(key, &f)
		if err != nil {
			// TODO(dbrower): this is probably too strict. We should
			// probably just skip this file
			return err
		}
		f.meta = s.meta
		f.fstore = s.fstore
		s.files[f.ID] = f
	}
	return nil
}

// Return a list of all the stored file names.
func (s *Store) List() []string {
	s.m.RLock()
	defer s.m.RUnlock()
	result := make([]string, 0, len(s.files))
	for k := range s.files {
		result = append(result, k)
	}
	return result
}

// Create a new file with the given name, and return a pointer to it.
// the file is not persisted until its first fragment has been written.
// If the file already exists, nil is returned.
func (s *Store) New(id string) *File {
	s.m.Lock()
	defer s.m.Unlock()
	if _, ok := s.files[id]; ok {
		return nil
	}
	newfile := &File{ID: id, meta: s.meta, fstore: s.fstore}
	s.files[id] = newfile
	return newfile
}

// Lookup a file. Returns nil if none exists with that id.
// Returned pointers are not safe to be accessed by more than one goroutine.
func (s *Store) Lookup(id string) *File {
	s.m.RLock()
	defer s.m.RUnlock()
	return s.files[id]
}

// Delete a file
func (s *Store) Delete(id string) error {
	s.m.Lock()
	f := s.files[id]
	delete(s.files, id)
	s.m.Unlock()

	if f == nil {
		return nil
	}

	// don't need the lock for the following
	err := s.meta.Delete(f.ID)
	for _, child := range f.Children {
		er := s.fstore.Delete(child.ID)
		if err == nil {
			err = er
		}
	}
	return err
}

// Open a file for writing. The writes are appended to the end.
func (f *File) Append() (io.WriteCloser, error) {
	f.m.Lock()
	defer f.m.Unlock()
	fragkey := fmt.Sprintf("%s+%04d", f.ID, f.N)
	f.N++
	w, err := f.fstore.Create(fragkey)
	if err != nil {
		return nil, err
	}
	frag := &Fragment{ID: fragkey}
	f.Children = append(f.Children, frag)
	err = f.save()
	return &fragwriter{Fragment: frag, parent: f, w: w}, err
}

type fragwriter struct {
	*Fragment
	parent *File
	w      io.WriteCloser
}

func (fw *fragwriter) Write(p []byte) (int, error) {
	n, err := fw.w.Write(p)
	fw.Size += int64(n)
	return n, err
}

func (fw *fragwriter) Close() error {
	err := fw.w.Close()
	if err == nil {
		fw.parent.m.Lock()
		fw.parent.Size += fw.Size
		err = fw.parent.save()
		fw.parent.m.Unlock()
	}
	return err
}

// Open a file for reading from the beginning.
func (f *File) Open() io.ReadCloser {
	f.m.RLock()
	defer f.m.RUnlock()
	var list = make([]string, len(f.Children))
	for i := range f.Children {
		list[i] = f.Children[i].ID
	}
	return &fragreader{
		s:    f.fstore,
		keys: list,
	}
}

// fragreader provides an io.Reader which will span a list of keys.
// Each fragment is opened and closed in turn, so there is at most one
// file descriptor open at any time.
type fragreader struct {
	s    store.Store        // the store containing the keys
	keys []string           // next one to open is at index 0
	r    store.ReadAtCloser // nil if no reader is open
	off  int64              // offset into r to read from next
}

func (fr *fragreader) Read(p []byte) (int, error) {
	for len(fr.keys) > 0 || fr.r != nil {
		var err error
		if fr.r == nil {
			// open a new reader
			fr.r, _, err = fr.s.Open(fr.keys[0])
			if err != nil {
				return 0, err
			}
			fr.off = 0
			fr.keys = fr.keys[1:]
		}
		n, err := fr.r.ReadAt(p, fr.off)
		fr.off += int64(n)
		if err == io.EOF {
			// need to check rest of list before sending EOF
			err = fr.r.Close()
			fr.r = nil
		}
		if n > 0 || err != nil {
			return n, err
		}
	}
	return 0, io.EOF
}

func (fr *fragreader) Close() error {
	if fr.r != nil {
		return fr.r.Close()
	}
	return nil
}

// Remove the last fragment from this file.
// Use RemoveFragment to remove a specific fragment.
func (f *File) Rollback() error {
	f.m.Lock()
	defer f.m.Unlock()
	return f.removefragment(len(f.Children) - 1)
}

// Remove the given fragment from a file. The first fragment is
// 0, the next is 1, etc. Use Rollback() to remove the last fragment.
func (f *File) RemoveFragment(n int) error {
	if n < 0 || n >= len(f.Children) {
		return nil
	}
	f.m.Lock()
	defer f.m.Unlock()
	return f.removefragment(n)
}

// must hold m.Lock() when calling
func (f *File) removefragment(n int) error {
	frag := f.Children[n]
	err := f.fstore.Delete(frag.ID)
	if err != nil {
		return err
	}
	f.Children = append(f.Children[:n], f.Children[n+1:]...)
	f.Size -= frag.Size
	return f.save()
}

// must hold at least a read lock to call this
func (f *File) save() error {
	return f.meta.Save(f.ID, f)
}
