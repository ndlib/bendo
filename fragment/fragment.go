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
	"sort"
	"sync"
	"time"

	"github.com/ndlib/bendo/store"
)

// Store wraps a store.Store and provides a fragment cache. This allows files
// to be uploaded in pieces, "fragments", and then read back as a single
// unit.
type Store struct {
	meta   JSONStore    // for the metadata
	fstore store.Store  // for the file fragments
	m      sync.RWMutex // protects everything below
	files  map[string]*file
	labels map[string][]string
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

type FileEntry interface {
	Append() (io.WriteCloser, error)
	Open() io.ReadCloser
	Stat() Stat
	Rollback() error
	SetLabels(labels []string)
	SetCreator(name string)
}

// The metadata kept on each file entry
type Stat struct {
	ID         string
	Size       int64
	NFragments int
	Created    time.Time
	Modified   time.Time
	Creator    string
	Labels     []string
}

// The internal struct which tracks a file's metadata
type file struct {
	parent   *Store
	m        sync.RWMutex // protects everything below
	ID       string       // name in the parent.fstore
	Size     int64        // sum of all the children sizes
	N        int          // the id number to use for the next fragment
	Children []*fragment  // Children ids, in the order to read them.
	Created  time.Time    // time this record was created
	Modified time.Time    // last time this record was modified
	Labels   []string     // list of labels for this file
	Creator  string       // the "user" (aka API key) who created this file
}

// An individual fragment of a file
type fragment struct {
	ID   string // the id of this fragment in the fstore
	Size int64  // the size of this fragment in bytes
}

// Create a new fragment store wrapping a store.Store. Call Load() before
// using the store.
func New(s store.Store) *Store {
	return &Store{
		meta:   NewJSON(store.NewWithPrefix(s, fileKeyPrefix)),
		fstore: store.NewWithPrefix(s, fragmentKeyPrefix),
		files:  make(map[string]*file),
		labels: make(map[string][]string),
	}
}

// Initialize an in memory data from the file entries stored inside.
// Must be called before using this store.
func (s *Store) Load() error {
	metadata, err := s.meta.ListPrefix("")
	if err != nil {
		return err
	}
	s.m.Lock()
	defer s.m.Unlock()
	for _, key := range metadata {
		f := new(file)
		err := s.meta.Open(key, &f)
		if err != nil {
			// TODO(dbrower): this is probably too strict. We should
			// probably just skip this file
			return err
		}
		f.parent = s
		s.files[f.ID] = f
		s.indexRecord(f)
	}
	return nil
}

// index the labels for f
// locks must be held on both s AND f to call this.
func (s *Store) indexRecord(f *file) {
	for _, label := range f.Labels {
		s.labels[label] = append(s.labels[label], f.ID)
		sort.Strings(s.labels[label])
	}
}

// remove a record from our label indices
// locks must be held on both s and f to call this
func (s *Store) unindexRecord(f *file) {
	for _, label := range f.Labels {
		list := s.labels[label]
		i := sort.SearchStrings(list, f.ID)
		if list[i] == f.ID {
			s.labels[label] = append(list[:i], list[i+1:]...)
		}
	}
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

// Return a list of the file ids matching a given set of labels.
// If the list of labels provided is empty, a list of every item is returned.
// The items in the returned list are in alphabetical order.
func (s *Store) ListFiltered(labels []string) []string {
	if len(labels) == 0 {
		result := s.List()
		sort.Sort(sort.StringSlice(result))
		return result
	}
	s.m.RLock()
	defer s.m.RUnlock()
	var lists [][]string
	for _, label := range labels {
		lists = append(lists, s.labels[label])
	}
	return combineCommon(lists)
}

// return the common elements from a sequence of lists. Each list must be
// in sorted order already.
func combineCommon(lists [][]string) []string {
	if len(lists) == 0 {
		return nil
	}
	var idxs = make([]int, len(lists))
	var result []string
	// this is similar to an n-way merge sort
	// first set bar to the maximum of what we need to scan for
	var bar string
	var current_list int
	var equal_count int
	for {
		// advance current_list until it is >= bar
		list := lists[current_list]
		for {
			// exit if we come to the end of this list
			if idxs[current_list] >= len(list) {
				return result
			}
			// if current list is == bar, see if other lists agree
			if list[idxs[current_list]] == bar {
				equal_count++
				if equal_count < len(lists) {
					break
				}
				// a match accross every list!
				result = append(result, bar)
				equal_count = 0
			} else if list[idxs[current_list]] > bar {
				bar = list[idxs[current_list]]
				equal_count = 0
				break
			}
			idxs[current_list]++
		}
		current_list++
		if current_list >= len(lists) {
			current_list = 0
		}
	}
}

// Create a new file with the given name, and return a pointer to it.
// the file is not persisted until its first fragment has been written.
// If the file already exists, nil is returned.
func (s *Store) New(id string) FileEntry {
	s.m.Lock()
	defer s.m.Unlock()
	if _, ok := s.files[id]; ok {
		return nil
	}
	newfile := &file{
		ID:       id,
		parent:   s,
		Created:  time.Now(),
		Modified: time.Now(),
	}
	s.files[id] = newfile
	return newfile
}

// Lookup a file. Returns nil if none exists with that id.
// Returned pointers are not safe to be accessed by more than one goroutine.
func (s *Store) Lookup(id string) FileEntry {
	s.m.RLock()
	defer s.m.RUnlock()
	result, ok := s.files[id]
	if !ok {
		// explicitly return nil otherwise we get a nil wrapped as
		// a valid interface...see https://golang.org/doc/faq#nil_error
		return nil
	}
	return result
}

// Delete a file. It is not an error to delete a file that does not exist.
func (s *Store) Delete(id string) error {
	s.m.Lock()
	f := s.files[id]
	delete(s.files, id)
	if f != nil {
		s.unindexRecord(f)
	}
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

func (f *file) Stat() Stat {
	f.m.RLock()
	defer f.m.RUnlock()
	return Stat{
		ID:         f.ID,
		Size:       f.Size,
		NFragments: len(f.Children),
		Created:    f.Created,
		Modified:   f.Modified,
		Creator:    f.Creator,
		Labels:     f.Labels[:],
	}
}

// Open a file for writing. The writes are appended to the end.
func (f *file) Append() (io.WriteCloser, error) {
	f.m.Lock()
	defer f.m.Unlock()
	fragkey := fmt.Sprintf("%s+%04d", f.ID, f.N)
	f.N++
	w, err := f.parent.fstore.Create(fragkey)
	if err != nil {
		return nil, err
	}
	frag := &fragment{ID: fragkey}
	f.Children = append(f.Children, frag)
	err = f.save()
	return &fragwriter{frag: frag, parent: f, w: w}, err
}

type fragwriter struct {
	w    io.WriteCloser
	size int64
	// must hold lock in parent to access these
	parent *file
	frag   *fragment // make it easy to update when we are closed
}

func (fw *fragwriter) Write(p []byte) (int, error) {
	n, err := fw.w.Write(p)
	fw.size += int64(n)
	return n, err
}

func (fw *fragwriter) Close() error {
	err := fw.w.Close()
	if err == nil {
		fw.parent.m.Lock()
		fw.parent.Size += fw.size
		fw.frag.Size = fw.size
		err = fw.parent.save()
		fw.parent.m.Unlock()
	}
	return err
}

// Open a file for reading from the beginning.
func (f *file) Open() io.ReadCloser {
	f.m.RLock()
	defer f.m.RUnlock()
	var list = make([]string, len(f.Children))
	for i := range f.Children {
		list[i] = f.Children[i].ID
	}
	return &fragreader{
		s:    f.parent.fstore,
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

// Remove the last fragment from this file. This is everything written during
// using the Writer gotten from the most recent call to Append.
// If called more than once, it will keep removing previous Append'ed blocks,
// until the file is empty.
// Returns an error if there was a problem deleting the most recent fragment.
func (f *file) Rollback() error {
	f.m.Lock()
	defer f.m.Unlock()
	n := len(f.Children) - 1
	frag := f.Children[n]
	err := f.parent.fstore.Delete(frag.ID)
	if err != nil {
		return err
	}
	f.Children = f.Children[:n]
	f.Size -= frag.Size
	return f.save()
}

// Save the metadata for this file object.
// must hold a write lock on f to call this
func (f *file) save() error {
	f.Modified = time.Now()
	return f.parent.meta.Save(f.ID, f)
}

// set the labels on the given file to those passed in.
// We overwrite the list of labels currently applied to the file.
func (f *file) SetLabels(labels []string) {
	dedup := make([]string, len(labels))
	for i := range labels {
		dedup[i] = labels[i]
	}
	sort.Sort(sort.StringSlice(dedup))
	// only run loop until the next-to-last element in the array
	for i := 0; i < len(dedup)-1; {
		if dedup[i] == dedup[i+1] {
			dedup = append(dedup[:i], dedup[i+1:]...)
		} else {
			i++
		}
	}
	// we do this locking dance to maintain the lock order of locking the
	// store before the file
	f.parent.m.Lock()
	defer f.parent.m.Unlock()
	f.m.Lock()
	defer f.m.Unlock()
	f.parent.unindexRecord(f)
	f.Labels = dedup
	f.parent.indexRecord(f)
	f.save()
}

func (f *file) SetCreator(name string) {
	f.m.Lock()
	defer f.m.Unlock()
	f.Creator = name
	f.save()
}
