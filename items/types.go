package items

import (
	"io"
	"time"
)

// BlobID identifies a single blob within an item
type BlobID int

// VersionID identifies a version of an item
type VersionID int

// Blob records metadata for each blob.
type Blob struct {
	ID       BlobID
	SaveDate time.Time
	Creator  string
	Size     int64 // logical size of associated content (i.e. before compression), 0 if deleted

	// following valid if blob is NOT deleted
	Bundle         int       // which bundle file this blob is stored in
	MD5            []byte    // unused if Size == 0
	SHA256         []byte    // unused if Size == 0
	ChecksumDate   time.Time // unused if Size == 0
	ChecksumStatus bool      // true == pass, false == error. Only valid if ChecksumDate > 0

	// following valid if blob is deleted
	DeleteDate time.Time // zero iff not deleted
	Deleter    string    // empty iff not deleted
	DeleteNote string    // optional note for deletion event
}

type Version struct {
	ID       VersionID
	SaveDate time.Time
	Creator  string
	Note     string
	Slots    map[string]BlobID
}

type Item struct {
	ID        string
	MaxBundle int        // largest bundle id used by this item
	Blobs     []*Blob    // list of blobs, sorted by id
	Versions  []*Version // list of versions, sorted by it
}

type ItemCache interface {
	// try to return an item record with the given id.
	// return nil if there is nothing matching in the cache.
	Lookup(id string) *Item

	Set(id string, item *Item)
}

type ReadAtCloser interface {
	io.ReaderAt
	io.Closer
}

type BundleStore interface {
	List() <-chan string
	ListPrefix(prefix string) ([]string, error)
	Open(key string, id string) (ReadAtCloser, int64, error)
	Create(key, id string) (io.WriteCloser, error)
	Delete(key, id string) error
}
