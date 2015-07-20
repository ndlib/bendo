package bendo

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
	maxBundle int        // largest bundle id used by this item
	blobs     []*Blob    // list of blobs, sorted by id
	versions  []*Version // list of versions, sorted by it
}

// The lower-level interface for working with Items.
// It wraps a BundleStore and provides code to serialize and deserialize them
// from bundles.
//
// The default ItemStore does not implement caching, and the caller needs to
// ensure only one goroutine is accessing an item at a time.
type ItemStore struct {
	Store BundleStore
}

/*
	// ItemList returns a list of item IDs. The channel is closed
	// when the scanning is finished.
	List() <-chan string

	// Return a new item structure for the given item. This structure
	// contains all the metadata about that item, including a version list
	// and a list of blob metadata.
	// In particular, the blob data is NOT returned
	//
	// This will block while the metadata is loading from the BundleStore.
	Item(id string) (*Item, error)

	// Return a stream containing the blob's contents.
	Blob(id string, b BlobID) (io.ReadCloser, error)

	// Start an update transaction on an item. It is an error to have two
	// parallel transactions on the same item.
	// If the item doesn't exist, it is created
	Open(id string) Writer

	// Validate all the bundles associated with the given item.
	// Returns the total number of bytes and a list of errors found.
	Validate(id string) (int64, []string, error)

	// Returns the underlying BundleStore
	BundleStore() BundleStore
*/

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
