package bendo

import (
	"io"
	"sync"
	"time"
)

// BlobID identifies a single blob within an item
type BlobID int

// XBid uniquely identifies a blob in the entire system
type XBid struct {
	ID   string
	Blob BlobID
}

func (x XBid) CacheKey() string {
	return x.ID
}

// VBid is a versioned blob id, and is an alternate way of identifying
// a blob. Though, prefer to use XBid's when possible.
type VBid struct {
	Item    string
	Version VersionID
	Slot    string
}

// VersionID identifies a version of an item
type VersionID int

// Blob records metadata for each blob. We keep one blob structure in memory
// for each blob on tape and share it. Use the mutex if making updates.
// The blob structures are backed by a database layer
//
// This mixes together 3 levels of information:
//   1. that saved on tape
//   2. operational info saved to our caching database
//   3. in memory data used by the program
type Blob struct {
	m       sync.RWMutex
	ID      BlobID
	Created time.Time
	Creator string
	Size    int64 // logical size of associated content (i.e. before compression), 0 if deleted

	// following valid if blob is NOT deleted
	Bundle         int       // which bundle file this blob is stored in
	MD5            []byte    // unused if Size == 0
	SHA256         []byte    // unused if Size == 0
	ChecksumDate   time.Time // unused if Size == 0
	ChecksumStatus bool      // true == pass, false == error. Only valid if ChecksumDate > 0

	// following valid if blob is deleted
	Deleted    time.Time // zero iff not deleted
	Deleter    string    // empty iff not deleted
	DeleteNote string    // optional note for deletion event
}

type Version struct {
	m         sync.RWMutex
	ID        VersionID
	SaveDate  time.Time
	CreatedBy string
	Note      string
	Slots     map[string]BlobID
}

type Item struct {
	m         sync.RWMutex
	ID        string
	maxBundle int        // largest bundle id used by this item
	blobs     []*Blob    // list of blobs, sorted by id
	versions  []*Version // list of versions, sorted by it
}

// BundleReadStore is the read only part of the underlying tape store.
// It fetches item information and the blob contents. The caching is implemented
// above this, so implementations of this interface should not cache data.
// This interface serialized the Item data, but otherwise does not use it.
// These methods should be thread safe.
type BundleReadStore interface {
	// ItemList starts a goroutine to scan the items on tape
	// and returns its results in the channel. the channel is closed
	// when the scanning is finished.
	ItemList() <-chan string

	// Given an item's ID, return a new structure containing metadata
	// about that item, including all versions and blob metadata.
	// In particular, the blob data is NOT returned
	Item(id string) (*Item, error)

	// Return a stream containing the blob's contents.
	BlobContent(id string, b BlobID) (io.ReadCloser, error)
}

type BlobData struct {
	id BlobID
	r  io.Reader
}

// BundleStore is the high level read and write interface
type BundleStore interface {
	BundleReadStore

	// Start an update transaction on an item. There can be at most one
	// update transaction at a time per item.
	NewTransaction(id string) Transaction
}

// type Transaction is not tread safe
type Transaction interface {
	// r needs to be open until the end of the transaction.
	// No deduplication checks are performed on the blob.
	// Will modify the ID in the record to be the new value
	AddBlob(b *Blob, r io.Reader) BlobID

	// Add a new version to the item
	AddVersion(v *Version) VersionID

	// Purge the given blob from the underlying storage.
	// Use this with caution.
	DeleteBlob(b BlobID)

	// Commits this given transaction to tape and releases the underlying
	// transaction lock on the item.
	Commit() error

	// Cancels this transaction and releases all the locks
	Cancel()
}

type ReadAtCloser interface {
	io.ReaderAt
	io.Closer
}

type BS2 interface {
	List() <-chan string
	ListPrefix(prefix string) ([]string, error)
	Open(key string, id string) (ReadAtCloser, int64, error)
	Create(key, id string) (io.WriteCloser, error)
	Delete(key, id string) error
}
