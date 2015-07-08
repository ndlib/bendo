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
type Blob struct {
	m       sync.RWMutex
	ID      BlobID
	Created time.Time
	Creator string
	Parent  string // the parent item's id
	Size    int    // logical size of associated content (i.e. before compression), 0 if deleted

	// valid if not deleted
	MD5            []byte    // unused if Size == 0
	SHA256         []byte    // unused if Size == 0
	ChecksumDate   time.Time // unused if Size == 0
	ChecksumStatus bool      // true == pass, false == error. Only valid if ChecksumDate > 0
	Cached         bool      // true == in our disk cache
	SourceVersion  VersionID // the version file this blob is in

	// following valid if item is deleted
	Deleted    time.Time // 0 value if not deleted
	Deleter    string    // empty iff not deleted
	DeleteNote string    // optional note for deletion event
}

type Version struct {
	m         sync.RWMutex
	ID        VersionID
	Iteration uint
	SaveDate  time.Time
	CreatedBy string
	Note      string
	Slots     map[string]BlobID
}

type Item struct {
	m        sync.RWMutex
	ID       string
	blobs    []*Blob
	versions []*Version
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

	// given an item's ID, return a new structure containing metadata
	// about that item, including all versions and blob metadata.
	// In particular, the blob data is NOT returned
	Item(id string) (*Item, error)

	// Given an extended blob id, return a stream giving the blob's contents.
	// While the version v is not necessary, it allows the blob content to
	// be read using a single tape access.
	BlobContent(id string, v VersionID, b BlobID) (io.ReadCloser, error)
}

type BlobData struct {
	id BlobID
	r  io.Reader
}

// BundleStore is the read and write interface to the tape store.
type BundleStore interface {
	BundleReadStore
	SaveItem(*Item, VersionID, []BlobData) error
	// DeleteBlobs(*Item, VersionID, []BlobID) error
}

type ReadAtCloser interface {
	io.ReaderAt
	io.Closer
}

type BS2 interface {
	List() <-chan string
	ListPrefix(prefix string) ([]string, error)
	Open(key string, id string) (ReadAtCloser, int64, error)
	Create(key string, id string) (io.WriteCloser, error)
	Delete(key string) error
}
