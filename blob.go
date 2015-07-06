package bendo

import (
	"date"
	"sync"
)

// BlobID identifies a single blob within an item
type BlobID int

// XBid is an extended blob id which uniquely identifies a blob
// in the entire system
type XBid struct {
	Item string
	Blob BlobID
}

// VBid is a versioned blob id, and is an alternate way of identifying
// a blob. It is preferred to use XBid's where ever possible.
type VBid struct {
	Item    string
	Version VersionID
	Slot    string
}

// versionid identifies a version of an item
type VersionID int

// Blob records metadata for each blob. We keep one blob structure in memory
// for each blob on tape and share it. Use the mutex if making updates.
// The blob structures are backed by a database layer
type Blob struct {
	m              sync.RWMutex
	ID             BlobID
	Created        date.DateTime
	Creator        string
	Deleted        date.DateTime // 0 value if not deleted
	Deleter        string        // empty iff not deleted
	DeleteNote     string        // optional note for deletion event
	Size           int           // logical size of associated content (i.e. before compression), 0 if deleted
	MD5            []byte        // unused if Size == 0
	SHA256         []byte        // unused if Size == 0
	ChecksumDate   date.DateTime // unused if Size == 0
	ChecksumStatus bool          // true == pass, false == error
	Cached         bool          // true == in our disk cache
	Parent         string        // the parent item's id
	SourceVersion  int           // the version file this blob is in
}

type Version struct {
	ID        VersionID
	Iteration int
	SaveDate  date.DateTime
	CreatedBy string
	Note      string
	Slots     map[string]BlobID
}

type Item struct {
	ID       string
	blobs    []Blob
	versions []Version
}
