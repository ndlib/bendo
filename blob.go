package bendo

import (
	"date"
)

// blobid identifies a single blob within an item
type blobid int

// xbid is an extended blob id which uniquely identifies a blob
// in the entire system
type xbid struct {
	item string
	blob blobid
}

// vbid is a versioned blob id, and is an alternate way of identifying
// a blob. It is preferred to use xbid's where ever possible.
type vbid struct {
	item    string
	version versionid
	slot    string
}

// versionid identifies a version of an item
type versionid int

type blob struct {
	ID             blobid
	Created        date.DateTime
	Creator        string
	Deleted        date.DateTime
	Deleter        string
	Size           int
	MD5            []byte
	SHA256         []byte
	ChecksumDate   date.DateTime
	ChecksumStatus bool
	Cached         bool
	Parent         string
}

type version struct {
	ID    versionid
	Slots map[string]blobid
}

type item struct {
	ID       string
	blobs    []blob
	versions []version
}
