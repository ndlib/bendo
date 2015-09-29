// Package bagit implements the enough of the BagIt specification to read and
// save the BagIt files used by Bendo. It is tailored to use the Store interface
// instead of directly using the file system. It creates zip files which do
// not use compression. It also only supports MD5 and SHA256 checksums for
// the manifest file.
//
// Specific items not implemented are fetch files and holely bags. It doesn't
// preserve the order of the tags in the bag-info.txt file. It also doesn't
// preserve multiple occurrences of tags in the bag-info.txt file.
//
// This package allows for reading a bag, verifying a bag, and creating new
// bags. It does not provide any services for updating a bag.
// Checksums are generated for each file when a bag is created.
// After that, checksums are only calculated when a bag is (explicitly) verified.
// In particular, checksums are not calculated when reading content from a bag.
//
// The interface is designed to mirror the archive/zip interface as much as
// possible.
//
// The BagIt spec can be found at https://tools.ietf.org/html/draft-kunze-bagit-11.
package bagit

// Bag represents a single BagIt file.
type Bag struct {
	// the bag's name, which is the directory this bag unserializes into.
	// includes the trailing slash, e.g. "ex-bag/"
	dirname string

	// for each file in this bag, the checksums we expect for it.
	// payload files begin with "data/". Tag and control files don't.
	manifest map[string]*Checksum

	// list of tags to be saved in the bag-info.txt file. The key is the
	// tag name, and the value is the content to save for that tag.
	// content strings are not wrapped at column 75 in this implementation.
	tags map[string]string
}

// Checksum contains all the checksums we know about for a given file.
// Some entries may be empty. At least one entry should be present.
type Checksum struct {
	MD5    []byte
	SHA1   []byte
	SHA256 []byte
	SHA512 []byte
}

const (
	// Version is the version of the BagIt specification this package implements.
	Version = "0.97"
)

// New creates a new BagIt structure. Is this needed at all?
func New() Bag {
	return Bag{
		manifest: make(map[string]*Checksum),
		tags:     make(map[string]string),
	}
}
