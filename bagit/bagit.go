// Package bagit implements the enough of the BagIt specification to read and
// save the bagit files used by Bendo. It is tailored to use the Store interface
// instead of directly using the file system. It creates zip files which do
// not use compression. It also only supports MD5 and SHA256 checksums for
// the manifest file.
package bagit

import (
	"github.com/ndlib/bendo/store"
)

type T struct {
	manifest map[string]checksum
	tags     map[string]string
}

type checksum struct {
	MD5    []byte
	SHA256 []byte
}
