package items

import (
	"bytes"
	"fmt"

	"github.com/ndlib/bendo/bagit"
	"github.com/ndlib/bendo/store"
)

// Validate the given item. Returns the total amount checksummed (in bytes),
// a list of issues which will be empty if everything is fine, and an error
// if an error happened during the validation. In particular, err does not
// show validation errors, only if a system error happened while validating.
//
// Things checked (not all are implemented yet):
// * Each blob has the correct checksum
// * Each blob appears in exactly one bundle
// * Every blob is assigned to at least one slot in at least one version
// * Each slot points to an existing (possibly deleted) blob
// * Each bundle is readable and in the correct format
// * There are no extra files in a bundle
// * All required metadata fields are present for each blob
// * All required metadata fields are present for each version
//
// This is a method on the Store instead of an Item since it needs access
// to the underlying bundle files.
func (s *Store) Validate(id string) (nb int64, problems []string, err error) {
	// First verify each bundle file
	var bundleNames []string
	bundleNames, err = s.S.ListPrefix(id)
	if err != nil {
		return
	}

	// can we prefetch all the bundle files?
	if x, ok := s.S.(store.Stager); ok {
		x.Stage(bundleNames)
	}

	for _, name := range bundleNames {
		// open the bundle directly since we need access to the size
		var stream store.ReadAtCloser
		var size int64
		stream, size, err = s.S.Open(name)
		if err != nil {
			return
		}
		var bag *bagit.Reader
		bag, err = bagit.NewReader(stream, size)
		if err != nil {
			stream.Close()
			return
		}
		err = bag.Verify()
		_ = stream.Close()
		nb += size
		if err != nil {
			if _, ok := err.(bagit.BagError); ok {
				// there was a failed verification
				problems = append(problems, err.Error())
				err = nil
			} else {
				// there was an actual error in doing the verification
				return
			}
		}
	}

	// then validate the metadata
	var item *Item
	item, err = s.Item(id)
	if err != nil {
		return
	}
	// validate blob metadata
	var bundleblobmap = make(map[string][]*Blob)
	for _, blob := range item.Blobs {
		if blob.SaveDate.IsZero() {
			problems = append(problems, fmt.Sprintf("Blob (%s,%d) has a zero save date", id, blob.ID))
		}
		if blob.DeleteDate.IsZero() {
			// this blob is not deleted
			if blob.Size < 0 {
				problems = append(problems, fmt.Sprintf("Blob (%s,%d) has negative size", id, blob.ID))
			}
			if blob.Bundle <= 0 {
				problems = append(problems, fmt.Sprintf("Blob (%s,%d) has non-positive bundle ID", id, blob.ID))
			}
			if len(blob.MD5) != 16 {
				problems = append(problems, fmt.Sprintf("Blob (%s,%d) has malformed MD5 hash", id, blob.ID))
			}
			if len(blob.SHA256) != 32 {
				problems = append(problems, fmt.Sprintf("Blob (%s,%d) has malformed SHA-256 hash", id, blob.ID))
			}
			if blob.Deleter != "" {
				problems = append(problems, fmt.Sprintf("Blob (%s,%d) has a deleter", id, blob.ID))
			}
			if blob.DeleteNote != "" {
				problems = append(problems, fmt.Sprintf("Blob (%s,%d) has a delete note", id, blob.ID))
			}
			// now verify these hashes match what is stored in the manifest
			bundlename := sugar(id, blob.Bundle)
			bundleblobmap[bundlename] = append(bundleblobmap[bundlename], blob)
		} else {
			// blob is deleted
			if blob.Bundle != 0 {
				problems = append(problems, fmt.Sprintf("Blob (%s,%d) is deleted and has non-zero bundle ID", id, blob.ID))
			}
			if blob.Size != 0 {
				problems = append(problems, fmt.Sprintf("Blob (%s,%d) is deleted and has non-zero size", id, blob.ID))
			}
			if blob.Deleter == "" {
				problems = append(problems, fmt.Sprintf("Blob (%s,%d) is deleted and has no deleter", id, blob.ID))
			}
		}
	}

	for bundlename, bloblist := range bundleblobmap {
		var bag *BagreaderCloser
		bag, err = OpenBundle(s.S, bundlename)
		if err != nil {
			return
		}
		for _, blob := range bloblist {
			checksum := bag.Checksum(fmt.Sprintf("blob/%d", blob.ID))
			if !bytes.Equal(blob.MD5, checksum.MD5) {
				problems = append(problems, fmt.Sprintf("Blob (%s,%d) has MD5 mismatch", id, blob.ID))
			}
			if !bytes.Equal(blob.SHA256, checksum.SHA256) {
				problems = append(problems, fmt.Sprintf("Blob (%s,%d) has SHA-256 mismatch", id, blob.ID))
			}
		}
		err = bag.Close()
		if err != nil {
			return
		}
	}

	// TODO(dbrower): validate version metadata
	return
}

// validateItemMetadata checks that the metadata for an item are consistent
// and matches the bag checksums as stored.
func (s *Store) validateItemMetadata() {
}
