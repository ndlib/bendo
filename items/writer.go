package items

import (
	"bytes"
	"io"
	"sort"
	"time"
)

// A Writer implements an io.Writer with extra methods to save a new
// version of an Item.
type Writer struct {
	store   *Store        // need for cache.Set() and Deletes
	item    *Item         // item we are writing out
	bw      *BundleWriter //
	bnext   BlobID        // the next available blob id
	del     []BlobID      // list of blobs to delete at Close
	version Version       // version info for this write
	bdel    []int         // bundle files to delete. generated from del
}

// Open opens the item id for writing. This will add a single new version to the
// item. New blobs can be written. Blobs can also be deleted (but that is not a
// quick operation).
//
// The creator is the name of the agent performing these updates.
//
// It is an error for more than one goroutine to open the same item at a time.
// This does not perform any locking itself.
func (s *Store) Open(id string, creator string) (*Writer, error) {
	wr := &Writer{
		store: s,
		version: Version{
			// version ids are 1 based
			ID:       1,
			Slots:    make(map[string]BlobID),
			SaveDate: time.Now(),
			Creator:  creator,
		},
	}
	item, err := s.Item(id)
	if err == ErrNoItem {
		// this is a new item
		item = &Item{ID: id}
	} else if err != nil {
		return nil, err
	}
	wr.item = item
	// figure out the next version number
	vlen := len(item.Versions)
	if vlen > 0 {
		// there is a previous version, so copy its slot assignments
		prev := item.Versions[vlen-1]
		wr.version.ID = prev.ID + 1
		for k, v := range prev.Slots {
			wr.version.Slots[k] = v
		}
	}
	wr.bw = NewBundler(s.S, item)
	return wr, nil
}

// Close closes the given Writer. The final metadata is written out, and any
// blobs marked for deletion are extracted and removed.
func (wr *Writer) Close() error {
	// Update item metadata
	wr.version.SaveDate = time.Now()
	wr.item.Versions = append(wr.item.Versions, &wr.version)

	// handle any deletions
	err := wr.doDeletes()

	wr.item.MaxBundle = wr.bw.CurrentBundle() // XXX: this should be handled by the BundleWriter
	err2 := wr.bw.Close()
	wr.store.cache.Set(wr.item.ID, wr.item)
	if err != nil {
		return err
	}
	if err2 != nil {
		return err2
	}

	// delete bundles which contain purged items
	// TODO(dbrower): figure out a policy on whether to do this deletion
	for _, bundleid := range wr.bdel {
		err = wr.store.S.Delete(sugar(wr.item.ID, bundleid))
		if err != nil {
			return err
		}
	}

	return nil
}

func (wr *Writer) doDeletes() error {
	// gather up which bundles need to be rewritten
	// and update blob metadata
	var bundles = make(map[int][]BlobID)
	for _, id := range wr.del {
		blob := wr.item.blobByID(id)
		if blob != nil && blob.Bundle != 0 {
			bundles[blob.Bundle] = append(bundles[blob.Bundle], id)

			blob.DeleteDate = time.Now()
			blob.Deleter = wr.version.Creator
			blob.DeleteNote = wr.version.Note
			blob.Bundle = 0
			blob.Size = 0
			blob.MimeType = ""
		}
	}

	for bundleid, blobids := range bundles {
		err := wr.bw.CopyBundleExcept(bundleid, blobids)
		if err != nil {
			return err
		}
		wr.bdel = append(wr.bdel, bundleid)
	}
	return nil
}

// WriteBlob signifies the intent to copy the given io.Reader into this item.
// If size and the hashes are provided, the item is checked to see if there is
// already a blob with them in this item. If there is, that blob id is returned
// and r is not read at all.
//
// If such a blob is not already in the item, WriteBlob will copy the io.Reader
// into the item as a new blob. The hashes and size are compared with the data
// read from r and an error is triggered if there is a difference.
//
// The hashes and size may be nil and 0 if unknown, in which case they will be
// calculated and stored as needed, and no mismatch error will be triggered.
//
// If there is an error writing the blob, the blob is not added to the item's
// blob list, and the id of 0 is returned. There may be a remnant "blob/{id}"
// entry in the zip file, so it is best to close this Writer and reopen before
// retrying writing the blob.
func (wr *Writer) WriteBlob(r io.Reader, size int64, md5, sha256 []byte) (BlobID, error) {
	// see if this blob is already in the item
	bid := wr.findBlobByHash(size, md5, sha256)
	if bid != 0 {
		return bid, nil
	}

	// lazily set up the blob counter
	if wr.bnext == 0 {
		wr.bnext = 1 // blob ids are 1 based
		blen := len(wr.item.Blobs)
		if blen > 0 {
			wr.bnext = wr.item.Blobs[blen-1].ID + 1
		}
	}
	// write the blob before appending the blob info to our blob list.
	// If there are any errors, we don't add the blob information.
	// We won't reuse the blob number this time, but it may get reused
	// in a subsequent call to Open().
	blob := &Blob{
		ID:       wr.bnext,
		SaveDate: time.Now(),
		Creator:  wr.version.Creator,
		// The following are updated by WriteBlob
		// Store what we expect so WriteBlob can compare
		Size:   size,
		MD5:    md5,
		SHA256: sha256,
	}
	// This is delicate because of error recovery.
	// If the blob file was created in the bundle then we will reserve this
	// blobID and add it to the blob list. Otherwise, we will reuse the blob
	// id for the next one.
	result, err := wr.bw.WriteBlob(blob, r)
	if err != nil && len(result.WrittenMD5) == 0 {
		// blob was never opened in the target bundle, so return an error
		// and don't increase the blob ID
		return 0, err
	}
	// whether or not there was an error, update the blob info to be saved
	wr.bnext++ // prevent duplicate blob names inside this bundle
	blob.Bundle = result.Bundle
	wr.item.Blobs = append(wr.item.Blobs, blob)
	// ensure blobs are always sorted by increasing ID
	sort.Stable(byID(wr.item.Blobs))
	wr.item.MaxBundle = wr.bw.CurrentBundle()
	// overwrite blank values in the blob...
	if blob.Size == 0 {
		blob.Size = result.BytesWritten
	}
	if len(blob.MD5) == 0 {
		blob.MD5 = result.WrittenMD5[:]
	}
	if len(blob.SHA256) == 0 {
		blob.SHA256 = result.WrittenSHA256[:]
	}
	// return error from WriteBlob(), if one
	if err != nil {
		return 0, err
	}
	// now check for validation errors
	err = ValidateWriteBlob(wr.bw.item.ID, blob, result)
	if err != nil {
		// since this blob was never sucessfully written, update the blob
		// structure to match what was actually written, and then signal an
		// error.
		blob.Size = result.BytesWritten
		blob.MD5 = result.WrittenMD5[:]
		blob.SHA256 = result.WrittenSHA256[:]
		return 0, err
	}
	return blob.ID, nil
}

// If a blob exists in the associated item having the same size and
// hash values, return the blob's id. Otherwise return zero.
// It is okay if size is 0 or if one or both hashes are empty.
// This function is conservative, and it is possible it may return 0 even
// though there is a matching blob.
func (wr *Writer) findBlobByHash(size int64, md5, sha256 []byte) BlobID {
	if size == 0 || (len(md5) == 0 && len(sha256) == 0) {
		return 0
	}
	for _, blob := range wr.item.Blobs {
		if blob.Size == size &&
			(len(md5) == 0 || bytes.Equal(md5, blob.MD5)) &&
			(len(sha256) == 0 || bytes.Equal(sha256, blob.SHA256)) {
			return blob.ID
		}
	}
	return 0
}

type byID []*Blob

func (p byID) Len() int           { return len(p) }
func (p byID) Less(i, j int) bool { return p[i].ID < p[j].ID }
func (p byID) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

// SetNote sets the note metadata field for this version.
func (wr *Writer) SetNote(s string) { wr.version.Note = s }

// SetCreator sets the creator metadata field. (Remove?)
func (wr *Writer) SetCreator(s string) { wr.version.Creator = s }

// SetSlot adds a slot mapping for this version. To explicitly remove a slot,
// set it  to 0. The slot mapping is initialized to that of the previous version.
func (wr *Writer) SetSlot(s string, id BlobID) {
	if id == 0 {
		delete(wr.version.Slots, s)
	} else {
		wr.version.Slots[s] = id
	}
}

// ClearSlots will remove all the slot information for the current version.
// Any slot entries made before calling this will be lost (but the blobs will
// still be around!).
func (wr *Writer) ClearSlots() {
	wr.version.Slots = make(map[string]BlobID)
}

// SetMimeType sets the mime type for the given blob. Nothing is changed if no
// blob has the given id or if the blob has been deleted.
func (wr *Writer) SetMimeType(id BlobID, mimetype string) {
	blob := wr.item.blobByID(id)
	if blob == nil || blob.Bundle == 0 {
		return
	}
	blob.MimeType = mimetype
}

// DeleteBlob marks the given blob for removal from the underlying storage.
// Blobs will be removed when Close() is called. Removal may take a while since
// every other blob in the bundle the blob is stored in will be copied into a
// new bundle.
//
// This function should be used infrequently. What is probably desired is to make
// a new version with the given slot removed by calling SetSlot with a 0 as a
// blob id.
func (wr *Writer) DeleteBlob(bid BlobID) {
	// deal with deduplication of blob ids in Close()
	wr.del = append(wr.del, bid)
}
