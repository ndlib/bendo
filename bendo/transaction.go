package bendo

import (
	"io"
	"sort"
	"time"
)

type Writer struct {
	item    *Item
	bw      *BundleWriter
	bnext   BlobID   // the next available blob id
	del     []BlobID // list of blobs to delete at Close
	version Version  // version info for this write
}

type byID []*Blob

func (p byID) Len() int           { return len(p) }
func (p byID) Less(i, j int) bool { return p[i].ID < p[j].ID }
func (p byID) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

func (s *Store) Open(id string) *Writer {
	wr := &Writer{
		version: Version{
			// version ids are 1 based
			ID:       1,
			Slots:    make(map[string]BlobID),
			SaveDate: time.Now(),
		},
	}
	item, err := s.Item(id)
	if item == nil || err != nil {
		// this is a new item
		item = &Item{ID: id}
	}
	wr.item = item
	// figure out the next version number
	vlen := len(item.versions)
	if vlen > 0 {
		// there is a previous version, so copy its slot assignments
		prev := item.versions[vlen-1]
		wr.version.ID = prev.ID + 1
		for k, v := range prev.Slots {
			wr.version.Slots[k] = v
		}
	}
	wr.bw = NewBundler(s.S, item, item.maxBundle+1)
	return wr
}

// Close the given Writer. The final metadata is written out, and any
// blobs marked for deletion are extracted and removed.
// SetCreator() must have been called first.
func (wr *Writer) Close() error {
	// TODO(dbrower): add error handling
	if wr.version.Creator == "" {
		panic("commit() called with empty Creator field")
	}

	// Update item metadata
	wr.version.SaveDate = time.Now()
	wr.item.versions = append(wr.item.versions, &wr.version)

	// handle any deletions
	err := wr.doDeletes()
	err2 := wr.bw.Close()
	if err != nil {
		return err
	}
	if err2 != nil {
		return err2
	}

	// TODO(dbrower): delete bundles which contain purged items

	return nil
}

func (wr *Writer) doDeletes() error {
	// gather up which bundles need to be rewritten
	// and update blob metadata
	var bundles = make(map[int][]BlobID)
	for _, id := range wr.del {
		blob := wr.item.blobByID(id)
		if blob != nil {
			bundles[blob.Bundle] = append(bundles[blob.Bundle], id)

			blob.DeleteDate = time.Now()
			blob.Deleter = wr.version.Creator
			blob.DeleteNote = wr.version.Note
			blob.Bundle = 0
			blob.Size = 0
		}
	}

	for bundleid, blobids := range bundles {
		err := wr.bw.CopyBundleExcept(bundleid, blobids)
		if err != nil {
			return err
		}
	}
	return nil
}

// Write the given io.Reader as a new blob. The hashes and size are compared with
// the data in r and an error is triggered if there is a difference.
// The hashes and size may be nil and 0 if unknown, in which case they are calculated
// and stored as needed, and no error is triggered.
// The creator needs to have been set previously with a call to SetCreator.
// A panic will happen if there is no creator.
//
// The data in r is written immeadately to the bundle file. The id of the new
// blob is returned.
func (wr *Writer) WriteBlob(r io.Reader, size int64, md5, sha256 []byte) (BlobID, error) {
	if wr.version.Creator == "" {
		panic("WriteBlob() called before call to SetCreator()")
	}

	// lazily set up the blob counter
	if wr.bnext == 0 {
		wr.bnext = 1 // blob ids are 1 based
		blen := len(wr.item.blobs)
		if blen > 0 {
			wr.bnext = wr.item.blobs[blen-1].ID + 1
		}
	}
	blob := &Blob{
		ID:       wr.bnext,
		SaveDate: time.Now(),
		Creator:  wr.version.Creator,
		// the following are updated by WriteBlob
		Size:   size,
		MD5:    md5,
		SHA256: sha256,
	}
	wr.bnext++
	wr.item.blobs = append(wr.item.blobs, blob)
	// ensure blobs are always sorted by increasing ID
	sort.Stable(byID(wr.item.blobs))
	// maybe updating maxBundle should be moved to bundler.end()?

	wr.item.maxBundle = wr.bw.CurrentBundle()
	err := wr.bw.WriteBlob(blob, r)
	return blob.ID, err
}

func (wr *Writer) SetNote(s string)    { wr.version.Note = s }
func (wr *Writer) SetCreator(s string) { wr.version.Creator = s }

// Sets a slot mapping for this version.
// To explicitly remove a slot, set it to 0.
// The slot mapping is initialized to that of the previous version.
func (wr *Writer) SetSlot(s string, id BlobID) {
	if id == 0 {
		delete(wr.version.Slots, s)
	} else {
		wr.version.Slots[s] = id
	}
}

// Mark the given blob for removal from the underlying storage. Blobs will be removed when Close() is called.
// Removal may take a while since every other blob in the bundle the blob is stored
// in will be copied into a new bundle.
//
// This is intended to be used seldomly. What is probably desired is to make a new
// version with the given slot removed by calling SetSlot with a 0 as a blob id.
func (wr *Writer) DeleteBlob(bid BlobID) {
	// deal with deduplication of blob ids in Close()
	wr.del = append(wr.del, bid)
}
