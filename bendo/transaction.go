package bendo

import (
	"io"
	"sort"
	"time"
)

type dTx struct {
	dty     *Directory
	item    *Item // we hold the lock on item.
	blobs   []blobData
	bnext   BlobID
	del     []BlobID
	version Version
}

type blobData struct {
	b *Blob
	r io.Reader
}

type byID []*Blob

func (p byID) Len() int           { return len(p) }
func (p byID) Less(i, j int) bool { return p[i].ID < p[j].ID }
func (p byID) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

type bySize []blobData

func (p bySize) Len() int           { return len(p) }
func (p bySize) Less(i, j int) bool { return p[i].b.Size < p[j].b.Size }
func (p bySize) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

func (dty *Directory) Update(id string) Transaction {
	tx := &dTx{dty: dty,
		version: Version{
			// version ids are 1 based
			ID:    1,
			Slots: make(map[string]BlobID),
		},
	}
	item := dty.cache.Lookup(id)
	if item == nil {
		// this is a new item
		item = &Item{ID: id}
	}
	tx.item = item
	vlen := len(tx.item.versions)
	if vlen > 0 {
		lst := tx.item.versions[vlen-1]
		tx.version.ID = lst.ID + 1
		for k, v := range lst.Slots {
			tx.version.Slots[k] = v
		}
	}
	return tx
}

func (tx *dTx) Cancel() {
}

// This blobID is provisional, provided the transaction completes successfully
// r must be open until after Commit() or Cancel() are called.
//
// not thread safe
func (tx *dTx) AddBlob(r io.Reader, size int64, md5, sha256 []byte) BlobID {
	// blob ids are 1 based
	if tx.bnext == 0 {
		tx.bnext = 1
		blen := len(tx.item.blobs)
		if blen > 0 {
			tx.bnext = tx.item.blobs[blen-1].ID + 1
		}
	}
	blob := &Blob{
		ID:     tx.bnext,
		Size:   size,
		MD5:    md5,
		SHA256: sha256,
	}
	tx.bnext++
	tx.blobs = append(tx.blobs, blobData{b: blob, r: r})
	return blob.ID
}

func (tx *dTx) SetNote(s string)    { tx.version.Note = s }
func (tx *dTx) SetCreator(s string) { tx.version.Creator = s }

func (tx *dTx) SetSlot(s string, id BlobID) {
	if id == 0 {
		delete(tx.version.Slots, s)
	} else {
		tx.version.Slots[s] = id
	}
}

// Remove the given blob.
//
// If the blob was added in this transaction, then the blob is removed, and will not be
// saved to tape.
// There can be holes in blob ids if, say, two blobs are added and then
// the first one is deleted while the transaction is still open. We cannot renumber the
// second blob, so there will be a gap in the ids where the first blob was.
func (tx *dTx) DeleteBlob(b BlobID) {
	// is this blob in the new blob list? if so, remove it from the list
	for j, bx := range tx.blobs {
		if bx.b.ID == b {
			tx.blobs = append(tx.blobs[:j], tx.blobs[j+1:]...)
			return
		}
	}
	tx.del = append(tx.del, b)
}

func (tx *dTx) Commit() error {
	// TODO(dbrower): add error handling
	if tx.version.Creator == "" {
		panic("commit() called with empty Creator field")
	}

	// Update item metadata
	tx.version.SaveDate = time.Now()
	tx.item.versions = append(tx.item.versions, &tx.version)

	// now save everything
	b := tx.dty.newBundler(tx.item, tx.item.maxBundle+1)

	// First handle deletions
	if err := tx.doDeletes(b); err != nil {
		b.Close()
		return err
	}

	// Save new blobs and new metadata
	// Try to save the larger blobs first.
	sort.Stable(sort.Reverse(bySize(tx.blobs))) // sort larger blobs first

	for _, bd := range tx.blobs {
		bd.b.SaveDate = time.Now()
		bd.b.Creator = tx.version.Creator
		tx.item.blobs = append(tx.item.blobs, bd.b)
		sort.Stable(byID(tx.item.blobs))
		// maybe updating maxBundle should be moved to bundler.end()?
		tx.item.maxBundle = b.n - 1
		err := b.writeBlob(bd.b, bd.r)
		if err != nil {
			b.Close()
			return err
		}
	}
	err := b.Close()
	if err != nil {
		return err
	}

	//tx.dty.Set(tx.item.ID, tx.item)
	//tx.dty.maxBundle <- scan{id: tx.item.ID, max: tx.item.maxBundle}

	// now delete bundles which contain purged items

	return nil
}

func (tx *dTx) doDeletes(b *bundleWriter) error {
	// gather up which bundles need to be rewritten
	// and update blob metadata
	var bundles = make(map[int][]BlobID)
	for _, id := range tx.del {
		blob := tx.item.blobByID(id)
		if blob != nil {
			bundles[blob.Bundle] = append(bundles[blob.Bundle], id)

			blob.DeleteDate = time.Now()
			blob.Deleter = tx.version.Creator
			blob.DeleteNote = tx.version.Note
			blob.Bundle = 0
			blob.Size = 0
		}
	}

	for k, v := range bundles {
		err := b.copyBundleExcept(k, v)
		if err != nil {
			return err
		}
	}
	return nil
}
