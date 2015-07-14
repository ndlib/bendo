package bendo

import (
	"io"
	"sort"
)

type dTx struct {
	dty   *Directory
	isnew bool
	item  *Item // we hold the lock on item.
	blobs []blobData
	next  BlobID
	del   []BlobID
	vers  []Version
}

type blobData struct {
	b *Blob
	r io.Reader
}

type bySize []blobData

func (p bySize) Len() int           { return len(p) }
func (p bySize) Less(i, j int) bool { return p[i].b.Size < p[j].b.Size }
func (p bySize) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

func (dty *Directory) Update(id string) Transaction {
	tx := &dTx{dty: dty}
	item := dty.cache.Lookup(id)
	if item == nil {
		// this is a new item
		item = &Item{ID: id}
		tx.isnew = true
	}
	tx.item = item
	item.m.Lock()
	return tx
}

func (tx *dTx) Cancel() {
	tx.item.m.Unlock()
}

// This blobID is provisional, provided the transaction completes successfully
// r must be open until after Commit() or Cancel() are called.
//
// not thread safe
func (tx *dTx) AddBlob(b *Blob, r io.Reader) BlobID {
	// blob ids are 1 based
	if tx.next == 0 {
		blen := len(tx.item.blobs)
		if blen == 0 {
			tx.next = 1
		} else {
			tx.next = tx.item.blobs[blen-1].ID + 1
		}
	}
	b.ID = tx.next
	tx.next++
	tx.blobs = append(tx.blobs, blobData{b: b, r: r})
	return b.ID
}

func (tx *dTx) AddVersion(v *Version) VersionID {
	n := VersionID(len(tx.item.versions) + len(tx.vers))
	v.ID = n
	tx.vers = append(tx.vers, *v)
	return n
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

	// Update item metadata

	//
	b := tx.dty.newBundler(tx.item, tx.item.maxBundle+1)

	// First handle deletions
	tx.doDeletes(b)

	// Now save new blobs and new metadata
	// Try to save the larger blobs first.
	sort.Stable(sort.Reverse(bySize(tx.blobs))) // sort larger blobs first

	for _, bd := range tx.blobs {
		tx.item.blobs = append(tx.item.blobs, bd.b)
		// TODO(dbrower): sort blobs list by blob id
		// maybe updating maxBundle should be moved to bundler.end()?
		tx.item.maxBundle = b.n - 1
		b.writeBlob(bd.b, bd.r)
	}
	b.Close()
	return nil
}

func (tx *dTx) doDeletes(b *bundleWriter) {
	// gather up which bundles need to be rewritten
	var bundles []int
	for _, id := range tx.del {
		b := tx.item.blobByID(id)
		if b != nil {
			bundles = append(bundles, b.Bundle)
		}
	}
	sort.Sort(sort.IntSlice(bundles))

	// now rewrite each bundle
}
