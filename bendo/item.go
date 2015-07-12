package bendo

import (
	"io"
	"sort"
)

//// handle interface to loading and saving items

// We have two levels of item knowledge:
//   1. We know an item exists and the extent of its bundle numbers
//   2. We have read a bundle and know the full item metadata
// We can get (1) by just stating files. So that is what we use to begin with.
// Knowledge of type (2) requires reading from tape. So we will fill it in
// on demand and selectively in the background.

type rompTx struct {
	rmp   *romp
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

func (rmp *romp) NewTransaction(id string) Transaction {
	tx := &rompTx{rmp: rmp}
	item, ok := rmp.items[id]
	if !ok {
		// this is a new item
		item = &Item{ID: id}
		tx.isnew = true
	}
	tx.item = item
	item.m.Lock()
	return tx
}

func (tx *rompTx) Cancel() {
	tx.item.m.Unlock()
}

// This blobID is provisional, provided the transaction completes successfully
// r must be open until after Commit() or Cancel() are called.
//
// not thread safe
func (tx *rompTx) AddBlob(b *Blob, r io.Reader) BlobID {
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

func (tx *rompTx) AddVersion(v *Version) VersionID {
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
func (tx *rompTx) DeleteBlob(b BlobID) {
	// is this blob in the new blob list? if so, remove it from the list
	for j, bx := range tx.blobs {
		if bx.b.ID == b {
			tx.blobs = append(tx.blobs[:j], tx.blobs[j+1:]...)
			return
		}
	}
	tx.del = append(tx.del, b)
}

const (
	MB              = 1000000
	IdealBundleSize = 500 * MB
)

func (tx *rompTx) Commit() error {
	// Update item metadata

	// First handle deletions
	//
	// TODO(dbrower): this could be more efficient...for example if we are
	// deleting blobs from more than one bundle, or if we are deleting a blob
	// and saving new ones, we could repack all the blobs into a single new
	// bundle. Instead now, each bundle having a blob deleted from it is copied
	// into its own new bundle, and then all the new blobs are saved into
	// another new bundle.
	tx.doDeletes()

	// Now save new blobs and new metadata
	// We try to make bundles about the same size, but only if size metadata
	// has been supplied already. If not, then there is not much we can do
	// about it now. Perhaps we should put this logic into the function actually
	// saving the blobs, since then we will always have correct size information.
	sort.Stable(sort.Reverse(bySize(tx.blobs))) // sort larger blobs first

	// TODO(dbrower): actually implement this algorithm.
	// For now jam everything into a single bundle
	var currentBundle = tx.item.maxBundle + 1
	var data []BlobData
	for _, b := range tx.blobs {
		tx.item.blobs = append(tx.item.blobs, b.b)
		data = append(data, BlobData{id: b.b.ID, r: b.r})
	}
	tx.item.maxBundle = currentBundle
	return tx.rmp.SaveItem(tx.item, currentBundle, data)
}

func (tx *rompTx) doDeletes() {
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

type byID []*Blob

func (p byID) Len() int           { return len(p) }
func (p byID) Less(i, j int) bool { return p[i].ID < p[j].ID }
func (p byID) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

func (item *Item) blobByID(id BlobID) *Blob {
	item.m.RLock()
	for _, b := range item.blobs {
		if b.ID == id {
			item.m.Unlock()
			return b
		}
	}
	item.m.Unlock()
	return nil
}
