package bendo

import (
	"io"
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
	del   []BlobID
	vers  []Version
}

type blobData struct {
	b BlobID
	r io.Reader
}

func (rmp *romp) NewTransaction(id string) Transaction {
	tx := &rompTx{rmp: rmp}
	item, ok := rmp.items[id]
	if !ok {
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

func (tx *rompTx) AddBlob(b *Blob, r io.Reader) BlobID {
	// blob ids are 1 based
	n := BlobID(len(tx.item.blobs) + len(tx.blobs) + 1)
	b.ID = n
	tx.blobs = append(tx.blobs, blobData{b: n, r: r})
	return n
}

func (tx *rompTx) AddVersion(v *Version) VersionID {
	n := VersionID(len(tx.item.versions) + len(tx.vers))
	v.ID = n
	tx.vers = append(tx.vers, *v)
	return n
}

func (tx *rompTx) DeleteBlob(b BlobID) {
	tx.del = append(tx.del, b)
}

func (tx *rompTx) Commit() error {
	// remove any new blobs that were deleted
	cutoff := len(tx.item.blobs)
	for i, did := range tx.del {
		if int(did) <= cutoff {
			continue
		}
		for j, b := range tx.blobs {
			if b.b == did {
				tx.blobs[j].b = 0
				break
			}
		}
		tx.del[i] = 0
	}

	// Update item metadata

	// First handle deletions

	// now save new blobs and new metadata

	return nil
}

func (tx *rompTx) doDeletes() {
	// gather up which bundles need to be rewritten
	var rewrites = make(map[int][]BlobID)
	for _, bid := range tx.del {
		b := &Blob{} // tx.item.Blob(bid)
		if b == nil {
			continue
		}
		lst := rewrites[b.Bundle]
		rewrites[b.Bundle] = append(lst, bid)
	}
	// now rewrite each bundle

}

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
