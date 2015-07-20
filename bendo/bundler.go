package bendo

import (
	"bytes"
	"crypto/md5"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"hash"
	"io"
	"strconv"
	"strings"
)

/*
BundleWriter helps with saving blobs into bundles, and with repackaging blobs
when doing deletions.
It keeps a reference to its source item, and will use that to save the item-info.json
file when needed.

It is not goroutine safe. Make sure to call Close when finished.
*/
type BundleWriter struct {
	store BundleStore
	item  *Item
	zw    *zipwriter // target bundle file. nil if nothing is open.
	size  int64      // amount written to current bundle
	n     int        // 1 + current bundle id
}

// make new bundle writer for item. n is the new bundle number to start with.
// more than one bundle may be written.
func NewBundler(s BundleStore, item *Item, n int) *BundleWriter {
	return &BundleWriter{
		store: s,
		item:  item,
		n:     n,
	}
}

func (bw *BundleWriter) CurrentBundle() int {
	if bw.zw == nil {
		return bw.n
	}
	return bw.n - 1
}

// sets our zw to write to the next bundle file. Assumes no other process is
// writting bundle files for this item.
func (bw *BundleWriter) openNext() error {
	var err error
	bw.zw, err = openZipWriter(bw.store, bw.item.ID, bw.n)
	if err != nil {
		return err
	}
	bw.n++
	bw.size = 0
	return nil
}

func (bw *BundleWriter) Close() error {
	if bw.zw == nil {
		return nil
	}
	// write out the item data
	w, err := bw.zw.makeStream("item-info.json")
	if err == nil {
		err = writeItemInfo(w, bw.item)
	}
	bw.zw.Close()
	bw.zw = nil
	return err
}

const (
	MB              = 1000000
	IdealBundleSize = 500 * MB
)

// assumes blob points to a blob already in the blob list for the bundle this
// item is for
func (bw *BundleWriter) WriteBlob(blob *Blob, r io.Reader) error {
	if bw.size >= IdealBundleSize {
		if err := bw.Close(); err != nil {
			return err
		}
	}
	if bw.zw == nil {
		if err := bw.openNext(); err != nil {
			return err
		}
	}
	if bw.item.blobByID(blob.ID) == nil {
		panic("Save blob with id not in blob list")
	}
	w, err := bw.zw.makeStream(fmt.Sprintf("blob/%d", blob.ID))
	if err != nil {
		return err
	}
	md5 := md5.New()
	sha256 := sha256.New()
	w = io.MultiWriter(w, md5, sha256)
	size, err := io.Copy(w, r)
	bw.size += size
	if err != nil {
		return err
	}
	// Don't update DateSaved timestamp, since the blob may be a copy
	// because of a purge
	blob.Bundle = bw.n - 1
	if blob.Size == 0 {
		blob.Size = size
	} else if blob.Size != size {
		return fmt.Errorf("commit (%s blob %d), copied %d bytes, expected %d",
			bw.item.ID,
			blob.ID,
			size,
			blob.Size)
	}
	err = testhash(md5, &blob.MD5, bw.item.ID)
	if err == nil {
		err = testhash(sha256, &blob.SHA256, bw.item.ID)
	}
	return err
}

func testhash(h hash.Hash, target *[]byte, name string) error {
	computed := h.Sum(nil)
	if *target == nil {
		*target = computed
	} else if bytes.Compare(*target, computed) != 0 {
		return fmt.Errorf("commit (%s), got %s, expected %s",
			name,
			hex.EncodeToString(computed),
			hex.EncodeToString(*target))
	}
	return nil
}

// copies all the blobs in the bundle src, except for blobs with an id in except.
func (bw *BundleWriter) CopyBundleExcept(src int, except []BlobID) error {
	// open the source bundle
	// NOTE: a lot of this is identical to the code which opens blobs for
	// reading. Can this be refactored to use the same code base. The difference
	// here is that we are scanning EVERYTHING in the bundle rather than
	// looking for a specific file.
	r, err := OpenBundle(bw.store, sugar(bw.item.ID, src))
	if err != nil {
		return err
	}
	defer r.Close()
	var badnames = make([]string, 1+len(except))
	badnames[0] = "item-info.json"
	for i, id := range except {
		badnames[i+1] = fmt.Sprintf("blob/%d", id)
	}
	for _, f := range r.File {
		if contains(badnames, f.Name) {
			continue
		}
		var rc io.ReadCloser
		rc, err = f.Open()
		if err != nil {
			return err
		}
		// TODO(dbrower): check for errors
		blob := bw.item.blobByID(extractBlobId(f.Name))
		err = bw.WriteBlob(blob, rc)
		rc.Close()
		if err != nil {
			return err
		}
	}
	return nil
}

func contains(lst []string, s string) bool {
	for i := range lst {
		if lst[i] == s {
			return true
		}
	}
	return false
}

// from "blob/xxx" return xxx as a BlobID
func extractBlobId(s string) BlobID {
	sa := strings.SplitN(s, "/", 2)
	if len(sa) != 2 || sa[0] != "blob" {
		return BlobID(0)
	}
	id, err := strconv.ParseInt(sa[1], 10, 0)
	if err != nil {
		id = 0
	}
	return BlobID(id)
}
