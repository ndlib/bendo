package items

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

	"github.com/ndlib/bendo/store"
)

/*
BundleWriter helps with saving blobs into bundles, and with repackaging blobs
when doing deletions. It keeps a reference to its source item, and will use
that to save the item-info.json file when needed.

It is not goroutine safe. Make sure to call Close when finished.
*/
type BundleWriter struct {
	store store.Store
	item  *Item
	zw    *Zipwriter // target bundle file. nil if nothing is open.
	size  int64      // amount written to current bundle
	n     int        // 1 + current bundle id
}

// NewBundler starts a new bundle writer for the given item. More than one bundle
// file may be written. The advancement to a new bundle file happens either when
// the current one grows larger than IdealBundleSize, or when Next() is called.
func NewBundler(s store.Store, item *Item) *BundleWriter {
	return &BundleWriter{
		store: s,
		item:  item,
		n:     item.MaxBundle + 1,
	}
}

// CurrentBundle returns the id of the bundle being written to.
func (bw *BundleWriter) CurrentBundle() int {
	if bw.zw == nil {
		return bw.n
	}
	return bw.n - 1
}

// Next() closes the current bundle, if any, and starts a new bundle file.
func (bw *BundleWriter) Next() error {
	var err error
	err = bw.Close()
	if err != nil {
		return err
	}
	bw.zw, err = OpenZipWriter(bw.store, bw.item.ID, bw.n)
	if err != nil {
		return err
	}
	bw.n++
	bw.size = 0
	return nil
}

// Close() writes out any final metadata and closes the current bundle.
// Since the bundle file is opened in the first call to WriteBlob(), opening a
// BundleWriter and then closing it will not write anything to disk.
func (bw *BundleWriter) Close() error {
	if bw.zw == nil {
		return nil
	}
	// write out the item data
	w, err := bw.zw.MakeStream("item-info.json")
	if err == nil {
		err = writeItemInfo(w, bw.item)
	}
	bw.zw.Close()
	bw.zw = nil
	return err
}

const (
	MB = 1000000

	// Start a new bundle once the current one becomes larger than this.
	IdealBundleSize = 500 * MB
)

// Write the given blob into the bundle. The blob must also be in the
// underlying item's blob list.
func (bw *BundleWriter) WriteBlob(blob *Blob, r io.Reader) error {
	// we lazily open bw
	if bw.size >= IdealBundleSize || bw.zw == nil {
		if err := bw.Next(); err != nil {
			return err
		}
	}
	if bw.item.blobByID(blob.ID) == nil {
		panic("Save blob with id not in blob list")
	}
	w, err := bw.zw.MakeStream(fmt.Sprintf("blob/%d", blob.ID))
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

// Copies all the blobs in the bundle src, except for those in the list, into
// the current place in the bundle writer.
func (bw *BundleWriter) CopyBundleExcept(src int, except []BlobID) error {
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
