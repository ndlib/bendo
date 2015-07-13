package bendo

import (
	"archive/zip"
	"bytes"
	"crypto/md5"
	"crypto/sha256"
	"fmt"
	"io"
	"strconv"
	"strings"
)

/*
bundler type helps with saving blobs into bundles, and with repackaging blobs
when doing deletions.
*/

type bundler struct {
	dty  *Directory
	item *Item
	zf   io.WriteCloser // the underlying bundle file
	zw   *zip.Writer    // the zip interface over the bundle file
	size int64          // amount written to current bundle
	n    int            // 1 + current bundle id
}

// make new bundle writer for item. n is the new bundle number to start with.
// more than one bundle may be written.
func (dty *Directory) newBundler(item *Item, n int) *bundler {
	return &bundler{
		dty:  dty,
		item: item,
		n:    n,
	}
}

func (b *bundler) openNext() error {
	var err error
	b.zf, err = b.dty.s.Create(sugar(b.item.ID, b.n), b.item.ID)
	if err != nil {
		return err
	}
	b.zw = zip.NewWriter(b.zf)
	b.n++
	b.size = 0
	return nil
}

func (b *bundler) Close() error {
	if b.zf == nil {
		return nil
	}
	// write out the item data
	w, err := makeStream(b.zw, "item-info.json")
	if err == nil {
		err = writeItemInfo(w, b.item)
	}
	if err == nil {
		err = b.zw.Close()
	}
	if err == nil {
		err = b.zf.Close()
		b.zf = nil
	}
	return err
}

const (
	MB              = 1000000
	IdealBundleSize = 500 * MB
)

// assumes blob points to a blob already in the blob list for the bundle this
// item is for
func (b *bundler) writeBlob(blob *Blob, r io.Reader) error {
	if b.zf == nil {
		if err := b.openNext(); err != nil {
			return err
		}
	}
	if b.item.blobByID(blob.ID) == nil {
		panic("Save blob with id not in blob list")
	}
	w, err := makeStream(b.zw, fmt.Sprintf("blob/%d", blob.ID))
	if err != nil {
		return err
	}
	md5 := md5.New()
	sha256 := sha256.New()
	sz := &writeSizer{}
	w = io.MultiWriter(w, md5, sha256, sz)
	_, err = io.Copy(w, r)
	if err != nil {
		return err
	}
	// Don't update DateSaved timestamp, since the blob may be a copy
	// because of a purge
	blob.Bundle = b.n
	if blob.Size != 0 && blob.Size != sz.Size() {
		// the size counts don't match
	} else {
		blob.Size = sz.Size()
	}
	h := md5.Sum(nil)
	if blob.MD5 != nil && bytes.Compare(blob.MD5, h) != 0 {
	} else {
		blob.MD5 = h
	}
	h = sha256.Sum(nil)
	if blob.SHA256 != nil && bytes.Compare(blob.SHA256, h) != 0 {
	} else {
		blob.SHA256 = h
	}
	// do this last, after we have updated the metadata for this blob
	b.size += sz.Size()
	if b.size >= IdealBundleSize {
		b.Close()
	}
	return nil
}

func makeStream(z *zip.Writer, name string) (io.Writer, error) {
	header := zip.FileHeader{
		Name:   name,
		Method: zip.Store,
	}
	return z.CreateHeader(&header)
}

type writeSizer struct {
	size int64
}

func (ws *writeSizer) Write(p []byte) (int, error) {
	ws.size += int64(len(p))
	return len(p), nil
}

func (ws *writeSizer) Size() int64 {
	return ws.size
}

// copies all the blobs in the bundle src, except for blobs with an id in except.
func (b *bundler) copyBundleExcept(src int, except []BlobID) error {
	// open the source bundle
	// NOTE: a lot of this is identical to the code which opens blobs for
	// reading. Can this be refactored to use the same code base. The difference
	// here is that we are scanning EVERYTHING in the bundle rather than
	// looking for a specific file.
	rac, size, err := b.dty.s.Open(sugar(b.item.ID, src), b.item.ID)
	if err != nil {
		return err
	}
	defer rac.Close()
	z, err := zip.NewReader(rac, size)
	if err != nil {
		return err
	}
	var badnames = make([]string, 1+len(except))
	badnames[0] = "item-info.json"
	for i, id := range except {
		badnames[i+1] = fmt.Sprintf("blob/%d", id)
	}
	for _, f := range z.File {
		if contains(badnames, f.Name) {
			continue
		}
		var rc io.ReadCloser
		rc, err = f.Open()
		if err != nil {
			return err
		}
		// TODO(dbrower): check for errors
		blob := b.item.blobByID(extractBlobId(f.Name))
		b.writeBlob(blob, rc)
		rc.Close()
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
