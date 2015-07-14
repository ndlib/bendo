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
bundleWriter helps with saving blobs into bundles, and with repackaging blobs
when doing deletions.
It keeps a reference to its source item, and will use that to save the item-info.json
file when needed.

It is not goroutine safe. Make sure to call Close when finished.
*/
type bundleWriter struct {
	dty  *Directory
	item *Item
	zf   io.WriteCloser // the underlying bundle file, nil if no file is currently open
	zw   *zip.Writer    // the zip interface over the bundle file
	size int64          // amount written to current bundle
	n    int            // 1 + current bundle id
}

// make new bundle writer for item. n is the new bundle number to start with.
// more than one bundle may be written.
func (dty *Directory) newBundler(item *Item, n int) *bundleWriter {
	return &bundleWriter{
		dty:  dty,
		item: item,
		n:    n,
	}
}

func (bw *bundleWriter) openNext() error {
	var err error
	bw.zf, err = bw.dty.s.Create(sugar(bw.item.ID, bw.n), bw.item.ID)
	if err != nil {
		return err
	}
	bw.zw = zip.NewWriter(bw.zf)
	bw.n++
	bw.size = 0
	return nil
}

func (bw *bundleWriter) Close() error {
	if bw.zf == nil {
		return nil
	}
	// write out the item data
	w, err := makeStream(bw.zw, "item-info.json")
	if err == nil {
		err = writeItemInfo(w, bw.item)
	}
	if err == nil {
		err = bw.zw.Close()
	}
	if err == nil {
		err = bw.zf.Close()
		bw.zf = nil
	}
	return err
}

const (
	MB              = 1000000
	IdealBundleSize = 500 * MB
)

// assumes blob points to a blob already in the blob list for the bundle this
// item is for
func (bw *bundleWriter) writeBlob(blob *Blob, r io.Reader) error {
	if bw.zf == nil {
		if err := bw.openNext(); err != nil {
			return err
		}
	}
	if bw.item.blobByID(blob.ID) == nil {
		panic("Save blob with id not in blob list")
	}
	w, err := makeStream(bw.zw, fmt.Sprintf("blob/%d", blob.ID))
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
	blob.Bundle = bw.n - 1
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
	bw.size += sz.Size()
	if bw.size >= IdealBundleSize {
		bw.Close()
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
func (bw *bundleWriter) copyBundleExcept(src int, except []BlobID) error {
	// open the source bundle
	// NOTE: a lot of this is identical to the code which opens blobs for
	// reading. Can this be refactored to use the same code base. The difference
	// here is that we are scanning EVERYTHING in the bundle rather than
	// looking for a specific file.
	rac, size, err := bw.dty.s.Open(sugar(bw.item.ID, src), bw.item.ID)
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
		blob := bw.item.blobByID(extractBlobId(f.Name))
		err = bw.writeBlob(blob, rc)
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
