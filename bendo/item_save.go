package bendo

import (
	"archive/zip"
	"bytes"
	"crypto/md5"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"
)

/*
Low level routines to serialize and deserialize items from the storage
interface, which is abstracted by a BundleStore.

An item's metadata and blobs are grouped into "bundles", which are zip files.
Each bundle contains the complete up-to-date metadata information on an item,
as well as zero or more blobs. Bundles are numbered, but they should not be
assumed to be numbered sequentially since deletions may remove some bundles.

There is no relationship between a bundle number and the versions of an item.
*/

// A romp is our item metadata registry
type romp struct {
	// our metadata cache store...the authoritative source is the bundles
	items map[string]*Item

	// our underlying bundle store
	s BundleStore
}

func NewRomp(s BundleStore) BundleReadStoreX {
	return &romp{items: make(map[string]*Item),
		s: s,
	}
}

// this is not thread safe on rmp
func (rmp *romp) ItemList() <-chan string {
	out := make(chan string)
	go func() {
		// get key list and then update our item list from it
		// make a new item list so we can detect deletions
		var items = make(map[string]*Item)
		c := rmp.s.List()
		for key := range c {
			id, _ := desugar(key)
			if id == "" {
				continue
			}
			item, ok := items[id]
			if !ok {
				out <- id
				item = &Item{ID: id}
				items[id] = item
			}
			// add this version info
		}
		rmp.items = items // not thread safe because of this
	}()
	return out
}

// turn an item id and a bundle number n into a string key
func sugar(id string, n int) string {
	return fmt.Sprintf("%s-%04d", id, n)
}

// extract an item id and a bundle number from a string key
// return an id of "" if the key could not be decoded
func desugar(s string) (id string, n int) {
	z := strings.Split(s, "-")
	if len(z) != 2 {
		return "", 0
	}
	id = z[0]
	n64, err := strconv.ParseInt(z[1], 10, 0)
	if err != nil {
		return "", 0
	}
	n = int(n64)
	return
}

func (rmp *romp) Item(id string) (*Item, error) {
	item, ok := rmp.items[id]
	if ok {
		return item, nil
	}
	// get the highest version number somehow
	var n int
	rc, err := rmp.openZipStream(sugar(id, n), "item-info.json")
	if err != nil {
		return nil, err
	}
	result, err := readItemInfo(rc)
	rc.Close()
	return result, err
}

//func (rmp *romp) BlobContent(id string, n int, b BlobID) (io.ReadCloser, error) {
func (rmp *romp) BlobContent(id string, b BlobID) (io.ReadCloser, error) {
	var n = 1
	// which bundle is this blob in?

	sname := fmt.Sprintf("blob/%d", b)
	return rmp.openZipStream(sugar(id, n), sname)
}

// save all the blobs, and then updates the blob info in Item and writes that out
// the Bundle, MD5, SHA256, and Size fields for the new blobs are verified (and set if they
// were not initialized)
func (rmp *romp) SaveItem(item *Item, n int, bd []BlobData) error {
	zf, err := rmp.s.Create(sugar(item.ID, n), item.ID)
	if err != nil {
		return err
	}
	defer zf.Close()
	z := zip.NewWriter(zf)
	defer z.Close()
	// write out all the blobs
	for _, blob := range bd {
		b := item.blobByID(blob.id)
		if b == nil {
			panic("Save blob with id not in blob list")
		}
		w, err := makeStream(z, fmt.Sprintf("blob/%d", blob.id))
		if err != nil {
			return err
		}
		md5 := md5.New()
		sha256 := sha256.New()
		sz := &writeSizer{}
		w = io.MultiWriter(w, md5, sha256, sz)
		_, err = io.Copy(w, blob.r)
		if err != nil {
			return err
		}
		// Don't update created timestamp, since the blob may be being
		// copied because of a purge
		b.Bundle = n
		if b.Size > 0 && b.Size != sz.Size() {
			// the size counts don't match
		} else {
			b.Size = sz.Size()
		}
		h := md5.Sum(nil)
		if b.MD5 != nil && bytes.Compare(b.MD5, h) != 0 {
		} else {
			b.MD5 = h
		}
		h = sha256.Sum(nil)
		if b.SHA256 != nil && bytes.Compare(b.SHA256, h) != 0 {
		} else {
			b.SHA256 = h
		}
	}
	// write out the item data
	w, err := makeStream(z, "item-info.json")
	if err != nil {
		return err
	}
	return writeItemInfo(w, item)
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

// copies all the blobs in the bundle src to the (new) bundle target, except for blobs with an id
// in except.
func (rmp *romp) copyBundleExcept(item *Item, src, target int, except []BlobID) error {
	rac, size, err := rmp.s.Open(sugar(item.ID, src), item.ID)
	if err != nil {
		return err
	}
	defer rac.Close()
	z, err := zip.NewReader(rac, size)
	if err != nil {
		return err
	}
	var badnames = []string{"item-info.json"}
	for _, i := range except {
		badnames = append(badnames, fmt.Sprintf("blob/%d", i))
	}
	var toclose []io.ReadCloser
	var blobcopies []BlobData
	defer func() {
		for i := range toclose {
			toclose[i].Close()
		}
	}()
	for _, f := range z.File {
		if contains(badnames, f.Name) {
			continue
		}
		var rc io.ReadCloser
		rc, err = f.Open()
		if err == nil {
			toclose = append(toclose, rc)
			blobcopies = append(blobcopies,
				BlobData{id: extractBlobId(f.Name),
					r: rc})
		}
	}
	rmp.SaveItem(item, target, blobcopies)
	return err
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

type parentReadCloser struct {
	parent io.Closer
	io.ReadCloser
}

func (r *parentReadCloser) Close() error {
	r.ReadCloser.Close()
	return r.parent.Close()
}

var (
	ErrNotFound = errors.New("stream not found")
)

func (rmp *romp) openZipStream(key, sname string) (io.ReadCloser, error) {
	rac, size, err := rmp.s.Open(key, key)
	if err != nil {
		return nil, err
	}
	// Don't leak the open rac.
	// Close it if we did not make a zstream to hold it.
	var result *parentReadCloser
	defer func() {
		if result == nil {
			rac.Close()
		}
	}()
	z, err := zip.NewReader(rac, size)
	if err != nil {
		return nil, err
	}
	err = ErrNotFound
	for _, f := range z.File {
		if f.Name != sname {
			continue
		}
		var rc io.ReadCloser
		rc, err = f.Open()
		if err == nil {
			result = &parentReadCloser{
				parent:     rac,
				ReadCloser: rc,
			}
		}
		break
	}
	return result, err
}

func readItemInfo(rc io.Reader) (*Item, error) {
	var fromTape itemOnTape
	decoder := json.NewDecoder(rc)
	err := decoder.Decode(fromTape)
	if err != nil {
		return nil, err
	}
	result := &Item{
		ID: fromTape.ItemID,
	}
	for _, ver := range fromTape.Versions {
		v := &Version{
			ID:        VersionID(ver.VersionID),
			SaveDate:  ver.CreatedDate,
			CreatedBy: ver.CreatedBy,
			Note:      ver.Note,
			Slots:     ver.Slots,
		}
		result.versions = append(result.versions, v)
	}
	for _, blob := range fromTape.Blobs {
		b := &Blob{
			ID:      BlobID(blob.BlobID),
			Created: time.Now(),
			Creator: "",
			Size:    blob.ByteCount,
			Bundle:  blob.Bundle,
		}
		b.MD5, _ = hex.DecodeString(blob.MD5)
		b.SHA256, _ = hex.DecodeString(blob.SHA256)
		result.blobs = append(result.blobs, b)
	}
	// TODO(dbrower): handle deleted blobs
	return result, nil
}

func writeItemInfo(w io.Writer, item *Item) error {
	itemStore := itemOnTape{
		ItemID: item.ID,
	}
	encoder := json.NewEncoder(w)
	return encoder.Encode(itemStore)
}

// on tape serialization data.
// Use this indirection so that we can change Item without worrying about
// being able to read data previously serialized
type itemOnTape struct {
	ItemID          string
	ByteCount       int
	ActiveByteCount int
	BlobCount       int
	CreatedDate     time.Time
	ModifiedDate    time.Time
	VersionCount    int
	Versions        []struct {
		VersionID   int
		CreatedDate time.Time
		SlotCount   int
		ByteCount   int64
		BlobCount   int
		CreatedBy   string
		Note        string
		Slots       map[string]BlobID
	}
	Blobs []struct {
		BlobID    int
		Bundle    int
		ByteCount int64
		MD5       string
		SHA256    string
	}
	Deleted []struct {
		BlobID      string
		DeletedBy   string
		DeletedDate time.Time
		Note        string
	}
}
