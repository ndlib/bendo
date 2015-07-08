package bendo

import (
	"archive/zip"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"
)

/*
Low level routines to serialize and deserialize items from the storage
interface, which is abstracted by a BS2.

An item's metadata and blobs are grouped into "bundles", which are zip files.
Each bundle contains the complete up-to-date metadata information on an item,
as well as some number of blobs. Bundles are numbered, but they should not be
assumed to be numbered sequentially since deletions may remove some bundles.

There is no relationship between a bundle number and the versions of an item.
*/

// A romp is our item metadata registry
type romp struct {
	// our metadata cache store...the authoritative source is the bundles
	items map[string]*Item

	// our underlying bundle store
	s BS2
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
		rmp.items = items
	}()
	return out
}

func sugar(id string, n int) string {
	return fmt.Sprintf("%s-%04d", id, n)
}

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

func (rmp *romp) BlobContent(id string, n int, b BlobID) (io.ReadCloser, error) {
	// which bundle is this blob in?

	sname := fmt.Sprintf("blob/%d", b)
	return rmp.openZipStream(sugar(id, n), sname)
}

func (rmp *romp) SaveItem(item *Item, bd []BlobData) error {
	var n int
	ww, err := rmp.s.Create(sugar(item.ID, n), item.ID)
	if err != nil {
		return err
	}
	defer ww.Close()
	z := zip.NewWriter(ww)
	defer z.Close()
	// write out the item data
	header := zip.FileHeader{
		Name:   "item-info.json",
		Method: zip.Store,
	}
	w, err := z.CreateHeader(&header)
	if err != nil {
		return err
	}
	err = writeItemInfo(w, item)
	if err != nil {
		return err
	}
	// write out all the blobs
	for _, blob := range bd {
		header := zip.FileHeader{
			Name:   "blob/%d", // blob.id
			Method: zip.Store,
		}
		w, err := z.CreateHeader(&header)
		if err != nil {
			return err
		}
		_, err = io.Copy(w, blob.r)
		if err != nil {
			return err
		}
	}
	return nil
}

type zstream struct {
	parent io.Closer
	io.ReadCloser
}

func (zs *zstream) Closer() error {
	zs.ReadCloser.Close()
	return zs.parent.Close()
}

func (rmp *romp) openZipStream(key, sname string) (io.ReadCloser, error) {
	rac, size, err := rmp.s.Open(key, key)
	if err != nil {
		return nil, err
	}
	// Don't leak the open rac.
	// Close it if we did not make a zstream to hold it.
	var result *zstream
	defer func() {
		if result == nil {
			rac.Close()
		}
	}()
	z, err := zip.NewReader(rac, size)
	if err != nil {
		return nil, err
	}
	for _, f := range z.File {
		if f.Name != sname {
			continue
		}
		var rc io.ReadCloser
		rc, err = f.Open()
		if err == nil {
			result = &zstream{
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
			Parent:  result.ID,
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
