package bendo

import (
	"archive/zip"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"time"
)

/*
Low level routines to serialize and deserialize items from the storage
interface, which is abstracted by a BundleStore.
*/

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

func (dty *Directory) openZipStream(key, sname string) (io.ReadCloser, error) {
	rac, size, err := dty.s.Open(key, key)
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
			ID:       VersionID(ver.VersionID),
			SaveDate: ver.CreatedDate,
			Creator:  ver.CreatedBy,
			Note:     ver.Note,
			Slots:    ver.Slots,
		}
		result.versions = append(result.versions, v)
	}
	for _, blob := range fromTape.Blobs {
		b := &Blob{
			ID:       BlobID(blob.BlobID),
			SaveDate: time.Now(),
			Creator:  "",
			Size:     blob.ByteCount,
			Bundle:   blob.Bundle,
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
