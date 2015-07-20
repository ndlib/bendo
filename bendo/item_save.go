package bendo

import (
	"encoding/hex"
	"encoding/json"
	"io"
	"time"
)

/*
Low level routines to serialize and deserialize items from the storage
interface, which is abstracted by a BundleStore.
*/

func readItemInfo(rc io.Reader) (*Item, error) {
	var fromTape itemOnTape
	decoder := json.NewDecoder(rc)
	err := decoder.Decode(&fromTape)
	if err != nil {
		return nil, err
	}
	result := &Item{
		ID: fromTape.ItemID,
	}
	for _, ver := range fromTape.Versions {
		v := &Version{
			ID:       VersionID(ver.VersionID),
			SaveDate: ver.SaveDate,
			Creator:  ver.Creator,
			Note:     ver.Note,
			Slots:    ver.Slots,
		}
		result.Versions = append(result.Versions, v)
	}
	for _, blob := range fromTape.Blobs {
		b := &Blob{
			ID:       BlobID(blob.BlobID),
			SaveDate: blob.SaveDate,
			Creator:  blob.Creator,
			Size:     blob.ByteCount,
			Bundle:   blob.Bundle,
		}
		b.MD5, _ = hex.DecodeString(blob.MD5)
		b.SHA256, _ = hex.DecodeString(blob.SHA256)
		result.Blobs = append(result.Blobs, b)
	}
	return result, nil
}

func writeItemInfo(w io.Writer, item *Item) error {
	itemStore := itemOnTape{
		ItemID: item.ID,
	}
	var byteCount int64
	for _, b := range item.Blobs {
		byteCount += b.Size
		bTape := blobTape{
			BlobID:    int(b.ID),
			Bundle:    b.Bundle,
			ByteCount: b.Size,
			MD5:       hex.EncodeToString(b.MD5),
			SHA256:    hex.EncodeToString(b.SHA256),
			SaveDate:  b.SaveDate,
			Creator:   b.Creator,
		}
		if b.Deleter != "" {
			bTape.DeleteDate = b.DeleteDate
			bTape.Deleter = b.Deleter
			bTape.DeleteNote = b.DeleteNote
		}
		itemStore.Blobs = append(itemStore.Blobs, bTape)
	}
	for _, v := range item.Versions {
		vTape := versionTape{
			VersionID: int(v.ID),
			SaveDate:  v.SaveDate,
			Creator:   v.Creator,
			Slots:     v.Slots,
		}
		itemStore.Versions = append(itemStore.Versions, vTape)
	}
	itemStore.ByteCount = byteCount
	encoder := json.NewEncoder(w)
	return encoder.Encode(itemStore)
}

// on tape serialization data.
// Use this indirection so that we can change Item without worrying about
// being able to read data previously serialized
type itemOnTape struct {
	ItemID    string
	ByteCount int64
	Versions  []versionTape
	Blobs     []blobTape
}

type versionTape struct {
	VersionID int
	SaveDate  time.Time
	ByteCount int64
	BlobCount int
	Creator   string
	Note      string
	Slots     map[string]BlobID
}

type blobTape struct {
	BlobID     int
	Bundle     int
	ByteCount  int64
	MD5        string
	SHA256     string
	SaveDate   time.Time
	Creator    string
	DeleteDate time.Time
	Deleter    string
	DeleteNote string
}
