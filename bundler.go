package bendo

import (
	"archive/zip"
	"encoding/json"
	"io"
)

// The item bundler maps between the in-memory representation of an item
// and its serialization into many zip files on disk or tape.

type Bendo struct {
	Metadata cache
	Blobs    cache
}

// do we keep one blob structure in memory for EACH blob and share it
// or do we load a blob structure on demand?

func (bo *Bendo) FindBlobInfo(b XBid) (*Blob, error) {
	var key = b.CacheKey()
	b, ok := bo.Metadata.Find(key)
	if !ok {
		// get blob metadata
	}
	b.Cached = bo.Blobs.Has(key)
	return b
}

const (
	// ErrNotCached means an item is being fetched from tape. Retry the
	// operation after some delay
	ErrNotCached = errors.Error("Not Cached")
)

func FindBlob(b XBid) (io.Reader, error) {
	var key = b.CacheKey()
	b, ok := bo.Blobs.Find(key)
	if !ok {
		// fetch blob
		return nil, ErrNotCached
	}
	// blob in cache, return it
}

func loadblob(b XBid) {
}

func FindItem(id string) {
}

func readZipFile(fname string) (*item, error) {
	var result *item
	z, err := zip.OpenReader(fname)
	if err != nil {
		return nil, err
	}
	defer z.Close()

	// find info file
	for _, f := range z.File {
		if f.Name == "item-info.json" {
			rc, err = f.Open()
			if err == nil {
				result, err = readItemInfo(rc)
				rc.Close()
			}
			break
		}
	}
	return result, err
}

func readItemInfo(rc io.Reader) (*item, error) {
	decoder := json.NewDecoder(rc)
	result = new(item)
	err := deocder.Decode(result)
	return result, err
}

// on tape serialization data
type itemOnTape struct {
	ItemID          string
	ByteCount       int
	ActiveByteCount int
	BlobCount       int
	CreatedDate     date.DateTime
	ModifiedDate    date.DateTime
	VersionCount    int
	Versions        []struct {
		VersionID   int
		Iteration   int
		CreatedDate date.DateTime
		SlotCount   int
		ByteCount   int
		BlobCount   int
		CreatedBy   string
		Note        string
		Slots       map[string]blobid
	}
	Blobs []struct {
		BlobID          string
		OriginalVersion int
		ByteCount       int
		MD5             string
		SHA256          string
	}
	Deleted []struct {
		BlobID      string
		DeletedBy   string
		DeletedDate date.DateTime
		Note        string
	}
}
