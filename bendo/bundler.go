package bendo

import (
//"archive/zip"
//"encoding/json"
//"errors"
//"io"
//"time"
)

// The item bundler maps between the in-memory representation of an item
// and its serialization into many zip files on disk or tape.

type cache interface {
	Find(string) (interface{}, bool)
	Has(string) bool
}

type Bendo struct {
	Metadata cache
	Blobs    cache
}

// do we keep one blob structure in memory for EACH blob and share it
// or do we load a blob structure on demand?

/*
func (bo *Bendo) FindBlobInfo(bid XBid) (*Blob, error) {
	var key = bid.CacheKey()
	result, ok := bo.Metadata.Find(key)
	if !ok {
		// get blob metadata
	}
	//result.Cached = bo.Blobs.Has(key)
	//return result, nil
}

const (
	// ErrNotCached means an item is being fetched from tape. Retry the
	// operation after some delay
	ErrNotCached = errors.Errorf("Not Cached")
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

func readZipFile(fname string) (*Item, error) {
	var result *Item
	z, err := zip.OpenReader(fname)
	if err != nil {
		return nil, err
	}
	defer z.Close()

	// find info file
	for _, f := range z.File {
		if f.Name == "item-info.json" {
			var rc io.ReadCloser
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

func readItemInfo(rc io.Reader) (*Item, error) {
	decoder := json.NewDecoder(rc)
	result = new(Item)
	err := deocder.Decode(result)
	return result, err
}

// on tape serialization data
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
		Iteration   int
		CreatedDate time.Time
		SlotCount   int
		ByteCount   int
		BlobCount   int
		CreatedBy   string
		Note        string
		Slots       map[string]BlobID
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
		DeletedDate time.Time
		Note        string
	}
}

func ItemList() <-chan string {

}

// implements a copy-on-write store mirroring
// another preservation system. Requires Read
// access to the other store.
func NewProxyStore(save BundleStore, source BundleReadStore) BundleStore {
}

type MetadataRegistry interface {
	Item(id string)
	Version()
	Blob()
}
*/
