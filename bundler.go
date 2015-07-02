package bendo

import (
	"archive/zip"
	"encoding/json"
	"io"
)

// The item bundler maps between the in-memory representation of an item
// and its serialization into many zip files on disk or tape.

func FindBlobInfo(b xbid) (*blob, error) {
	// if blob metadata cached
	//	...
	// else
	//	...
	// is blob cached?
}

func FindBlob(b xbid) (io.Reader, error) {
	// if blob in cache, return it

	// else load it from system and return error
}

func loadblob(b xbid) {
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

type item struct {
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
