// This package manages a list of files, and their checksums

package fileutil

import (
	"crypto/md5"
	"encoding/base64"
	"fmt"
	"github.com/antonholmquist/jason"
	"io"
	"os"
)

// Our underlying data structure

type FileList struct {
	Root  string
	Files map[string]map[int64][]byte
}

// Create an empty FileList

func New(root string) *FileList {
	fl := new(FileList)
	fl.Root = root
	fl.Files = make(map[string]map[int64][]byte)

	return fl
}

// Construct a FileList from a channel of filenames - open local file, and compute checksums

func (f *FileList) BuildListFromChan(filePipe <-chan string) {

	for fileName := range filePipe {
		// Open the local file
		fReader, ferr := os.Open(f.Root + "/" + fileName)

		if ferr != nil {
			fmt.Println(ferr)
			continue
		}

		md5w := md5.New()

		// Copy from the Reader into the Writer (this will compute the CheckSums)

		io.Copy(md5w, fReader)

		// Get the Checksums

		md5Sum := md5w.Sum(nil)

		innerMap, ok := f.Files[fileName]

		if !ok {
			innerMap = make(map[int64][]byte)
			f.Files[fileName] = innerMap
		}

		f.Files[fileName][1] = md5Sum

	}
}

func (f *FileList) AddToSendQueue(sendQueue chan string) {

	defer close(sendQueue)

	for file, _ := range f.Files {
		sendQueue <- file
	}
}

// Construct a FileList from a JSON return by the Bendo API

func (f *FileList) BuildListFromJSON(json *jason.Object) {

	blobArray, _ := json.GetObjectArray("Blobs")
	versionArray, _ := json.GetObjectArray("Versions")
	for _, version := range versionArray {
		versionID, _ := version.GetInt64("ID")
		slotMap, _ := version.GetObject("Slots")

		for key, _ := range slotMap.Map() {

			blobID, _ := slotMap.GetInt64(key)
			md5Sum, _ := blobArray[blobID-1].GetString("MD5")
			DecodedMD5, _ := base64.StdEncoding.DecodeString(md5Sum)

			innerMap, ok := f.Files[key]

			if !ok {
				innerMap = make(map[int64][]byte)
				f.Files[key] = innerMap
			}

			f.Files[key][versionID] = DecodedMD5
		}
	}
}
