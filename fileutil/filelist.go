// This package manages a list of files, and their checksums

package fileutil

import (
	"crypto/md5"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path"
	"sort"
	"strings"

	"github.com/antonholmquist/jason"
)

// Our underlying data structure
// Blobs id used only for lists built from server data

type FileList struct {
	Root  string
	Files map[string]map[int64][]byte
	Blobs map[string]int64
}

// Create an empty FileList

func New(root string) *FileList {
	fl := new(FileList)
	fl.Root = root
	fl.Files = make(map[string]map[int64][]byte)
	fl.Blobs = make(map[string]int64)

	return fl
}

// Construct a FileList from a channel of filenames - open local file, and compute checksums

func (f *FileList) BuildListFromChan(filePipe <-chan string) {

	for fileName := range filePipe {
		// Open the local file
		fReader, ferr := os.Open(path.Join(f.Root, fileName))

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

// add file to the send queue

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

	// build lovely mapping of md5 to blobs
	for _, blob := range blobArray {
		md5Sum, _ := blob.GetString("MD5")
		DecodedMD5, _ := base64.StdEncoding.DecodeString(md5Sum)
		blobID, _ := blob.GetInt64("ID")
		f.Blobs[hex.EncodeToString(DecodedMD5)] = blobID
	}

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

			f.Files[key][blobID] = DecodedMD5

			// maps files in latest version to their blobs
			if versionID == int64(len(versionArray)) {
				f.Blobs[key] = blobID
			}
		}
	}
}

// Parse item JSON returned from bendo get item  for ls action
func PrintLsFromJSON(json *jason.Object, version int, long bool, blobs bool, item string) {
	// note: the blobs parameter is unused. remove it?

	var displayVersion bool

	blobArray, _ := json.GetObjectArray("Blobs")
	versionArray, _ := json.GetObjectArray("Versions")

	// if version set to zero, use lastest, else use the one given
	if version < 0 || version > len(versionArray) {
		fmt.Printf("Version %d is out of range\n", version)
		return
	} else if version == 0 {
		version = len(versionArray)
	} else {
		// if a specific version is desired, display it in the file listing
		// to make copy and paste easy
		displayVersion = true
	}

	// Find the version in the JSON, and its subtending slot map
	slotMap, _ := versionArray[version-1].GetObject("Slots")

	// sort the Slot Keys (filenames) in an array
	var keyMap []string

	for key, _ := range slotMap.Map() {
		keyMap = append(keyMap, key)
	}

	sort.Strings(keyMap)

	// Print the slots in the sorted order

	if long {
		fmt.Println(" Blob        Bytes Uploaded            Creator  File")
		fmt.Println("-------------------------------------------------------------------------------------")
	}

	for i := range keyMap {

		if long {
			blobID, _ := slotMap.GetInt64(keyMap[i])
			itemSize, _ := blobArray[blobID-1].GetInt64("Size")
			saveDate, _ := blobArray[blobID-1].GetString("SaveDate")
			creator, _ := blobArray[blobID-1].GetString("Creator")

			fmt.Printf("%5d %12d %s %-8s ",
				blobID,
				itemSize,
				strings.Split(strings.Replace(saveDate, "T", " ", 1), ".")[0],
				creator)
		}

		fmt.Printf("%s/", item)
		if displayVersion {
			fmt.Printf("@%d/", version)
		}
		fmt.Printf("%s\n", keyMap[i])
	}
}

func MakeStubFromJSON(json *jason.Object, item string, pathPrefix string) {

	versionArray, _ := json.GetObjectArray("Versions")

	thisVersion := len(versionArray)

	// Find the version in the JSON, and its subtending slot map

	versionElement := versionArray[thisVersion-1]
	slotMap, _ := versionElement.GetObject("Slots")

	for key, _ := range slotMap.Map() {
		targetFile := path.Join(pathPrefix, key)

		// create target directory, return on error

		targetDir, _ := path.Split(targetFile)

		err := os.MkdirAll(targetDir, 0755)

		if err != nil {
			fmt.Printf("Error: could not create directory %s\n%s\n", targetDir, err.Error())
			return
		}

		filePtr, err := os.Create(targetFile)

		if err != nil {
			fmt.Printf("Error: could not create file %s\n%s\n", targetFile, err.Error())
			return
		}

		err = filePtr.Close()

		if err != nil {
			fmt.Printf("Error: could not close file %s\n%s\n", targetFile, err.Error())
			return
		}

		fmt.Println(targetFile)
	}

}

func PrintListFromJSON(json *jason.Object) {

	versionArray, _ := json.GetObjectArray("Versions")

	for _, version := range versionArray {
		versionID, _ := version.GetInt64("ID")
		saveDate, _ := version.GetString("SaveDate")
		creator, _ := version.GetString("Creator")
		note, _ := version.GetString("Note")

		if note == "" {
			note = "\"\""
		}

		fmt.Printf("@%02d %s %s %s\n", versionID, strings.Split(strings.Replace(saveDate, "T", " ", 1), ".")[0], creator, note)
	}
}
