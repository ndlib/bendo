// This package manages a list of files, and their checksums

package fileutil

import (
	"crypto/md5"
	"encoding/base64"
	"fmt"
	"github.com/antonholmquist/jason"
	"io"
	"os"
	"path"
	"sort"
	"strings"
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
		fReader, ferr := os.Open(path.Join( f.Root,fileName))

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
	for _, version := range versionArray {
		//versionID, _ := version.GetInt64("ID")
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
		}
	}
}

// Parse item JSON returned from bendo get item  for ls action

func PrintLsFromJSON(json *jason.Object, version int, long bool, blobs bool, item string) {

	var thisVersion int

	blobArray, _ := json.GetObjectArray("Blobs")
	versionArray, _ := json.GetObjectArray("Versions")

	// if version set to zero, use lastest, else use the one given

	if version == 0 {
		thisVersion = len(versionArray)
	} else {
		thisVersion = version
	}

	// Find the version in the JSON, and its subtending slot map

	versionElement := versionArray[thisVersion-1]
	slotMap, _ := versionElement.GetObject("Slots")

	// sort the Slot Keys (filenames) in an array

	keyMap := []string{}

	for key, _ := range slotMap.Map() {
		keyMap = append(keyMap, key)

	}

	sort.Strings(keyMap)

	// Print the slots in the sorted order

	if long {
		fmt.Println("Blob       Size Date                 Creator File")
		fmt.Println("-------------------------------------------------------------------------------------")
	}

	for i := range keyMap {

		if long {
			blobID, _ := slotMap.GetInt64(keyMap[i])
			itemSize, _ := blobArray[blobID-1].GetInt64("Size")
			saveDate, _ := blobArray[blobID-1].GetString("SaveDate")
			creator, _ := blobArray[blobID-1].GetString("Creator")

			fmt.Printf("%03d %12d %s %-8s ", blobID, itemSize, strings.Split(strings.Replace(saveDate, "T", " ", 1), ".")[0], creator)
		}

		fmt.Printf("%s/", item)

		if version != 0 {
			fmt.Printf("@%d/", thisVersion)
		}

		fmt.Printf("%s ", keyMap[i])
		fmt.Printf("\n")
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
			fmt.Printf("Error: could not create directory %s\n%s\n", err.Error())
			return
		}

		filePtr, err := os.Create(targetFile)

		if err != nil {
			fmt.Printf("Error: could not file directory %s\n%s\n", err.Error())
			return
		}

		err = filePtr.Close()

		if err != nil {
			fmt.Printf("Error: could not close file %s\n%s\n", err.Error())
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
