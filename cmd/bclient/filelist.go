// This package manages a list of files, and their checksums

package main

import (
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"os"
	"path"
	"sort"
	"strings"

	"github.com/antonholmquist/jason"
)

// Our underlying data structure
// Blobs id used only for lists built from server data

type FileList struct {
	// root of file tree in file system
	Root string

	// mapping of path to info record for each file
	Files map[string]File

	// mapping of md5 sum (as hex) to blob id.
	// also mapping of file path to blob id.
	Blobs map[string]int64
}

// A File represents information for both local and remote files,
// although the meaning of the fields is slightly different
// for the two cases.
// For local files, MD5 is the hash of the complete file.
// The BlobID is either 0, if unassigned, >0 if this file is
// the same as a remote blob, and <0 if this file is a new blob that has to be
// uploaded to the server.
type File struct {
	Name     string // relative path for the file
	AbsPath  string // (local only) absolute path to file
	MD5      []byte
	SHA256   []byte
	MimeType string
	BlobID   int64 // 0 if nothing has been assigned yet
}

// Create an empty FileList

func New(root string) *FileList {
	fl := new(FileList)
	fl.Root = root
	fl.Files = make(map[string]File)
	fl.Blobs = make(map[string]int64)

	return fl
}

// AddFiles reads `File` structures in on a channel and merges them
// with the files already in the list. New files are added. Other files
// are merged with the new file winning. Only fields that are not zero are
// merged.
func (fl *FileList) AddFiles(in <-chan File) {
	for f := range in {
		info := fl.Files[f.Name]
		if f.AbsPath != "" {
			info.AbsPath = f.AbsPath
		}
		if len(f.MD5) > 0 {
			info.MD5 = f.MD5
		}
		if len(f.SHA256) > 0 {
			info.SHA256 = f.SHA256
		}
		if len(f.MimeType) > 0 {
			info.MimeType = f.MimeType
		}
		if f.BlobID != 0 {
			info.BlobID = f.BlobID
		}
		fl.Files[f.Name] = info
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

	if len(versionArray) == 0 {
		// huh? why is this zero?
		return
	}
	// only care about the file mappings in the newest version
	version := versionArray[len(versionArray)-1]
	slotMap, _ := version.GetObject("Slots")

	for key, value := range slotMap.Map() {
		blobID, _ := value.Int64()
		md5Sum, _ := blobArray[blobID-1].GetString("MD5")
		DecodedMD5, _ := base64.StdEncoding.DecodeString(md5Sum)

		info := f.Files[key]
		info.BlobID = blobID
		info.MD5 = DecodedMD5
		info.MimeType, _ = blobArray[blobID-1].GetString("MimeType")
		f.Files[key] = info

		f.Blobs[key] = blobID
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
	slotMap, _ := versionArray[thisVersion-1].GetObject("Slots")

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
