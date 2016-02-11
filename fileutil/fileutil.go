package fileutil

// A catchall package for bclient file operations and data structures.
// May need decomposition if too unwieldy

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"github.com/antonholmquist/jason"
	"os"
	"path"
	"path/filepath"
	"strings"
)

var (
	verbose bool
)

type ListData struct {
	localFileList  *FileList
	remoteFileList *FileList
	FilesWalked    chan string
	rootPrefix     string
}

//A public Method to get the Md5 sum of file on the upload list

func (ld *ListData) ShowUploadFileMd5(fileName string) []byte {

	return ld.localFileList.Files[fileName][1]
}

// Print out remote file list
func (ld *ListData) PrintRemoteList() {
	for fileName, map1 := range ld.remoteFileList.Files {
		for versionID, md5 := range map1 {
			fmt.Printf("File: %s blob %d md5 %s\n", fileName, versionID, hex.EncodeToString(md5))
		}
	}
}

// Print out remote blob list
func (ld *ListData) PrintBlobList() {
	for md5, blob := range ld.remoteFileList.Blobs {
		fmt.Printf("BlobId[ %s ] = %d\n", md5, blob)
	}
	fmt.Printf("\n")
}

// Print out local file list
func (ld *ListData) PrintLocalList() {

	for key, value := range ld.localFileList.Files {
		for _, md5 := range value {
			fmt.Printf("File: %s md5 %s\n", key, hex.EncodeToString(md5))
		}
	}

}

func NewLists(root string) *ListData {
	this := new(ListData)
	this.rootPrefix = root
	this.FilesWalked = make(chan string)

	return this
}

func SetVerbose(isVerbose bool) {
	verbose = isVerbose
}

func (ld *ListData) CreateUploadList(files string) {

	filepath.Walk(path.Join(ld.rootPrefix, files), ld.addToUploadList)

	close(ld.FilesWalked)
}

// addToUploadList is called by fileUtil.CreatUploadList  once for each file under filepath.walk()

func (ld *ListData) addToUploadList(filePath string, info os.FileInfo, err error) error {

	if err != nil {
		return err
	}

	// We only want files in the list- leave directories out
	// If the directory name starts with '.', don't walk it.

	if info.IsDir() {

		dirName := path.Base(filePath)

		if strings.HasPrefix(dirName, ".") {
			return filepath.SkipDir
		} else {
			return nil
		}
	}

	ld.FilesWalked <- strings.TrimPrefix(filePath, ld.rootPrefix+"/")

	return nil
}

func (ld *ListData) ComputeLocalChecksums() {

	ld.localFileList = New(ld.rootPrefix)
	ld.localFileList.BuildListFromChan(ld.FilesWalked)
}

func (ld *ListData) BuildRemoteList(json *jason.Object) {
	ld.remoteFileList = New(ld.rootPrefix)
	ld.remoteFileList.BuildListFromJSON(json)
}

func (ld *ListData) BuildLocalList(json *jason.Object) {
	ld.localFileList = New(ld.rootPrefix)
	ld.localFileList.BuildListFromJSON(json)
}

// If latest version of local file already exists on server don't upload it

func (ld *ListData) CullLocalList() {

	// item does not exist on remote bendo server
	if ld.remoteFileList == nil {
		return
	}

	for localFile, localMD5 := range ld.localFileList.Files {

		remoteMD5Map := ld.remoteFileList.Files[localFile]

		if remoteMD5Map == nil {
			continue
		}

		for blobID, remoteMD5 := range remoteMD5Map {
			if bytes.Compare(localMD5[1], remoteMD5) == 0 && ld.remoteFileList.Blobs[localFile] == blobID {
				delete(ld.localFileList.Files, localFile)
				continue
			}
		}

	}
}

// Compare the MD5Sum of the local file with all blobs that exist on the bendo server
// if one is found, return its blobID + false; otherwise, return true

func (ld *ListData) IsUploadNeeded(fileName string) (int64, bool) {

	localMD5 := ld.localFileList.Files[fileName][1]

	if ld.remoteFileList == nil {
		return 0, true
	}

	blobID := ld.remoteFileList.Blobs[hex.EncodeToString(localMD5)]

	if blobID == 0 {
		return 0, true
	}

	return blobID, false
}

func (ld *ListData) BuildLocalFromFiles(files []string) {

	// item does not exist on remote bendo server
	if ld.remoteFileList == nil {
		return
	}

	// create empty local list
	ld.localFileList = New(ld.rootPrefix)

	for localFile := range files {

		remoteMD5Map := ld.remoteFileList.Files[files[localFile]]

		if remoteMD5Map == nil {
			continue
		}

		ld.localFileList.Files[files[localFile]] = remoteMD5Map
	}
}

func (ld *ListData) IsLocalListEmpty() bool {

	if len(ld.localFileList.Files) == 0 {
		return true
	}
	return false
}

func (ld *ListData) QueueFiles(fileQueue chan string) {

	ld.localFileList.AddToSendQueue(fileQueue)
}
