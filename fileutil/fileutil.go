package fileutil

// A catchall package for bclient file operations and data structures.
// May need decomposition if too unwieldy

import (
	"bytes"
	"fmt"
	"os"
	"path"
	"path/filepath"

	"github.com/antonholmquist/jason"
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

// Print out remote filer list
func (ld *ListData) PrintRemoteList() {
	for key, value := range ld.remoteFileList.Files {
		fmt.Println("Key:", key, "Value:", value)
	}
}

// Print out local file list
func (ld *ListData) PrintLocalList() {

	for key, value := range ld.localFileList.Files {
		fmt.Println("Key:", key, "Value:", value[1])
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

func IfVerbose(output string) {
	if verbose {
		fmt.Println(output)
	}
}

func (ld *ListData) CreateUploadList(files string) {

	IfVerbose("CreateUploadList called")

	filepath.Walk(path.Join(ld.rootPrefix, files), ld.addToUploadList)

	close(ld.FilesWalked)
	IfVerbose("CreateUploadList exit")
}

// addToUploadList is called by fileUtil.CreatUploadList  once for each file under filepath.walk()

func (ld *ListData) addToUploadList(path string, info os.FileInfo, err error) error {

	if err != nil {
		return err
	}

	// We only want files in the list- leave directories out

	if info.IsDir() {
		return nil
	}

	ld.FilesWalked <- path

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

		for _, remoteMD5 := range remoteMD5Map {
			if bytes.Compare(localMD5[1], remoteMD5) == 0 {
				delete(ld.localFileList.Files, localFile)
				continue
			}
		}

	}
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
