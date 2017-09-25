package main

// A catchall package for bclient file operations and data structures.
// May need decomposition if too unwieldy

import (
	"encoding/hex"
	"fmt"

	"github.com/antonholmquist/jason"
)

type ListData struct {
	Local      *FileList
	Remote     *FileList
	rootPrefix string
}

// Print out remote blob list
func (fl *FileList) PrintBlobs() {
	for md5, blob := range fl.Blobs {
		fmt.Printf("BlobId[ %s ] = %d\n", md5, blob)
	}
	fmt.Printf("\n")
}

func (fl *FileList) PrintFiles() {
	for filename, info := range fl.Files {
		fmt.Printf("File: %s blob %d md5 %s\n",
			filename,
			info.BlobID,
			hex.EncodeToString(info.MD5))
	}
}

func NewLists(root string) *ListData {
	return &ListData{rootPrefix: root}
}

func (ld *ListData) BuildRemoteList(json *jason.Object) {
	ld.Remote = New(ld.rootPrefix)
	ld.Remote.BuildListFromJSON(json)
}

func (ld *ListData) BuildLocalList(json *jason.Object) {
	ld.Local = New(ld.rootPrefix)
	ld.Local.BuildListFromJSON(json)
}

func (ld *ListData) BuildLocalFromFiles(files []string) {
	// item does not exist on remote bendo server
	if ld.Remote == nil {
		return
	}

	// create empty local list
	ld.Local = New(ld.rootPrefix)

	for _, fname := range files {
		remoteInfo := ld.Remote.Files[fname]

		if len(remoteInfo.MD5) == 0 {
			continue
		}

		ld.Local.Files[fname] = remoteInfo
	}
}

func (ld *ListData) QueueFiles(fileQueue chan string) {
	ld.Local.AddToSendQueue(fileQueue)
}
