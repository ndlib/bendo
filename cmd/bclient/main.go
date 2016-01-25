package main

//The bclient tool is meant to be invoked by the CurateND batch ingest procees

import (
	"flag"
	"fmt"
	"sync"

	"github.com/ndlib/bendo/cmd/bclient/bserver"
	"github.com/ndlib/bendo/cmd/bclient/fileutil"
)

var (
	fileroot     = flag.String("root", ".", "root prefix to upload files")
	server       = flag.String("server", "libvirt9.library.nd.edu:14000", "Bendo Server to Use")
	creator      = flag.String("creator", "butil", "Creator name to use")
	verbose      = flag.Bool("v", false, "Display more information")
	numuploaders = flag.Int("ul", 2, "Number Uploaders")
	usage        = `
bclient <command> <file> <command arguments>

Possible commands:
    upload  <item id> <blob number>

    get <item id list>

`
	FilesToSend  = make(chan string)
	SendFileDone sync.WaitGroup
)

func main() {
	flag.Parse()
	fileutil.SetVerbose(*verbose)

	args := flag.Args()

	if len(args) == 0 {
		return
	}

	switch args[0] {
	case "upload":
		doUpload(args[1], args[2])
	case "get":
		doGet(args[1])
	}

}

func doUpload(item string, files string) {

	bserver.Init(server)
	fileutil.InitFileListStep(*fileroot)

	go fileutil.CreateUploadList(files)
	go fileutil.ComputeLocalChecksums()
	go bserver.FetchItemInfo(item)
	fileutil.WaitFileListStep()

	// If FetchItemInfo returns ErrNotFound it's anew item- upload whole local list
	// If FetchItemInfo retuens other error, bendo unvavailablr for upload- abort!
	// default: build remote filelist of returned json, diff against local list, upload remainder

	switch {
	case bserver.ItemFetchStatus() == bserver.ErrNotFound:
		break
	case bserver.ItemFetchStatus() != nil:
		fmt.Println(bserver.ItemFetchStatus())
		return
	default:
		fileutil.BuildRemoteList(bserver.RemoteJason)
		// This compares the local list with the remote list (if the item already exists)
		// and eliminates any unnneeded duplicates
		fileutil.CullLocalList()
		break
	}

	// Culled list is empty, nothing to upload

	if fileutil.IsLocalListEmpty() {
		fmt.Printf("Nothing to do:\nThe vesions of All Files given for upload in item %s\nare already present on the server\n", item)
		return
	}

	fileutil.PrintLocalList()

	// set up our barrier, that will wait for all the file chunks to be uploaded
	SendFileDone.Add(*numuploaders)

	//Spin off desire number of upload workers
	for cnt := int(0); cnt < *numuploaders; cnt++ {
		go bserver.SendFiles(FilesToSend, item, *fileroot, &SendFileDone)
	}

	fileutil.QueueFiles(FilesToSend)

	// wait for all file chunks to be uploaded
	SendFileDone.Wait()

	// chunks uploaded- submit trnsaction to add FileIDs to item
	transErr := bserver.SendTransactionRequest(item)

	if transErr != nil {
		fmt.Println(transErr)
		return
	}

	// TODO: cleanup uploads on success
}

func doGet(item string) {
	fmt.Printf("Item = %s\n", item)
}
