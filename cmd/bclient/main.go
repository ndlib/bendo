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

	switch {
	case bserver.ItemFetchStatus == bserver.ErrNotFound:
		break
	case bserver.ItemFetchStatus != nil:
		fmt.Println(bserver.ItemFetchStatus)
		return
	default:
		fileutil.BuildRemoteList(bserver.RemoteJason)
		break
	}

	// This compares the local list with the remote list (if the item already exists)
	// and eliminates any unnneeded duplicates

	//	fileutil.PrintLocalList()

	// set up our barrier, that will wait for all the file chunks to be uploaded
	SendFileDone.Add(*numuploaders)

	//Spin off desire number of upload workers
	for cnt := int(0); cnt < *numuploaders; cnt++ {
		go bserver.SendFiles(FilesToSend, item, *fileroot, &SendFileDone)
	}

	fileutil.QueueFiles(FilesToSend)

	// wait for all file chunks to be uploaded
	SendFileDone.Wait()
}

func doGet(item string) {
	fmt.Printf("Item = %s\n", item)
}
