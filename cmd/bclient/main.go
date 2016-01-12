package main

//The bclient tool is meant to be invoked by the CurateND batch ingest procees

import (
	"flag"
	"fmt"

	"github.com/ndlib/bendo/cmd/bclient/bserver"
	"github.com/ndlib/bendo/cmd/bclient/fileutil"
)

var (
	fileroot = flag.String("root", ".", "root prefix to upload files")
	server   = flag.String("server", "libvirt9.library.nd.edu:14000", "Bendo Server to Use")
	creator  = flag.String("creator", "butil", "Creator name to use")
	verbose  = flag.Bool("v", false, "Display more information")
	usage    = `
bclient <command> <file> <command arguments>

Possible commands:
    upload  <item id> <blob number>

    get <item id list>

`
	FilesToSend = make(chan string)
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

	// Compare Local and remote Lists- what reamins in Local List we'll Send
	fileutil.CullLocalList()

	//	fileutil.PrintLocalList()

	//	go bserver.SendFiles( FilesToSend , item, *fileroot )
	go bserver.SendFiles(FilesToSend, item, *fileroot)

	fileutil.QueueFiles(FilesToSend)
}

func doGet(item string) {
	fmt.Printf("Item = %s\n", item)
}
