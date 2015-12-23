package main

//The bclient tool is meant to be invoked by the CurateND batch ingest procees

import (
	"flag"
	"fmt"

	"github.com/ndlib/bendo/cmd/bclient/fileutil"
	"github.com/ndlib/bendo/cmd/bclient/bserver"
)

var (
	fileroot = flag.String("root", ".", "root prefix to upload files")
	server   = flag.String("server", "127.0.0.1:14000", "Bendo Server to Use")
	creator  = flag.String("creator", "butil", "Creator name to use")
	verbose  = flag.Bool("v", false, "Display more information")
	usage    = `
butil <command> <file> <command arguments>

Possible commands:
    upload  <item id> <blob number>

    get <item id list>

`
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
		doUpload( args[1], args[2])
	case "get":
		doGet( args[1])
	}

}

func doUpload(  item string, files string) {

	bserver.Init()
	fileutil.InitFileListStep( *fileroot)

        go fileutil.CreateUploadList( files) 	
        go fileutil.ComputeLocalChecksums( item) 	
	//go bserver.FetchItemInfo( item )
	fileutil.WaitFileListStep()

	// Item Not Found on server, this is a new item.
	// If Item Exists on server, need to figure differences
	// Allow Deletions, or is this a versioning issue?
        
	//if bserver.ItemFetchStatus != ItemNotFound {
	//	fileutil.Compare( fileutil.List[item] , bserver.List[item]) 
	//}

	// If there's anything left, upload it
}

func doGet(item string) {
	fmt.Printf("Item = %s\n", item)
}

