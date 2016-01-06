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
	server   = flag.String("server", "libvirt9.library.nd.edu:14000", "Bendo Server to Use")
	creator  = flag.String("creator", "butil", "Creator name to use")
	verbose  = flag.Bool("v", true, "Display more information")
	usage    = `
bclient <command> <file> <command arguments>

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

	bserver.Init( server)
	fileutil.InitFileListStep( *fileroot)

        go fileutil.CreateUploadList( files) 	
        go fileutil.ComputeLocalChecksums() 	
	go bserver.FetchItemInfo( item )
	fileutil.WaitFileListStep()

	switch {
	case  bserver.ItemFetchStatus == bserver.ErrNotFound:
		fmt.Println("1")
		fileutil.UseRemoteList = false
		break
	case bserver.ItemFetchStatus != nil:
		fmt.Println("2")
		fmt.Println(bserver.ItemFetchStatus)
		return
	default:
		fmt.Println("3")
		fileutil.BuildRemoteList( bserver.RemoteJason)
	        break
	}
	 
	fileutil.PrintLocalList()
//	fileutil.PrintRemoteList()
        
	// If there's anything left, upload it
}

func doGet(item string) {
	fmt.Printf("Item = %s\n", item)
}

