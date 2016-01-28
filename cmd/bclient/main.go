package main

//The bclient tool is meant to be invoked by the CurateND batch ingest procees

import (
	"flag"
	"fmt"
	"sync"

	"github.com/ndlib/bendo/cmd/bclient/bserver"
	"github.com/ndlib/bendo/cmd/bclient/fileutil"
        "github.com/antonholmquist/jason"
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

    get <item> <files>
    list <item id>
    upload  <item id> <files>
    version <item id> 

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
		doUpload(args[1], args[2])
	case "list":
		doList(args[1])
	case "get":
		doGet(args[1], args[2])
	case "history":
		doHistory(args[1])
	}

}

func doUpload(item string, files string) {

	filesToSend := make(chan string)
	var upLoadDone sync.WaitGroup
	var sendFileDone sync.WaitGroup
	var json *jason.Object	
	var jsonFetchErr error

	thisItem := bserver.New(*server, item, *fileroot)
	fileLists := fileutil.NewLists(*fileroot)

	// Set up Barrier for 3 goroutines below
	upLoadDone.Add(3)

	// Fire 1!
	go func() {
		fileLists.CreateUploadList(files)
		upLoadDone.Done()
	}()

	// Fire 2!
	go func() {
		fileLists.ComputeLocalChecksums()
		upLoadDone.Done()
	}()

	// Fire 3!
	go func() {
		json, jsonFetchErr = thisItem.GetItemInfo()
		upLoadDone.Done()
	}()

	// Wait for everyone to finish
	upLoadDone.Wait()

	// If GetItemInfo returns ErrNotFound it's anew item- upload whole local list
	// If GetItemInfo returns other error, bendo unvavailable for upload- abort!
	// default: build remote filelist of returned json, diff against local list, upload remainder

	switch {
	case jsonFetchErr  == bserver.ErrNotFound:
		break
	case jsonFetchErr != nil:
		fmt.Println(jsonFetchErr)
		return
	default:
		fileLists.BuildRemoteList(json)
		// This compares the local list with the remote list (if the item already exists)
		// and eliminates any unnneeded duplicates
		fileLists.CullLocalList()
		break
	}

	// Culled list is empty, nothing to upload

	if fileLists.IsLocalListEmpty() {
		fmt.Printf("Nothing to do:\nThe vesions of All Files given for upload in item %s\nare already present on the server\n", item)
		return
	}

	fileLists.PrintLocalList()

	// set up our barrier, that will wait for all the file chunks to be uploaded
	sendFileDone.Add(*numuploaders)

	//Spin off desire number of upload workers
	for cnt := int(0); cnt < *numuploaders; cnt++ {
		go func() {
			thisItem.SendFiles(filesToSend, fileLists)
			sendFileDone.Done()
		}()
	}

	fileLists.QueueFiles(filesToSend)

	// wait for all file chunks to be uploaded
	sendFileDone.Wait()

	// chunks uploaded- submit trnsaction to add FileIDs to item
	transErr := thisItem.SendTransactionRequest()

	if transErr != nil {
		fmt.Println(transErr)
		return
	}

	// TODO: cleanup uploads on success
}

func doGet(item string, files string) {
	fmt.Printf("Item = %s\n", item)
}

func doHistory(item string) {

	var json *jason.Object
        var jsonFetchErr error

        thisItem := bserver.New(*server, item, *fileroot)

	// Fetch Item Info from bserver
	//thisItem.FetchItemInfo()
	json, jsonFetchErr = thisItem.GetItemInfo()

	switch {
        	case jsonFetchErr  == bserver.ErrNotFound:
			fmt.Printf("\n Item %s was not found on server %\n", item, *server)
        	case jsonFetchErr != nil:
                	fmt.Println(jsonFetchErr)
        	default:
			fmt.Println(json.String())
        }

}

func doList(item string) {
	fmt.Printf("Item = %s\n", item)
}
