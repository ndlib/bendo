package main

//The bclient tool is meant to be invoked by the CurateND batch ingest procees

import (
	"flag"
	"fmt"
	"github.com/antonholmquist/jason"
	"github.com/ndlib/bendo/bclientapi"
	"github.com/ndlib/bendo/fileutil"
	"os"
	"path"
	"sync"
)

// various command line flags, with default values

var (
	fileroot     = flag.String("root", ".", "root prefix to upload files")
	server       = flag.String("server", "libvirt9.library.nd.edu:14000", "Bendo Server to Use")
	creator      = flag.String("bclient", "butil", "Creator name to use")
	longV        = flag.Bool("longV", false, "Print  Long Version")
	blobs        = flag.Bool("blobs", false, "Show Blobs Instead of Files")
	verbose      = flag.Bool("v", false, "Display more information")
	version      = flag.Int("version", 0, "version number")
	chunksize    = flag.Int("chunksize", 10, "chunk size of uploads (in meagabytes)")
	stub         = flag.Bool("stub", false, "Get Item Information, construct stub number")
	numuploaders = flag.Int("ul", 2, "Number Uploaders")

        Usage  = `
Usage:

bclient [<flags>] <action> <bendo item number> [file]

Available actions:

    bclient [<flags>] get <item> [file]               get item's files. If none are specified, get them all
    bclient [<flags>] ls <item id> [file]             show details about item's files.
    bclient [<flags>] upload  <item id> <files>       upload a file or directory into an exiting item, or create a new one.
    bclient [<flags>] version <item id>               display item versioning information

    General Flags:

    -fileroot (defaults to current directory)  location to get or put files
    -server   (defaults to bendo-staging.library.nd.edu:14000) server_name:port of bendo server
    -numuploaders (defaults to 2) number of concurrent upload/download threads
    -version ( defaults to latest version: ls & get actions) desired version number

    upload Flags:

    -chunksize    ( defaults to 10) Size (in MB) of chunks bclient will use for upload  
    -creator      ( defaults to bclient) owner of upload in bendo
    -numuploaders ( defaults to 2) number of upload threads
    -v            ( defaults to false) Provide verbose upload information for troubleshooting

    ls Flags:	  

    -longV        ( defaults to false) show blob id, size, date created, and creator of each file in item 

    get Flags:
    -stub         (defaults to false)  retreive file tree of item, create zero-length stub for each file

    
	`

    )

// main program

func main() {

	// parse command line
	flag.Usage = func() { fmt.Println(Usage) }	
	flag.Parse()
	fileutil.SetVerbose(*verbose)

	args := flag.Args()

	if len(args) == 0 {
		fmt.Println(Usage)
		return
	}

	switch args[0] {
	case "upload":
		if len(args) != 3 {
			fmt.Println("Usage: bclient <flags>upload <item> <file>")
			return
		}
		doUpload(args[1], args[2])
	case "ls":
		if len(args) != 2 {
			fmt.Println("Usage: bclient <flags> ls <item> ")
			return
		}
		doLs(args[1])
	case "get":
		if *stub {
			doGetStub(args[1])
			return
		}
		doGet(args[1], args[2:])
	case "history":
		if len(args) != 2 {
			fmt.Println("Usage: bclient <flags> history <item> ")
			return
		}
		doHistory(args[1])
	default:
		fmt.Println(Usage)
	}

}

func doUpload(item string, files string) {

	filesToSend := make(chan string)
	var upLoadDone sync.WaitGroup
	var sendFileDone sync.WaitGroup
	var json *jason.Object
	var jsonFetchErr error

	thisItem := bclientapi.New(*server, item, *fileroot, *chunksize)
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

	if *verbose {
		fmt.Printf("\nLocal Files:\n")
		fileLists.PrintLocalList()
	}

	// If GetItemInfo returns ErrNotFound it's anew item- upload whole local list
	// If GetItemInfo returns other error, bendo unvavailable for upload- abort!
	// default: build remote filelist of returned json, diff against local list, upload remainder

	switch {
	case jsonFetchErr == bclientapi.ErrNotFound:
		break
	case jsonFetchErr != nil:
		fmt.Println(jsonFetchErr)
		return
	default:
		fileLists.BuildRemoteList(json)

		if *verbose {
			fmt.Printf("\nRemote Files:\n")
			fileLists.PrintRemoteList()
			fmt.Printf("\nBlobs:\n")
			fileLists.PrintBlobList()
			fmt.Printf("\n")
		}

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

	if *verbose {
		fmt.Printf("\nFiles to Upload:\n")
		fileLists.PrintLocalList()
	}

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
	transErr := thisItem.SendNewTransactionRequest()

	if transErr != nil {
		fmt.Println(transErr)
		return
	}

	// TODO: cleanup uploads on success
}

//  doGet , given only an item, returns all the files in that item.
//  Given one or more files in the item, it returns only them

func doGet(item string, files []string) {
	var json *jason.Object
	var jsonFetchErr error
	filesToGet := make(chan string)
	var getFileDone sync.WaitGroup

	// if file or dir exists in target path named after the item, give error mesg and exit
	pathPrefix := path.Join(*fileroot, item)

	_, err := os.Stat(pathPrefix)

	if err == nil {
		// file already exists
		fmt.Printf("Error: target %s already exists", pathPrefix)
		return
	}

	// set up communication to the bendo server, and init local and remote filelists

	thisItem := bclientapi.New(*server, item, *fileroot, *chunksize)
	fileLists := fileutil.NewLists(*fileroot)

	// Fetch Item Info from bclientapi
	json, jsonFetchErr = thisItem.GetItemInfo()

	// If not found or error, we're done

	switch {
	case jsonFetchErr == bclientapi.ErrNotFound:
		fmt.Printf("\n Item %s was not found on server %s\n", item, *server)
		return
	case jsonFetchErr != nil:
		fmt.Println(jsonFetchErr)
		return
	}

	// if item only, get all of the files; otherwise, only those asked for

	if len(files) == 0 {
		fileLists.BuildLocalList(json)
	} else {
		fileLists.BuildRemoteList(json)
		fileLists.BuildLocalFromFiles(files)
	}

	// At this point, the local list contains files, verified to exist on server

	// set up our barrier, that will wait for all the file chunks to be uploaded
	getFileDone.Add(*numuploaders)

	//Spin off desire number of upload workers
	for cnt := int(0); cnt < *numuploaders; cnt++ {
		go func() {
			thisItem.GetFiles(filesToGet, pathPrefix)
			getFileDone.Done()
		}()
	}

	fileLists.QueueFiles(filesToGet)

	// wait for all file chunks to be uploaded
	getFileDone.Wait()
}

// doGetStub builds an empty skeleton of an item, with zero length files

func doGetStub(item string) {
	var json *jason.Object
	var jsonFetchErr error

	// if file or dir exists in target path named after the item, give error mesg and exit
	pathPrefix := path.Join(*fileroot, item)

	_, err := os.Stat(pathPrefix)

	if err == nil {
		// file already exists
		fmt.Printf("Error: target %s already exists", pathPrefix)
		return
	}

	// fetch info about this item from the bendo server

	thisItem := bclientapi.New(*server, item, *fileroot, *chunksize)

	// Fetch Item Info from bclientapi
	json, jsonFetchErr = thisItem.GetItemInfo()

	// If not found or error, we're done; otherwise, create Item Stub

	switch {
	case jsonFetchErr == bclientapi.ErrNotFound:
		fmt.Printf("\n Item %s was not found on server %s\n", item, *server)
	case jsonFetchErr != nil:
		fmt.Println(jsonFetchErr)
	default:
		fileutil.MakeStubFromJSON(json, item, pathPrefix)
	}
}

func doHistory(item string) {

	var json *jason.Object
	var jsonFetchErr error

	thisItem := bclientapi.New(*server, item, *fileroot, *chunksize)

	// Fetch Item Info from bclientapi
	json, jsonFetchErr = thisItem.GetItemInfo()

	switch {
	case jsonFetchErr == bclientapi.ErrNotFound:
		fmt.Printf("\n Item %s was not found on server %s\n", item, *server)
	case jsonFetchErr != nil:
		fmt.Println(jsonFetchErr)
	default:
		fileutil.PrintListFromJSON(json)
	}

}

func doLs(item string) {

	var json *jason.Object
	var jsonFetchErr error

	thisItem := bclientapi.New(*server, item, *fileroot, *chunksize)

	// Fetch Item Info from bclientapi
	json, jsonFetchErr = thisItem.GetItemInfo()

	switch {
	case jsonFetchErr == bclientapi.ErrNotFound:
		fmt.Printf("\n Item %s was not found on server %s\n", item, *server)
	case jsonFetchErr != nil:
		fmt.Println(jsonFetchErr)
	default:
		fileutil.PrintLsFromJSON(json, *version, *longV, *blobs, item)
	}
}
