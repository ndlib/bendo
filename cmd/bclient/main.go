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
	goReturnMutex sync.Mutex
	goReturn      int = 0

	fileroot     = flag.String("root", ".", "root prefix to upload files")
	server       = flag.String("server", "libvirt9.library.nd.edu:14000", "Bendo Server to Use")
	creator      = flag.String("bclient", "butil", "Creator name to use")
	token      = flag.String("token", "", "API authentication token")
	longV        = flag.Bool("longV", false, "Print  Long Version")
	blobs        = flag.Bool("blobs", false, "Show Blobs Instead of Files")
	verbose      = flag.Bool("v", false, "Display more information")
	version      = flag.Int("version", 0, "version number")
	chunksize    = flag.Int("chunksize", 10, "chunk size of uploads (in meagabytes)")
	stub         = flag.Bool("stub", false, "Get Item Information, construct stub number")
	numuploaders = flag.Int("ul", 2, "Number Uploaders")
	wait         = flag.Bool("wait", false, "Wait for Upload Transaction to complte before exiting")

	Usage = `
Usage:

bclient [<flags>] <action> <bendo item number> [file]

Available actions:

    bclient [<flags>] get <item> [file]               get item's files. If none are specified, get them all
    bclient [<flags>] ls <item id> [file]             show details about item's files.
    bclient [<flags>] upload  <item id> <files>       upload a file or directory into an exiting item, or create a new one.
    bclient [<flags>] version <item id>               display item versioning information

    General Flags:

    -root (defaults to current directory)  location to get or put files
    -server   (defaults to bendo-staging.library.nd.edu:14000) server_name:port of bendo server
    -numuploaders (defaults to 2) number of concurrent upload/download threads
    -version ( defaults to latest version: ls & get actions) desired version number
    -token   ( no default ) API Authentication Token to be passed to the Bendo server

    upload Flags:

    -chunksize    ( defaults to 10) Size (in MB) of chunks bclient will use for upload  
    -creator      ( defaults to bclient) owner of upload in bendo
    -numuploaders ( defaults to 2) number of upload threads
    -v            ( defaults to false) Provide verbose upload information for troubleshooting
    -wait         ( defaults to true)  Wait for Upload Transaction to complte before exiting

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
		os.Exit(1)
	}

	switch args[0] {
	case "upload":
		if len(args) != 3 {
			fmt.Println("Usage: bclient <flags>upload <item> <file>")
			os.Exit(1)
		}
		os.Exit(doUpload(args[1], args[2]))
	case "ls":
		if len(args) != 2 {
			fmt.Println("Usage: bclient <flags> ls <item> ")
			os.Exit(1)
		}
		os.Exit(doLs(args[1]))
	case "get":
		if *stub {
			os.Exit(doGetStub(args[1]))
		}
		os.Exit(doGet(args[1], args[2:]))
	case "history":
		if len(args) != 2 {
			fmt.Println("Usage: bclient <flags> history <item> ")
			os.Exit(1)
		}
		os.Exit(doHistory(args[1]))
	default:
		fmt.Println(Usage)
		os.Exit(1)
	}

}

func doUpload(item string, files string) int {

	filesToSend := make(chan string)
	var upLoadDone sync.WaitGroup
	var sendFileDone sync.WaitGroup
	var json *jason.Object
	var jsonFetchErr error

	thisItem := bclientapi.New(*server, item, *fileroot, *chunksize, *wait, *token)
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

	// If GetItemInfo returns ErrNotFound it's a new item- upload whole local list
	// If GetItemInfo returns other error, bendo unvavailable for upload- abort!
	// default: build remote filelist of returned json, diff against local list, upload remainder

	switch {
	case jsonFetchErr == bclientapi.ErrNotFound:
		break
	case jsonFetchErr != nil:
		fmt.Println(jsonFetchErr)
		return 1
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
		return 0
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
			err := thisItem.SendFiles(filesToSend, fileLists)
			if err != nil {
				goReturnMutex.Lock()
				goReturn = 1
				goReturnMutex.Unlock()
			}
			sendFileDone.Done()
		}()
	}

	fileLists.QueueFiles(filesToSend)

	// wait for all file chunks to be uploaded
	sendFileDone.Wait()

	// If a file upload failed, return an error to main
	if goReturn == 1 {
		return 1
	}

	// chunks uploaded- submit trnsaction to add FileIDs to item
	transaction, transErr := thisItem.SendNewTransactionRequest()

	if transErr != nil {
		fmt.Println(transErr)
		return 1
	}

	if *verbose {
		fmt.Printf("\n Transaction id is %s\n", transaction)
	}

	if *wait {
		thisItem.WaitForCommitFinish(transaction)
	}

	return 0
}

//  doGet , given only an item, returns all the files in that item.
//  Given one or more files in the item, it returns only them

func doGet(item string, files []string) int {
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
		return 1
	}

	// set up communication to the bendo server, and init local and remote filelists

	thisItem := bclientapi.New(*server, item, *fileroot, *chunksize, *wait, *token)
	fileLists := fileutil.NewLists(*fileroot)

	// Fetch Item Info from bclientapi
	json, jsonFetchErr = thisItem.GetItemInfo()

	// If not found or error, we're done

	switch {
	case jsonFetchErr == bclientapi.ErrNotFound:
		fmt.Printf("\n Item %s was not found on server %s\n", item, *server)
		return 1
	case jsonFetchErr != nil:
		fmt.Println(jsonFetchErr)
		return 1
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
			err := thisItem.GetFiles(filesToGet, pathPrefix)
			if err != nil {
				goReturnMutex.Lock()
				goReturn = 1
				goReturnMutex.Unlock()
			}
			getFileDone.Done()
		}()
	}

	fileLists.QueueFiles(filesToGet)

	getFileDone.Wait()

	// If a file upload failed, return an error to main
	if goReturn == 1 {
		return 1
	}

	return 0
}

// doGetStub builds an empty skeleton of an item, with zero length files

func doGetStub(item string) int {
	var json *jason.Object
	var jsonFetchErr error

	// if file or dir exists in target path named after the item, give error mesg and exit
	pathPrefix := path.Join(*fileroot, item)

	_, err := os.Stat(pathPrefix)

	if err == nil {
		// file already exists
		fmt.Printf("Error: target %s already exists", pathPrefix)
		return 1
	}

	// fetch info about this item from the bendo server

	thisItem := bclientapi.New(*server, item, *fileroot, *chunksize, *wait, *token)

	// Fetch Item Info from bclientapi
	json, jsonFetchErr = thisItem.GetItemInfo()

	// If not found or error, we're done; otherwise, create Item Stub

	switch {
	case jsonFetchErr == bclientapi.ErrNotFound:
		fmt.Printf("\n Item %s was not found on server %s\n", item, *server)
	case jsonFetchErr != nil:
		fmt.Println(jsonFetchErr)
		return 1
	default:
		fileutil.MakeStubFromJSON(json, item, pathPrefix)
	}

	return 0
}

func doHistory(item string) int {

	var json *jason.Object
	var jsonFetchErr error

	thisItem := bclientapi.New(*server, item, *fileroot, *chunksize, *wait, *token)

	// Fetch Item Info from bclientapi
	json, jsonFetchErr = thisItem.GetItemInfo()

	switch {
	case jsonFetchErr == bclientapi.ErrNotFound:
		fmt.Printf("\n Item %s was not found on server %s\n", item, *server)
		return 0
	case jsonFetchErr != nil:
		fmt.Println(jsonFetchErr)
		return 1
	default:
		fileutil.PrintListFromJSON(json)
	}

	return 0

}

func doLs(item string) int {

	var json *jason.Object
	var jsonFetchErr error

	thisItem := bclientapi.New(*server, item, *fileroot, *chunksize, *wait, *token)

	// Fetch Item Info from bclientapi
	json, jsonFetchErr = thisItem.GetItemInfo()

	switch {
	case jsonFetchErr == bclientapi.ErrNotFound:
		fmt.Printf("\n Item %s was not found on server %s\n", item, *server)
	case jsonFetchErr != nil:
		fmt.Println(jsonFetchErr)
		return 1
	default:
		fileutil.PrintLsFromJSON(json, *version, *longV, *blobs, item)
	}

	return 0
}
