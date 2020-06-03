package main

// The bclient tool is meant to be invoked by the CurateND batch ingest process

import (
	"flag"
	"fmt"
	"log"
	"os"
	"path"
	"runtime"
	"runtime/pprof"
	"sync"

	"github.com/antonholmquist/jason"
	"github.com/ndlib/bendo/bclientapi"
)

// various command line flags, with default values

var (
	fileroot     = flag.String("root", ".", "root prefix to upload files")
	server       = flag.String("server", "http://localhost:14000", "Bendo Server to Use")
	creator      = flag.String("bclient", "butil", "Creator name to use")
	token        = flag.String("token", "", "API authentication token")
	longV        = flag.Bool("longV", false, "Print  Long Version")
	blobs        = flag.Bool("blobs", false, "Show Blobs Instead of Files")
	verbose      = flag.Bool("v", false, "Display more information")
	version      = flag.Int("version", 0, "version number")
	chunksize    = flag.Int("chunksize", 10, "chunk size of uploads (in meagabytes)")
	stub         = flag.Bool("stub", false, "Get Item Information, construct stub number")
	numuploaders = flag.Int("ul", 2, "Number Uploaders")
	wait         = flag.Bool("wait", true, "Wait for Upload Transaction to complte before exiting")

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
    -server   (defaults to http://localhost:14000) server_name:port of bendo server
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
    -stub         (defaults to false)  retrieve file tree of item, create zero-length stub for each file

    
	`
)

var cpuprofile = flag.String("cpuprofile", "", "write cpu profile `file`")
var memprofile = flag.String("memprofile", "", "write memory profile to `file`")

// main program

func main() {

	// parse command line
	flag.Usage = func() { fmt.Println(Usage) }
	flag.Parse()
	args := flag.Args()

	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatal("could not create CPU profile: ", err)
		}
		if err := pprof.StartCPUProfile(f); err != nil {
			log.Fatal("could not start CPU profile: ", err)
		}
	}

	if len(args) == 0 {
		fmt.Println(Usage)
		os.Exit(1)
	}

	// convert chunksize from megabytes to bytes
	*chunksize *= 1 << 20

	var code int
	switch args[0] {
	case "upload":
		if len(args) != 3 {
			fmt.Println("Usage: bclient <flags>upload <item> <file>")
			os.Exit(1)
		}
		code = doUpload(args[1], args[2])
	case "ls":
		if len(args) != 2 {
			fmt.Println("Usage: bclient <flags> ls <item> ")
			os.Exit(1)
		}
		code = doLs(args[1])
	case "get":
		if *stub {
			code = doGetStub(args[1])
		} else {
			code = doGet(args[1], args[2:])
		}
	case "history":
		if len(args) != 2 {
			fmt.Println("Usage: bclient <flags> history <item> ")
			os.Exit(1)
		}
		code = doHistory(args[1])
	default:
		fmt.Println(Usage)
		os.Exit(1)
	}

	if *cpuprofile != "" {
		pprof.StopCPUProfile()
	}

	if *memprofile != "" {
		f, err := os.Create(*memprofile)
		if err != nil {
			log.Fatal("could not create memory profile: ", err)
		}
		runtime.GC() // get up-to-date statistics
		if err := pprof.WriteHeapProfile(f); err != nil {
			log.Fatal("could not write memory profile: ", err)
		}
		f.Close()
	}

	os.Exit(code)
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

	// set up communication to the bendo server, and init local and remote filelists

	thisItem := &bclientapi.Connection{
		HostURL:   *server,
		Item:      item,
		Fileroot:  *fileroot,
		ChunkSize: *chunksize,
		Wait:      *wait,
		Token:     *token,
	}
	fileLists := NewLists(*fileroot)

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

	errorChan := make(chan error, 1)

	//Spin off desire number of upload workers
	for cnt := int(0); cnt < *numuploaders; cnt++ {
		go func() {
			defer getFileDone.Done()
			err := thisItem.GetFiles(filesToGet, pathPrefix)
			if err != nil {
				// try to send error back without blocking
				select {
				case errorChan <- err:
				default:
				}
				return
			}
		}()
	}

	fileLists.QueueFiles(filesToGet)

	getFileDone.Wait()

	// If a file upload failed, return an error to main
	select {
	case <-errorChan:
		return 1
	default:
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

	thisItem := &bclientapi.Connection{
		HostURL:   *server,
		Item:      item,
		Fileroot:  *fileroot,
		ChunkSize: *chunksize,
		Wait:      *wait,
		Token:     *token,
	}

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
		MakeStubFromJSON(json, item, pathPrefix)
	}

	return 0
}

func doHistory(item string) int {

	var json *jason.Object
	var jsonFetchErr error

	thisItem := &bclientapi.Connection{
		HostURL:   *server,
		Item:      item,
		Fileroot:  *fileroot,
		ChunkSize: *chunksize,
		Wait:      *wait,
		Token:     *token,
	}

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
		PrintListFromJSON(json)
	}

	return 0

}

func doLs(item string) int {

	var json *jason.Object
	var jsonFetchErr error

	thisItem := &bclientapi.Connection{
		HostURL:   *server,
		Item:      item,
		Fileroot:  *fileroot,
		ChunkSize: *chunksize,
		Wait:      *wait,
		Token:     *token,
	}

	// Fetch Item Info from bclientapi
	json, jsonFetchErr = thisItem.GetItemInfo()

	switch {
	case jsonFetchErr == bclientapi.ErrNotFound:
		fmt.Printf("\n Item %s was not found on server %s\n", item, *server)
	case jsonFetchErr != nil:
		fmt.Println(jsonFetchErr)
		return 1
	default:
		PrintLsFromJSON(json, *version, *longV, *blobs, item)
	}

	return 0
}
