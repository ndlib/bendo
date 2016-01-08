package main

import (
	"flag"
	"log"
	"os"

	"github.com/ndlib/bendo/blobcache"
	"github.com/ndlib/bendo/fragment"
	"github.com/ndlib/bendo/items"
	"github.com/ndlib/bendo/server"
	"github.com/ndlib/bendo/store"
	"github.com/ndlib/bendo/transaction"
)

func main() {
	var (
		storeDir   = flag.String("storage", ".", "location of the storage directory")
		uploadDir  = flag.String("upload", "upload", "location of the upload directory")
		tokenfile  = flag.String("user-tokens", "", "file containing allowable user tokens")
		cacheDir   = flag.String("cache-dir", "", "directory to store the blob cache")
		cacheSize  = flag.Int64("cache-size", 10, "the maximum size of the cache in MB")
		portNumber = flag.String("port", "14000", "Port Number to Use")
		pProfPort  = flag.String("pfport", "14001", "PPROF Port Number to Use")
	)
	flag.Parse()

	log.Printf("Using storage dir %s\n", *storeDir)
	log.Printf("Using upload dir %s\n", *uploadDir)
	var validator server.TokenValidator
	if *tokenfile != "" {
		var err error
		log.Printf("Using user token file %s\n", *tokenfile)
		validator, err = server.NewListValidatorFile(*tokenfile)
		if err != nil {
			log.Println(err)
			return
		}
	} else {
		log.Printf("No user token file specified")
		validator = server.NewNobodyValidator()
	}
	var cache blobcache.T
	if *cacheDir == "" {
		log.Printf("No cache directory given")
		cache = blobcache.EmptyCache{}
	} else {
		log.Printf("Caching in %s", *cacheDir)
		log.Printf("Cache Size %d (MB)", *cacheSize)
		cache = blobcache.New(store.NewFileSystem(*cacheDir),
			(*cacheSize)*1000000)
		os.MkdirAll(*cacheDir, 0755)
		go cache.(*blobcache.StoreLRU).Scan()
	}
	os.MkdirAll(*uploadDir, 0755)
	var s = server.RESTServer{
		Items:      items.New(store.NewFileSystem(*storeDir)),
		TxStore:    transaction.New(store.NewFileSystem(*uploadDir)),
		FileStore:  fragment.New(store.NewFileSystem(*uploadDir)),
		Validator:  validator,
		PortNumber: *portNumber,
		PProfPort:  *pProfPort,
		Cache:      cache,
	}
	s.Run()
}
