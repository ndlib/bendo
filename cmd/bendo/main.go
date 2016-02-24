package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/ndlib/bendo/items"
	"github.com/ndlib/bendo/server"
	"github.com/ndlib/bendo/store"
)

func main() {
	var (
		storeDir   = flag.String("storage", ".", "location of the storage directory")
		tokenfile  = flag.String("user-tokens", "", "file containing allowable user tokens")
		cacheDir   = flag.String("cache-dir", "", "directory to store the blob cache")
		cacheSize  = flag.Int64("cache-size", 100, "the maximum size of the cache in MB")
		portNumber = flag.String("port", "14000", "Port Number to Use")
		pProfPort  = flag.String("pfport", "14001", "PPROF Port Number to Use")
		mysql      = flag.String("mysql", "", "Connection information to use a MySQL database")
		cow        = flag.String("copy-on-write", "", "External bendo server to mirror content from")
	)
	flag.Parse()

	log.Printf("Using storage dir %s\n", *storeDir)
	log.Printf("Using cache dir %s\n", *cacheDir)
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
		validator = server.NobodyValidator{}
	}
	if *cacheDir != "" {
		os.MkdirAll(*cacheDir, 0755)
	}
	var itemstore store.Store = store.NewFileSystem(*storeDir)
	if *cow != "" {
		itemstore = store.NewCOW(itemstore, *cow, "")
	}
	var s = server.RESTServer{
		Items:      items.New(itemstore),
		Validator:  validator,
		MySQL:      *mysql,
		CacheDir:   *cacheDir,
		CacheSize:  *cacheSize * 1000000,
		PortNumber: *portNumber,
		PProfPort:  *pProfPort,
	}
	if *cow != "" {
		// don't run fixity if we are using a copy-on-write.
		// (doing so will cause us to download ALL the data from
		// the target bendo over time)
		s.DisableFixity = true
	}

	// set up signal handlers
	sig := make(chan os.Signal, 5)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	go signalHandler(sig, &s)

	s.Run()
}

func signalHandler(sig <-chan os.Signal, svr *server.RESTServer) {
	for s := range sig {
		log.Println("---Received signal", s)
		switch s {
		case syscall.SIGINT, syscall.SIGTERM:
			log.Println("Exiting")
			svr.Stop() // this will cause Run to exit
		}
	}
}
