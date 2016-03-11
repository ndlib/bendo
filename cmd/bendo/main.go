package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/BurntSushi/toml"
	"github.com/ndlib/bendo/items"
	"github.com/ndlib/bendo/server"
	"github.com/ndlib/bendo/store"
)

//Config info needed for Bendo

type bendoConfig struct {
	StoreDir   string
	Tokenfile  string
	CacheDir   string
	CacheSize  int64
	PortNumber string
	PProfPort  string
	Mysql      string
	Cow        string
}

func main() {

	// Start with the Default values
	//config := bendoConfig{".", "", "", 100, "14000", "14001", "", ""}
	var config bendoConfig

	var configFile = flag.String("config-file", "", "Configuration File")

	flag.Parse()

	// If config file arg provided, try to open & decode it
	if *configFile != "" {
		if _, err := toml.DecodeFile(*configFile, &config); err != nil {
			log.Println(err)
			return
		}
		log.Printf("Using config file %s\n", *configFile)
	}

	log.Printf("Using storage dir %s\n", config.StoreDir)
	log.Printf("Using cache dir %s\n", config.CacheDir)
	var validator server.TokenValidator
	if config.Tokenfile != "" {
		var err error
		log.Printf("Using user token file %s\n", config.Tokenfile)
		validator, err = server.NewListValidatorFile(config.Tokenfile)
		if err != nil {
			log.Println(err)
			return
		}
	} else {
		log.Printf("No user token file specified")
		validator = server.NobodyValidator{}
	}
	if config.CacheDir != "" {
		os.MkdirAll(config.CacheDir, 0755)
	}
	var itemstore store.Store = store.NewFileSystem(config.StoreDir)
	if config.Cow != "" {
		itemstore = store.NewCOW(itemstore, config.Cow, "")
	}
	var s = server.RESTServer{
		Items:      items.New(itemstore),
		Validator:  validator,
		MySQL:      config.Mysql,
		CacheDir:   config.CacheDir,
		CacheSize:  config.CacheSize * 1000000,
		PortNumber: config.PortNumber,
		PProfPort:  config.PProfPort,
	}
	if config.Cow != "" {
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
