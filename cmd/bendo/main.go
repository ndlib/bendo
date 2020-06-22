package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/ndlib/bendo/items"
	"github.com/ndlib/bendo/server"
	"github.com/ndlib/bendo/store"
)

//Config info needed for Bendo

type bendoConfig struct {
	StoreDir     string
	Tokenfile    string
	CacheDir     string
	CacheSize    int64
	CacheTimeout string
	PortNumber   string
	PProfPort    string
	Mysql        string
	CowHost      string
	CowToken     string
}

func main() {

	// Start with the Default values
	config := bendoConfig{
		StoreDir:     ".",
		Tokenfile:    "",
		CacheDir:     "",
		CacheSize:    100,
		CacheTimeout: "",
		PortNumber:   "14000",
		PProfPort:    "14001",
		Mysql:        "",
		CowHost:      "",
		CowToken:     "",
	}

	var configFile = flag.String("config-file", "", "Configuration File")

	flag.Parse()

	// If config file arg provided, try to open & decode it
	if *configFile != "" {
		log.Printf("Using config file %s\n", *configFile)
		if _, err := toml.DecodeFile(*configFile, &config); err != nil {
			log.Println(err)
			return
		}
	}

	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	log.Printf("Using storage dir %s\n", config.StoreDir)
	log.Printf("Using cache dir %s\n", config.CacheDir)
	log.Printf("Using cache timeout %s\n", config.CacheTimeout)
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
	var itemstore store.Store = store.NewFileSystem(config.StoreDir)
	if config.CowHost != "" {
		log.Printf("Using COW with target %s", config.CowHost)
		itemstore = store.NewCOW(itemstore, config.CowHost, config.CowToken)
	}
	timeout, _ := time.ParseDuration(config.CacheTimeout)
	var s = server.RESTServer{
		Items:        items.New(itemstore),
		Validator:    validator,
		MySQL:        config.Mysql,
		CacheDir:     config.CacheDir,
		CacheSize:    config.CacheSize * 1000000,
		CacheTimeout: timeout,
		PortNumber:   config.PortNumber,
		PProfPort:    config.PProfPort,
	}
	if config.CowHost != "" {
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
	log.Println("Exiting")
}

func signalHandler(sig <-chan os.Signal, svr *server.RESTServer) {
	for s := range sig {
		log.Println("---Received signal", s)
		switch s {
		case syscall.SIGINT, syscall.SIGTERM:
			svr.Stop() // this will cause Run to exit
		}
	}
}
