package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/ndlib/bendo/fragment"
	"github.com/ndlib/bendo/items"
	"github.com/ndlib/bendo/server"
	"github.com/ndlib/bendo/store"
	"github.com/ndlib/bendo/transaction"
)

func main() {
	var storeDir = flag.String("storage", ".", "location of the storage directory")
	var uploadDir = flag.String("upload", "upload", "location of the upload directory")
	var portNumber = flag.String("port","14000","Port Number to Use")
	var pProfPort = flag.String("port","14001","PPROF Port Number to Use")
	flag.Parse()

	fmt.Printf("Using storage dir %s\n", *storeDir)
	fmt.Printf("Using upload dir %s\n", *uploadDir)
	fmt.Printf("Using port number %s \n", portNumber )
	fmt.Printf("Using pprof port number %s \n", pProfPort )
	os.MkdirAll(*uploadDir, 0664)
	server.Items = items.New(store.NewFileSystem(*storeDir))
	server.TxStore = transaction.New(store.NewFileSystem(*uploadDir))
	server.FileStore = fragment.New(store.NewFileSystem(*uploadDir))
	server.PortNumber = portNumber
	server.PProfPort = pProfPort
	server.Run()
}
