package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/ndlib/bendo/items"
	"github.com/ndlib/bendo/server"
	"github.com/ndlib/bendo/store"
	"github.com/ndlib/bendo/transaction"
)

func main() {
	var storeDir = flag.String("storage", ".", "location of the storage directory")
	var uploadDir = flag.String("upload", "upload", "location of the upload directory")
	flag.Parse()

	fmt.Printf("Using storage dir %s\n", *storeDir)
	fmt.Printf("Using upload dir %s\n", *uploadDir)
	os.MkdirAll(*uploadDir, 0664)
	server.Items = items.New(store.NewFileSystem(*storeDir))
	server.TxStore = transaction.New(store.NewFileSystem(*uploadDir))
	server.Run()
}
