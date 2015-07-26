package main

import (
	"flag"
	"fmt"
	"net/http"

	"github.com/ndlib/bendo/items"
	"github.com/ndlib/bendo/server"
	"github.com/ndlib/bendo/store"
)

func main() {
	var storeDir = flag.String("s", ".", "location of the storage directory")
	flag.Parse()

	fmt.Printf("Using storage dir %s\n", *storeDir)
	server.Items = items.New(store.NewFileSystem(*storeDir))
	http.ListenAndServe(":14000", server.AddRoutes())
}
