package main

import (
	"flag"
	"fmt"
	"net/http"

	"github.com/ndlib/bendo/items"
	"github.com/ndlib/bendo/server"
)

func main() {
	var storeDir = flag.String("s", ".", "location of the storage directory")
	flag.Parse()

	fmt.Printf("Using storage dir %s\n", *storeDir)
	server.Items = items.New(items.NewFileStore(*storeDir))
	http.ListenAndServe(":14000", server.AddRoutes())
}
