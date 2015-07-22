package main

import (
	"net/http"

	"github.com/ndlib/bendo/items"
	"github.com/ndlib/bendo/server"
)

func main() {
	server.Items = items.New(items.NewFileStore("."))
	http.ListenAndServe(":14000", server.AddRoutes())
}
