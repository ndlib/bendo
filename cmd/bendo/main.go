package main

import (
	"net/http"

	"github.com/ndlib/bendo/bendo"
	"github.com/ndlib/bendo/server"
)

func main() {
	server.Items = bendo.New(bendo.NewFileStore("."))
	http.ListenAndServe(":14000", server.AddRoutes())
}
