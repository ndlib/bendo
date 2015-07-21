package main

import (
	"net/http"

	"github.com/ndlib/bendo/server"
)

func main() {
	http.ListenAndServe(":14000", server.AddRoutes())
}
