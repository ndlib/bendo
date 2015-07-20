package http

import (
	"fmt"
	"net/http"

	"github.com/gorilla/mux"
)

func AddRoutes() http.Handler {
	r := mux.NewRouter()
	r.HandleFunc("/item/{id}/blob/{bid}", BlobHandler)
	return r
}

func BlobHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	fmt.Fprintf(w, "Hello, item %s with blob %s", vars["id"], vars["bid"])
}
