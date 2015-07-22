package server

import (
	"fmt"
	"io"
	"net/http"
	"strconv"

	"github.com/julienschmidt/httprouter"

	"github.com/ndlib/bendo/items"
)

var (
	Items *items.Store
)

func AddRoutes() http.Handler {
	r := httprouter.New()
	r.Handle("GET", "/blob/:id/:bid", BlobHandler)
	r.Handle("GET", "/item/:id", ItemHandler)
	r.Handle("GET", "/item/:id/:version/:slot", SlotHandler)
	r.Handle("PATCH", "/item/:id", ItemPatchHandler)
	return r
}

func BlobHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	fmt.Fprintf(w, "Blob %s/%s\n", ps.ByName("id"), ps.ByName("bid"))
	n, err := strconv.ParseInt(ps.ByName("bid"), 10, 0)
	if err != nil {
		fmt.Fprintln(w, err)
		return
	}
	src, err := Items.Blob(ps.ByName("id"), items.BlobID(n))
	if err != nil {
		fmt.Fprintln(w, err)
		return
	}
	io.Copy(w, src)
}

func ItemHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	fmt.Fprintf(w, "Hello, item %s", ps.ByName("id"))
}

func ItemPatchHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	fmt.Fprintf(w, "PATCH to item %s", ps.ByName("id"))
}

func SlotHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	fmt.Fprintf(w, "Hello, item %s with version %s slot %s",
		ps.ByName("id"),
		ps.ByName("version"),
		ps.ByName("slot"))
}
