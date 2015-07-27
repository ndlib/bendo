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

	// Bread and butter routes
	r.Handle("GET", "/blob/:id/:bid", BlobHandler)
	r.Handle("HEAD", "/blob/:id/:bid", BlobHandler)
	r.Handle("GET", "/item/:id/:version/:slot", SlotHandler)
	r.Handle("HEAD", "/item/:id/:version/:slot", SlotHandler)
	r.Handle("GET", "/item/:id", ItemHandler)

	// all the transaction things. Sooo many transaction things.
	r.Handle("POST", "/item/:id", NewTxHandler)
	r.Handle("GET", "/transaction", ListTx)
	r.Handle("GET", "/transaction/:tid", ListTxInfo)
	r.Handle("POST", "/transaction/:tid", AddBlobHandler)
	r.Handle("GET", "/transaction/:tid/commands", GetCommands)
	r.Handle("PUT", "/transaction/:tid/commands", AddCommands)
	r.Handle("GET", "/transaction/:tid/blob/:bid", ListBlobInfo)
	r.Handle("PUT", "/transaction/:tid/blob/:bid", AddBlobHandler)
	r.Handle("POST", "/transaction/:tid/commit", CommitTx)
	r.Handle("POST", "/transaction/:tid/cancel", CancelTx)

	// the read only bundle stuff
	r.Handle("GET", "/bundle/list/", BundleListHandler)
	r.Handle("GET", "/bundle/listprefix/:prefix", BundleListPrefixHandler)
	r.Handle("GET", "/bundle/open/:key", BundleOpenHandler)

	// other
	r.Handle("GET", "/", WelcomeHandler)
	r.Handle("GET", "/stats", NotImplementedHandler)

	return r
}

var (
	NewTxHandler   = NotImplementedHandler
	ListTx         = NotImplementedHandler
	ListTxInfo     = NotImplementedHandler
	GetCommands    = NotImplementedHandler
	AddCommands    = NotImplementedHandler
	ListBlobInfo   = NotImplementedHandler
	AddBlobHandler = NotImplementedHandler
	CommitTx       = NotImplementedHandler
	CancelTx       = NotImplementedHandler
	Bundle         = NotImplementedHandler
)

func NotImplementedHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	w.WriteHeader(http.StatusNotImplemented)
	fmt.Fprintf(w, "Not Implemented\n")
}

func BlobHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	fmt.Fprintf(w, "Blob %s/%s\n", ps.ByName("id"), ps.ByName("bid"))
	fmt.Fprintf(w, "Method: %s\n", r.Method)
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
