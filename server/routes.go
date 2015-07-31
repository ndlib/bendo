package server

import (
	"fmt"
	"net/http"

	"github.com/julienschmidt/httprouter"

	"github.com/ndlib/bendo/items"
)

var (
	Items *items.Store
)

var (
	routes = []struct {
		method, route string
		handler       httprouter.Handle
	}{
		{"GET", "/blob/:id/:bid", BlobHandler},
		{"HEAD", "/blob/:id/:bid", BlobHandler},
		{"GET", "/item/:id/:version/*slot", SlotHandler},
		{"HEAD", "/item/:id/:version/*slot", SlotHandler},
		{"GET", "/item/:id", ItemHandler},
		// all the transaction things. Sooo many transaction things.
		{"POST", "/item/:id", NewTxHandler},
		{"GET", "/transaction", ListTxHandler},
		{"GET", "/transaction/:tid", TxInfoHandler},
		{"POST", "/transaction/:tid", AddBlobHandler},
		{"GET", "/transaction/:tid/commands", GetCommands},
		{"PUT", "/transaction/:tid/commands", AddCommands},
		{"GET", "/transaction/:tid/blob/:bid", ListBlobInfo},
		{"PUT", "/transaction/:tid/blob/:bid", AddBlobHandler},
		{"POST", "/transaction/:tid/commit", CommitTx},
		{"POST", "/transaction/:tid/cancel", CancelTx},
		// the read only bundle stuff
		{"GET", "/bundle/list/", BundleListHandler},
		{"GET", "/bundle/listprefix/:prefix", BundleListPrefixHandler},
		{"GET", "/bundle/open/:key", BundleOpenHandler},
		// other
		{"GET", "/", WelcomeHandler},
		{"GET", "/stats", NotImplementedHandler},
	}
)

func AddRoutes() http.Handler {
	r := httprouter.New()

	for _, route := range routes {
		r.Handle(route.method, route.route, route.handler)
	}
	return r
}

var (
	GetCommands  = NotImplementedHandler
	AddCommands  = NotImplementedHandler
	ListBlobInfo = NotImplementedHandler
	CommitTx     = NotImplementedHandler
	CancelTx     = NotImplementedHandler
)

func NotImplementedHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	w.WriteHeader(http.StatusNotImplemented)
	fmt.Fprintf(w, "Not Implemented\n")
}

func ItemPatchHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	fmt.Fprintf(w, "PATCH to item %s", ps.ByName("id"))
}
