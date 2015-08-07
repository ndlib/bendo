package server

import (
	"fmt"
	"net/http"

	"github.com/julienschmidt/httprouter"

	"github.com/ndlib/bendo/items"
	"github.com/ndlib/bendo/transaction"
)

var (
	Items   *items.Store
	TxStore *transaction.Registry
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
		{"POST", "/item/:id/transaction", NewTxHandler},
		{"GET", "/transaction", ListTxHandler},
		{"GET", "/transaction/:tid", TxInfoHandler},
		{"POST", "/transaction/:tid", AddBlobHandler},
		{"GET", "/transaction/:tid/commands", GetCommandsHandler},
		{"PUT", "/transaction/:tid/commands", AddCommandsHandler},
		{"GET", "/transaction/:tid/blob/:bid", ListBlobInfoHandler},
		{"PUT", "/transaction/:tid/blob/:bid", AddBlobHandler},
		{"POST", "/transaction/:tid/commit", CommitTxHandler},
		{"POST", "/transaction/:tid/cancel", CancelTxHandler},
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

func NotImplementedHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	w.WriteHeader(http.StatusNotImplemented)
	fmt.Fprintf(w, "Not Implemented\n")
}

func ItemPatchHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	fmt.Fprintf(w, "PATCH to item %s", ps.ByName("id"))
}
