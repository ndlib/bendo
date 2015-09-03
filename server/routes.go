package server

import (
	"fmt"
	"log"
	"net/http"
	_ "net/http/pprof"

	"github.com/julienschmidt/httprouter"

	"github.com/ndlib/bendo/fragment"
	"github.com/ndlib/bendo/items"
	"github.com/ndlib/bendo/transaction"
)

var (
	Items     *items.Store
	TxStore   *transaction.Store
	FileStore *fragment.Store
)

var (
	routes = []struct {
		method  string
		route   string
		handler httprouter.Handle
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
		{"POST", "/transaction/:tid/cancel", CancelTxHandler}, //keep?
		// file upload things
		{"GET", "/upload", ListFileHandler},
		{"POST", "/upload", AppendFileHandler},
		{"GET", "/upload/:fileid", GetFileHandler},
		{"POST", "/upload/:fileid", AppendFileHandler},
		{"DELETE", "/upload/:fileid", DeleteFileHandler},
		{"GET", "/upload/:fileid/metadata", GetFileInfoHandler},
		{"PUT", "/upload/:fileid/metadata", SetFileInfoHandler},
		// the read only bundle stuff
		{"GET", "/bundle/list/", BundleListHandler},
		{"GET", "/bundle/listprefix/:prefix", BundleListPrefixHandler},
		{"GET", "/bundle/open/:key", BundleOpenHandler},
		// other
		{"GET", "/", WelcomeHandler},
		{"GET", "/stats", NotImplementedHandler},
	}
)

func Run() {
	TxStore.Load()
	FileStore.Load()
	initCommitQueue()

	// for pprof
	go func() {
		log.Println(http.ListenAndServe("localhost:14001", nil))
	}()
	http.ListenAndServe(":14000", AddRoutes())
}

func initCommitQueue() {
	// for each commit, if in StateWaiting or StateCommit, start a goroutine
	for _, tid := range TxStore.List() {
		tx := TxStore.Lookup(tid)
		if tx.Status == transaction.StatusWaiting || tx.Status == transaction.StatusIngest {
			tx.SetStatus(transaction.StatusWaiting)
			go processCommit(tx)
		}
	}
	// also! put username into tx record
}

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
