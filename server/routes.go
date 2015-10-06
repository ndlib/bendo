package server

import (
	"encoding/json"
	"fmt"
	"html/template"
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
	PortNumber string = ":14000"
	PProfPort string = ":14001"
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
	openDatabase("memory")

	log.Println("Loading Transactions")
	TxStore.Load()
	log.Println("Loading Upload Queue")
	FileStore.Load()
	log.Println("Starting pending transactions")
	go initCommitQueue()

	// for pprof
	go func() {
		log.Println(http.ListenAndServe("localhost" + PProfPort), nil))
	}()
	log.Println("Listening on ",PortNumber)
	http.ListenAndServe(PortNumber, AddRoutes())
}

func initCommitQueue() {
	// for each commit, pass to processCommit, and let it sort things out
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

func writeHTMLorJSON(w http.ResponseWriter,
	r *http.Request,
	tmpl *template.Template,
	val interface{}) {
	if r.Header.Get("Accept-Encoding") == "application/json" {
		json.NewEncoder(w).Encode(val)
		return
	}
	tmpl.Execute(w, val)
}
