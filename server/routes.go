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
	Items      *items.Store
	TxStore    *transaction.Store
	FileStore  *fragment.Store
	PortNumber *string
	PProfPort  *string
	Validator  TokenValidator
)

var (
	routes = []struct {
		method  string
		route   string
		role    Role
		handler httprouter.Handle
	}{
		{"GET", "/blob/:id/:bid", RoleRead, BlobHandler},
		{"HEAD", "/blob/:id/:bid", RoleRead, BlobHandler},
		{"GET", "/item/:id/:version/*slot", RoleRead, SlotHandler},
		{"HEAD", "/item/:id/:version/*slot", RoleRead, SlotHandler},
		{"GET", "/item/:id", RoleRead, ItemHandler},
		// all the transaction things. Sooo many transaction things.
		{"POST", "/item/:id/transaction", RoleWrite, NewTxHandler},
		{"GET", "/transaction", RoleRead, ListTxHandler},
		{"GET", "/transaction/:tid", RoleRead, TxInfoHandler},
		{"POST", "/transaction/:tid/cancel", RoleWrite, CancelTxHandler}, //keep?
		// file upload things
		{"GET", "/upload", RoleRead, ListFileHandler},
		{"POST", "/upload", RoleWrite, AppendFileHandler},
		{"GET", "/upload/:fileid", RoleRead, GetFileHandler},
		{"POST", "/upload/:fileid", RoleWrite, AppendFileHandler},
		{"DELETE", "/upload/:fileid", RoleWrite, DeleteFileHandler},
		{"GET", "/upload/:fileid/metadata", RoleMDOnly, GetFileInfoHandler},
		{"PUT", "/upload/:fileid/metadata", RoleWrite, SetFileInfoHandler},
		// the read only bundle stuff
		{"GET", "/bundle/list/", RoleRead, BundleListHandler},
		{"GET", "/bundle/listprefix/:prefix", RoleRead, BundleListPrefixHandler},
		{"GET", "/bundle/open/:key", RoleRead, BundleOpenHandler},
		// other
		{"GET", "/", RoleUnknown, WelcomeHandler},
		{"GET", "/stats", RoleUnknown, NotImplementedHandler},
	}
)

func Run() {
	openDatabase("memory")
	Validator = NewNobodyValidator()

	log.Println("Loading Transactions")
	TxStore.Load()
	log.Println("Loading Upload Queue")
	FileStore.Load()
	log.Println("Starting pending transactions")
	go initCommitQueue()

	// for pprof
	go func() {
		log.Println(http.ListenAndServe("localhost"+*PProfPort, nil))
	}()
	log.Println("Listening on ", PortNumber)
	http.ListenAndServe(*PortNumber, AddRoutes())
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
		r.Handle(route.method,
			route.route,
			checkTokenWrapper(route.handler, route.role))
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

// checkTokenWrapper returns a Handler which will first verify the user token
// as having at least the given Role. The user name is added as a parameter
// "username".
func checkTokenWrapper(handler httprouter.Handle, leastRole Role) httprouter.Handle {
	return func(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
		// user token exists? "X-Api-Key"

		token := r.Header.Get("X-Api-Key")
		user, role, err := Validator.TokenValid(token)
		if err != nil {
			w.WriteHeader(500)
			fmt.Fprintln(w, err.Error())
			return
		}

		// is role valid?
		if role < leastRole {
			w.WriteHeader(401)
			fmt.Fprintln(w, "Forbidden")
			return
		}

		// remove any previous username
		for i := range ps {
			if ps[i].Key == "username" {
				ps[i].Value = user
				goto out
			}
		}
		// add a new username
		ps = append(ps, httprouter.Param{"username", user})
	out:
		handler(w, r, ps)
	}
}
