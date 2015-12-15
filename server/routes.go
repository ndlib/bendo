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

// RESTServer is the base type containing the configuration for a Bendo REST
// API server.
//
// Set all the public fields and then call Run. Run will listen on the given
// port and handle requests. (At the moment there is no maximum simultaneous
// request limit). Do not change any fields after calling Run.
//
// Run will start a goroutine to handle serializing new file uploads
// into storage bags, and a goroutine to do fixity checking.
type RESTServer struct {
	PortNumber string
	PProfPort  string
	Validator  TokenValidator
	Items      *items.Store
}

var (
	Items     *items.Store
	TxStore   *transaction.Store
	FileStore *fragment.Store
)

func (server *RESTServer) Run() {
	log.Println("==========")
	log.Println("Starting Bendo Server version", Version)

	openDatabase("memory")
	server.Validator = NewNobodyValidator()

	log.Println("Loading Transactions")
	TxStore.Load()
	log.Println("Loading Upload Queue")
	FileStore.Load()
	log.Println("Starting pending transactions")
	go initCommitQueue()

	// for pprof
	if server.PProfPort != "" {
		log.Println("Starting PProf on port", server.PProfPort)
		go func() {
			log.Println(http.ListenAndServe(":"+server.PProfPort, nil))
		}()
	}
	log.Println("Listening on", server.PortNumber)
	http.ListenAndServe(":"+server.PortNumber, server.addRoutes())
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

func (s *RESTServer) addRoutes() http.Handler {
	var routes = []struct {
		method  string
		route   string
		role    Role // RoleUnknown means no API key is needed to access
		handler httprouter.Handle
	}{
		{"GET", "/blob/:id/:bid", RoleRead, s.BlobHandler},
		{"HEAD", "/blob/:id/:bid", RoleRead, s.BlobHandler},
		{"GET", "/item/:id/*slot", RoleRead, s.SlotHandler},
		{"HEAD", "/item/:id/*slot", RoleRead, s.SlotHandler},
		{"GET", "/item/:id", RoleMDOnly, s.ItemHandler},

		// all the transaction things.
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
		{"GET", "/bundle/list/:prefix", RoleRead, s.BundleListPrefixHandler},
		{"GET", "/bundle/list/", RoleRead, s.BundleListHandler},
		{"GET", "/bundle/open/:key", RoleRead, s.BundleOpenHandler},

		// other
		{"GET", "/", RoleUnknown, WelcomeHandler},
		{"GET", "/stats", RoleUnknown, NotImplementedHandler},
	}

	r := httprouter.New()
	for _, route := range routes {
		r.Handle(route.method,
			route.route,
			s.authzWrapper(route.handler, route.role))
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

// authzWrapper returns a Handler which will first verify the user token as
// having at least the given Role. The user name is added as a parameter
// "username".
func (server *RESTServer) authzWrapper(handler httprouter.Handle, leastRole Role) httprouter.Handle {
	return func(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
		token := r.Header.Get("X-Api-Key")
		user, role, err := server.Validator.TokenValid(token)
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
		// add a new username if none found
		ps = append(ps, httprouter.Param{"username", user})
	out:
		handler(w, r, ps)
	}
}
