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

	// Validator does authentication by validating any user tokens
	// presented to the API.
	Validator TokenValidator

	// Items is the base item store.
	Items *items.Store

	// TxStore keeps information on transactions in progress.
	TxStore *transaction.Store

	// FileStore keeps the uploaded file waiting to be saved into the
	// Item store.
	FileStore *fragment.Store
}

// Run initializes and starts all the goroutines used by the server. It then
// blocks listening for and handling http requests.
func (s *RESTServer) Run() {
	log.Println("==========")
	log.Println("Starting Bendo Server version", Version)

	if s.Validator == nil {
		panic("Validator is nil")
	}

	openDatabase("memory")

	log.Println("Loading Transactions")
	s.TxStore.Load()
	log.Println("Loading Upload Queue")
	s.FileStore.Load()
	log.Println("Starting pending transactions")
	go s.initCommitQueue()

	// for pprof
	if s.PProfPort != "" {
		log.Println("Starting PProf on port", s.PProfPort)
		go func() {
			log.Println(http.ListenAndServe(":"+s.PProfPort, nil))
		}()
	}
	log.Println("Listening on", s.PortNumber)
	http.ListenAndServe(":"+s.PortNumber, s.addRoutes())
}

func (s *RESTServer) initCommitQueue() {
	// for each commit, pass to processCommit, and let it sort things out
	for _, tid := range s.TxStore.List() {
		tx := s.TxStore.Lookup(tid)
		if tx.Status == transaction.StatusWaiting || tx.Status == transaction.StatusIngest {
			tx.SetStatus(transaction.StatusWaiting)
			go s.processCommit(tx)
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
		{"POST", "/item/:id/transaction", RoleWrite, s.NewTxHandler},
		{"GET", "/transaction", RoleRead, s.ListTxHandler},
		{"GET", "/transaction/:tid", RoleRead, s.TxInfoHandler},
		{"POST", "/transaction/:tid/cancel", RoleWrite, s.CancelTxHandler}, //keep?

		// file upload things
		{"GET", "/upload", RoleRead, s.ListFileHandler},
		{"POST", "/upload", RoleWrite, s.AppendFileHandler},
		{"GET", "/upload/:fileid", RoleRead, s.GetFileHandler},
		{"POST", "/upload/:fileid", RoleWrite, s.AppendFileHandler},
		{"DELETE", "/upload/:fileid", RoleWrite, s.DeleteFileHandler},
		{"GET", "/upload/:fileid/metadata", RoleMDOnly, s.GetFileInfoHandler},
		{"PUT", "/upload/:fileid/metadata", RoleWrite, s.SetFileInfoHandler},

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
func (s *RESTServer) authzWrapper(handler httprouter.Handle, leastRole Role) httprouter.Handle {
	return func(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
		token := r.Header.Get("X-Api-Key")
		user, role, err := s.Validator.TokenValid(token)
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
