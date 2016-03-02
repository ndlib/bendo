package server

import (
	"encoding/json"
	"fmt"
	"html/template"
	"log"
	"net/http"
	_ "net/http/pprof" // for pprof server
	"os"
	"path/filepath"

	"github.com/facebookgo/httpdown"
	"github.com/julienschmidt/httprouter"

	"github.com/ndlib/bendo/blobcache"
	"github.com/ndlib/bendo/fragment"
	"github.com/ndlib/bendo/items"
	"github.com/ndlib/bendo/store"
	"github.com/ndlib/bendo/transaction"
	"github.com/ndlib/bendo/util"
)

// RESTServer holds the configuration for a Bendo REST API server.
//
// Set all the public fields and then call Run. Run will listen on the given
// port and handle requests. At the moment there is no maximum simultaneous
// request limit. Do not change any fields after calling Run.
//
// Run will also start a goroutine to handle serializing new file uploads
// into storage bags, and a goroutine to do fixity checking.
//
// There are two levels of configuration. It should be enough to only set
// CacheDir and Items. The other fields are exposed to allow more customization.
type RESTServer struct {
	// Port number to run bendo on. defaults to 14000
	PortNumber string
	PProfPort  string

	// Items is the base item store. Run will panic if Items is nil.
	Items *items.Store

	// CacheDir is the path to put the cache in the filesystem.
	// Used if Cache, FileStore, or TxStore are nil.
	// If CacheDir is empty then no caching is done, and any transactions
	// and uploads are kept entirely in memory.
	CacheDir  string
	CacheSize int64 // in bytes

	// Pass in a dial command to use a MySQL server as a database.
	// Otherwise a lightweight internal database is used, and placed inside
	// the CacheDir directory. The special value "memory" will run
	// the database entirely inside the server's memory. (useful for testing).
	// e.g. "user:password@tcp(localhost:5555)/dbname" or just "/dbname"
	// if everything else can be the default. Can also use domain sockets:
	// "user@unix(/path/to/socket)/dbname"
	MySQL string

	// --- The following fields are more advanced and only need to be
	// set in special situations. ---

	// Validator does authentication by validating any user tokens
	// presented to the API. If this is nil then no authentication will be
	// done.
	Validator TokenValidator

	// TxStore keeps information on transactions in progress. If this is
	// nil, transactions will be kept inside the cache directory.
	TxStore *transaction.Store

	// FileStore keeps the uploaded file waiting to be saved into the Item
	// store. If nil, the files will be stored inside the cache directory.
	FileStore *fragment.Store

	// Cache keeps smallish blobs retreived from tape.
	Cache blobcache.T

	// Fixity stores the records tracking past and future fixity checks.
	Fixity        FixityDB
	DisableFixity bool

	server httpdown.Server // used to close our listening socket
	txgate *util.Gate      // limits number of concurrent transactions
}

// the number of active commits onto tape we allow at a given time
const MaxConcurrentCommits = 2

// Run initializes and starts all the goroutines used by the server. It then
// blocks listening for and handling http requests.
func (s *RESTServer) Run() error {
	log.Println("==========")
	log.Printf("Starting Bendo Server version %s", Version)
	log.Printf("CacheDir = %s", s.CacheDir)
	log.Printf("CacheSize = %d", s.CacheSize)

	if s.Items == nil {
		panic("No base storage given. Items is nil.")
	}

	if s.Validator == nil {
		log.Println("No Validator given")
		s.Validator = NobodyValidator{}
	}

	// init database
	var db interface {
		FixityDB
		items.ItemCache
	}
	var err error
	if s.MySQL != "" {
		db, err = NewMysqlCache(s.MySQL)
	} else {
		var path string
		if s.CacheDir != "" {
			path = filepath.Join(s.CacheDir, "bendo.ql")
		} else {
			path = "memory"
		}
		db, err = NewQlCache(path)
	}
	if db == nil || err != nil {
		panic("problem setting up database")
	}
	s.Items.SetCache(db)

	// init fixity
	if !s.DisableFixity {
		if s.Fixity == nil {
			s.Fixity = db
		}
		s.StartFixity()
	}

	// init blobcache
	if s.Cache == nil {
		if s.CacheDir == "" || s.CacheSize == 0 {
			log.Println("Not using blob cache")
			s.Cache = blobcache.EmptyCache{}
		} else {
			path := filepath.Join(s.CacheDir, "blobcache")
			os.MkdirAll(path, 0755)
			fs := store.NewFileSystem(path)
			c := blobcache.NewLRU(fs, s.CacheSize)
			go c.Scan()
			s.Cache = c
		}
	}

	// init TxStore
	if s.TxStore == nil {
		var fs store.Store
		if s.CacheDir == "" {
			fs = store.NewMemory()
		} else {
			path := filepath.Join(s.CacheDir, "transaction")
			os.MkdirAll(path, 0755)
			fs = store.NewFileSystem(path)
		}
		s.TxStore = transaction.New(fs)
	}
	log.Println("Scanning Transactions")
	s.TxStore.Load()

	// init upload store
	if s.FileStore == nil {
		var fs store.Store
		if s.CacheDir == "" {
			fs = store.NewMemory()
		} else {
			path := filepath.Join(s.CacheDir, "upload")
			os.MkdirAll(path, 0755)
			fs = store.NewFileSystem(path)
		}
		s.FileStore = fragment.New(fs)
	}
	log.Println("Scanning Upload Queue")
	s.FileStore.Load()

	log.Println("Starting Transaction Cleaner")
	s.StartTxCleaner()

	log.Println("Starting pending transactions")
	s.txgate = util.NewGate(MaxConcurrentCommits)
	go s.initCommitQueue()

	// for pprof
	if s.PProfPort != "" {
		log.Println("Starting PProf on port", s.PProfPort)
		go func() {
			log.Println(http.ListenAndServe(":"+s.PProfPort, nil))
		}()
	}
	log.Println("Listening on", s.PortNumber)

	h := httpdown.HTTP{}
	s.server, err = h.ListenAndServe(&http.Server{
		Addr:    ":" + s.PortNumber,
		Handler: s.addRoutes(),
	})
	if err != nil {
		log.Println(err)
		return err
	}
	return s.server.Wait()
}

// Stop will stop the server and return when all the server goroutines have
// exited and the socked closed.
func (s *RESTServer) Stop() error {
	// first shutdown the transaction writing.
	// We don't stop the fixity process. Should we?
	s.txgate.Stop() // release and wait for any in progress transactions

	// then shutdown all the HTTP connections
	return s.server.Stop()
}

func (s *RESTServer) initCommitQueue() {
	// for each commit, pass to processCommit, and let it sort things out
	for _, tid := range s.TxStore.List() {
		tx := s.TxStore.Lookup(tid)
		if tx.Status == transaction.StatusWaiting ||
			tx.Status == transaction.StatusChecking ||
			tx.Status == transaction.StatusIngest {

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
		{"GET", "/item/:id/*slot", RoleUnknown, s.SlotHandler},
		{"HEAD", "/item/:id/*slot", RoleUnknown, s.SlotHandler},
		{"GET", "/item/:id", RoleUnknown, s.ItemHandler},

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

		// fixity reporting
		{"GET", "/fixity", RoleRead, nil},
		{"GET", "/fixity/errors", RoleRead, nil},
		{"GET", "/fixity/item/:itemid", RoleRead, nil},
		{"POST", "/fixity/:itemid", RoleWrite, nil},

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
			logWrapper(s.authzWrapper(route.handler, route.role)))
	}
	return r
}

// General route handlers and convinence functions

// NotImplementedHandler will return a 501 not implemented error.
func NotImplementedHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	w.WriteHeader(http.StatusNotImplemented)
	fmt.Fprintf(w, "Not Implemented\n")
}

// writeHTMLorJSON will either return val as JSON or as rendered using the
// given template, depending on the request header "Accept-Encoding".
func writeHTMLorJSON(w http.ResponseWriter,
	r *http.Request,
	tmpl *template.Template,
	val interface{}) {

	if r.Header.Get("Accept-Encoding") == "application/json" {
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
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
		ps = append(ps, httprouter.Param{Key: "username", Value: user})
	out:
		handler(w, r, ps)
	}
}

// logWrapper takes a handler and returns a handler which does the same thing,
// after first logging the request URL.
func logWrapper(handler httprouter.Handle) httprouter.Handle {
	return func(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
		log.Println(r.Method, r.URL)
		handler(w, r, ps)
	}
}
