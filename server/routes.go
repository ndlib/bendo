package server

import (
	"encoding/json"
	"expvar"
	"fmt"
	"html/template"
	"log"
	"net/http"
	_ "net/http/pprof" // for pprof server
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/facebookgo/httpdown"
	raven "github.com/getsentry/raven-go"
	"github.com/julienschmidt/httprouter"

	"github.com/ndlib/bendo/blobcache"
	"github.com/ndlib/bendo/fragment"
	"github.com/ndlib/bendo/items"
	"github.com/ndlib/bendo/store"
	"github.com/ndlib/bendo/transaction"
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
	// CacheDir may point to an s3 bucket using the notation "s3:/bucket/prefix"
	// or "s3://hostname:port/bucket/prefix"
	CacheDir     string
	CacheSize    int64         // in bytes
	CacheTimeout time.Duration // nonzero for time-based caching. Ignores cache size

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

	// BlobDB holds the item/version/slot/blob information in a structured way
	// so we can query it without needing to read and parse the JSON structures
	// describing items.
	BlobDB blobDB

	// Fixity stores the records tracking past and future fixity checks.
	FixityDatabase FixityDB
	DisableFixity  bool

	server   httpdown.Server // used to close our listening socket
	txqueue  chan string     // channel to feed background transaction workers. contains tx ids
	txwg     sync.WaitGroup  // for waiting for all background tx workers to exit
	txcancel chan struct{}   // Is closed to indicate tx workers should exit
	useTape  bool            // Is Bendo reading/writing from tape?
}

// the number of transaction commits to tape we allow at a given time. If there are more
// they will wait in a queue.
const MaxConcurrentCommits = 2

// getcachestore will use the value of s.CacheDir and return an approporate
// store (i.e. memory, filesystem, or S3).
func (s *RESTServer) getcachestore(name string) store.Store {
	if s.CacheDir == "" {
		return store.NewMemory()
	}
	u, _ := url.Parse(s.CacheDir)
	switch u.Scheme {
	case "", "file":
		path := filepath.Join(u.Path, name)
		os.MkdirAll(path, 0755)
		return store.NewFileSystem(path)
	case "s3":
		conf := &aws.Config{}
		if u.Host != "" {
			conf.Endpoint = aws.String(u.Host)
			conf.Region = aws.String("us-east-1")
			// disable SSL for local development
			if strings.Contains(u.Host, "localhost") {
				conf.DisableSSL = aws.Bool(true)
				conf.S3ForcePathStyle = aws.Bool(true)
			}
		}
		var bucket, prefix string
		if len(u.Path) > 0 {
			v := strings.SplitN(u.Path[1:], "/", 2)
			bucket = v[0]
			if len(v) > 1 {
				prefix = v[1]
			}
		}
		if bucket == "" {
			log.Println("Error parsing location, no bucket name", s.CacheDir)
			break
		}
		// make sure there is a / between prefix and name, and a trailing slash
		// if the result is not empty
		if len(prefix) > 0 && prefix[len(prefix)-1] != '/' {
			prefix += "/"
		}
		prefix += name
		if len(prefix) > 0 && prefix[len(prefix)-1] != '/' {
			prefix += "/"
		}
		return store.NewS3(bucket, prefix, session.New(conf))
	default:
		log.Println("Unknown cache location", s.CacheDir)
	}
	// there was some kind of error. Return a Memory store? or fail?
	return store.NewMemory()
}

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
		log.Printf("Using MySQL")
		db, err = NewMysqlCache(s.MySQL)
	} else {
		var path string
		// this gets wonky if the cacheDir is an s3: path. but it still works!
		// (it makes a file system directory named "s3:")
		if s.CacheDir != "" {
			os.MkdirAll(s.CacheDir, 0755)
			path = filepath.Join(s.CacheDir, "bendo.ql")
		} else {
			path = "memory"
		}
		log.Printf("Using internal database at %s", path)
		db, err = NewQlCache(path)
	}
	if db == nil || err != nil {
		panic("problem setting up database")
	}
	s.Items.SetCache(db)

	// init tapeuse
	s.EnableTapeUse()

	// init fixity
	if s.FixityDatabase == nil {
		s.FixityDatabase = db
	}
	if !s.DisableFixity {
		s.StartFixity()
	}

	// init blobcache
	if s.Cache == nil {
		if s.CacheDir == "" || s.CacheSize == 0 {
			log.Println("Not using blob cache")
			s.Cache = blobcache.EmptyCache{}
		} else {
			v := s.getcachestore("blobcache")
			if s.CacheTimeout != 0 {
				log.Println("Using time-based cache strategy")
				c := blobcache.NewTime(v, s.CacheTimeout)
				go c.Scan()
				s.Cache = c
			} else {
				log.Println("Using size-based cache strategy")
				c := blobcache.NewLRU(v, s.CacheSize)
				go c.Scan()
				s.Cache = c
			}
		}
	}

	// init TxStore
	if s.TxStore == nil {
		v := s.getcachestore("transaction")
		s.TxStore = transaction.New(v)
	}
	log.Println("Scanning Transactions")
	s.TxStore.Load()

	// init upload store
	if s.FileStore == nil {
		v := s.getcachestore("upload")
		s.FileStore = fragment.New(v)
	}
	log.Println("Scanning Upload Queue")
	s.FileStore.Load()

	log.Println("Starting Transaction Cleaner")
	go s.TxCleaner()

	log.Println("Starting pending transactions")
	s.txqueue = make(chan string, 100) // 100 is arbitrary. don't expect that many.
	s.txcancel = make(chan struct{})
	for i := 0; i < MaxConcurrentCommits; i++ {
		s.txwg.Add(1)
		go s.transactionWorker(s.txqueue)
	}
	go s.initCommitQueue() // run in background

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
		Handler: raven.Recoverer(s.addRoutes()),
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
	// first shutdown the transaction workers
	// We don't stop the fixity process. Should we?
	close(s.txcancel)
	s.txwg.Wait() // wait for all tx workers to exit

	// then shutdown all the HTTP connections
	return s.server.Stop()
}

// initCommitQueue adds all transactions in the tx store to the transaction queue.
// It may block until they are all loaded and processed.
func (s *RESTServer) initCommitQueue() {
	// throw all transactions, even finished and errored ones, into
	// the queue. The transaction workers will sort it out.
	for _, tid := range s.TxStore.List() {
		select {
		case s.txqueue <- tid:
		case <-s.txcancel:
			return
		}
	}
}

func (s *RESTServer) addRoutes() http.Handler {
	var routes = []struct {
		method  string
		route   string
		role    Role // RoleUnknown means no API key is needed to access
		handler httprouter.Handle
	}{
		// the /blob/* routes can be removed. they are functionally the
		// same as /item/@blob/*
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

		// fixity routes
		{"GET", "/fixity", RoleRead, s.GetFixityHandler},
		{"GET", "/fixity/:id", RoleRead, s.GetFixityIdHandler},
		{"POST", "/fixity/:item", RoleWrite, s.PostFixityHandler},
		{"PUT", "/fixity/:id", RoleWrite, s.PutFixityHandler},
		{"DELETE", "/fixity/:id", RoleWrite, s.DeleteFixityHandler},

		// /admin/tape_use (enable, disable, get status)
		{"GET", "/admin/use_tape", RoleUnknown, s.GetTapeUseHandler},
		{"PUT", "/admin/use_tape/:status", RoleAdmin, s.SetTapeUseHandler},

		// the read only bundle stuff
		{"GET", "/bundle/list/:prefix", RoleRead, s.BundleListPrefixHandler},
		{"GET", "/bundle/list/", RoleRead, s.BundleListHandler},
		{"GET", "/bundle/open/:key", RoleRead, s.BundleOpenHandler},

		// other
		{"GET", "/", RoleUnknown, WelcomeHandler},
		{"GET", "/stats", RoleUnknown, NotImplementedHandler},
		{"GET", "/debug/vars", RoleUnknown, VarHandler}, // standard route for expvars data
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

// VarHandler adapts the expvar default handler to the httprouter three parameter handler.
func VarHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	// this code is taken from the stdlib expvar package.
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	fmt.Fprintf(w, "{\n")
	first := true
	expvar.Do(func(kv expvar.KeyValue) {
		if !first {
			fmt.Fprintf(w, ",\n")
		}
		first = false
		fmt.Fprintf(w, "%q: %s", kv.Key, kv.Value)
	})
	fmt.Fprintf(w, "\n}\n")
}

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

	if r.Header.Get("Accept-Encoding") == "application/json" ||
		r.FormValue("format") == "json" {
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		json.NewEncoder(w).Encode(val)
		return
	}
	err := tmpl.Execute(w, val)
	if err != nil {
		log.Println(err)
		raven.CaptureError(err, nil)
	}
}

// authzWrapper returns a Handler which will first verify the user token as
// having at least the given Role. The user name is added as a parameter
// "username".
func (s *RESTServer) authzWrapper(handler httprouter.Handle, leastRole Role) httprouter.Handle {
	return func(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
		// the token may be passed in either the X-Api-Key header, or as the username
		// or the password in basic auth (to support human use)
		token := r.Header.Get("X-Api-Key")
		if token == "" {
			// token in username field?
			token, _, _ = r.BasicAuth()
		}
		if token == "" {
			// token in password field?
			_, token, _ = r.BasicAuth()
		}
		user, role, err := s.Validator.TokenValid(token)
		if err != nil {
			w.WriteHeader(500)
			fmt.Fprintln(w, err.Error())
			return
		}

		// is role valid?
		if role < leastRole {
			w.Header().Set("WWW-Authenticate", "Basic") // tell web browsers to display password box
			w.WriteHeader(401)
			fmt.Fprintln(w, "Forbidden")
			return
		}

		log.Println("User", user)

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
