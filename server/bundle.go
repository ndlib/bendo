package server

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/julienschmidt/httprouter"

	"github.com/ndlib/bendo/items"
	"github.com/ndlib/bendo/store"
)

// BundleListHandler handles GET requests to "/bundle/list".
func (s *RESTServer) BundleListHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {

	if !s.useTape {
		w.WriteHeader(503)
		fmt.Fprintln(w, items.ErrNoStore)
		return
	}

	c := s.Items.S.List()
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	// we encode this as JSON ourselves....how could it go wrong?
	w.Write([]byte("["))
	// comma starts as a space
	var comma = ' '
	for key := range c {
		fmt.Fprintf(w, `%c"%s"`, comma, key)
		comma = ','
	}
	w.Write([]byte("]"))
}

// BundleListPrefixHandler handles GET requests to "/bundle/list/:prefix".
func (s *RESTServer) BundleListPrefixHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	prefix := ps.ByName("prefix")

	if !s.useTape {
		w.WriteHeader(503)
		fmt.Fprintln(w, items.ErrNoStore)
		return
	}

	result, err := s.Items.S.ListPrefix(prefix)
	if err != nil {
		w.WriteHeader(500)
		fmt.Fprintln(w, err)
		return
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	enc := json.NewEncoder(w)
	enc.Encode(result) // ignore any error
}

// BundleOpenHandler handles GET requests to "/bundle/open/:key"
func (s *RESTServer) BundleOpenHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	key := ps.ByName("key")

	if !s.useTape {
		w.WriteHeader(503)
		fmt.Fprintln(w, items.ErrNoStore)
		return
	}

	data, _, err := s.Items.S.Open(key)
	if err != nil {
		// assume it is a missing key
		w.WriteHeader(404)
		fmt.Fprintln(w, err)
		return
	}
	defer data.Close()
	io.Copy(w, store.NewReader(data))
}
