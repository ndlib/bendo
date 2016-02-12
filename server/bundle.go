package server

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/julienschmidt/httprouter"

	"github.com/ndlib/bendo/store"
)

// BundleListHandler handles GET requests to "/bundle/list".
func (s *RESTServer) BundleListHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	c := s.Items.S.List()
	// we encode this as JSON ourselves....how could it go wrong?
	w.Write([]byte("["))
	// comma starts as a space
	var comma = ' '
	for key := range c {
		fmt.Fprintf(w, "%c\"%s\"", comma, key)
		comma = ','
	}
	w.Write([]byte("]"))
}

// BundleListPrefixHandler handles GET requests to "/bundle/list/:prefix".
func (s *RESTServer) BundleListPrefixHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	prefix := ps.ByName("prefix")
	result, err := s.Items.S.ListPrefix(prefix)
	if err != nil {
		fmt.Fprintln(w, err)
		return
	}
	enc := json.NewEncoder(w)
	enc.Encode(result) // ignore any error
}

// BundleOpenHandler handles GET requests to "/bundle/open/:key"
func (s *RESTServer) BundleOpenHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	key := ps.ByName("key")
	data, _, err := s.Items.S.Open(key)
	if err != nil {
		fmt.Fprintln(w, err)
		return
	}
	io.Copy(w, store.NewReader(data))
	data.Close()
}
