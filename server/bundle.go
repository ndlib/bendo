package server

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/julienschmidt/httprouter"

	"github.com/ndlib/bendo/store"
)

func BundleListHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	c := Items.S.List()
	// we encode this as JSON ourselves....how could it go wrong?
	w.Write([]byte("["))
	var needcomma bool
	for key := range c {
		if needcomma {
			w.Write([]byte(","))
		}
		needcomma = true
		fmt.Fprintf(w, "\"%s\"", key)
	}
	w.Write([]byte("]"))
}

func BundleListPrefixHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	prefix := ps.ByName("prefix")
	result, err := Items.S.ListPrefix(prefix)
	if err != nil {
		fmt.Fprintln(w, err)
		return
	}
	enc := json.NewEncoder(w)
	enc.Encode(result) // ignore any error
}

func BundleOpenHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	key := ps.ByName("key")
	data, _, err := Items.S.Open(key)
	if err != nil {
		fmt.Fprintln(w, err)
		return
	}
	io.Copy(w, store.NewReader(data))
	data.Close()
}
