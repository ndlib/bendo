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
	// comma starts as a space
	var comma = ' '
	for key := range c {
		fmt.Fprintf(w, "%c\"%s\"", comma, key)
		comma = ','
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
