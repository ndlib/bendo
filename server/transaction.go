package server

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/julienschmidt/httprouter"

	"github.com/ndlib/bendo/fragment"
)

var (
	TxStore fragment.Store
)

func NewTxHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
}

func ListTxHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	enc := json.NewEncoder(w)
	enc.Encode(TxStore.List())
}

func TxInfoHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	id := ps.ByName("tid")
	tx := TxStore.Lookup(id)
	enc := json.NewEncoder(w)
	enc.Encode(tx)
}

func AddBlobHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	tid := ps.ByName("tid")
	bid := ps.ByName("bid")
	tx := TxStore.Lookup(tid)
	if tx == nil {
		fmt.Fprintln(w, "cannot find transaction")
		return
	}
	if bid == "" {
		// make a new blob
		//tx := TxStore.New("a")
	} else {
		// append to find existing one
	}
}

// all the transaction things. Sooo many transaction things.
//r.Handle("POST", "/item/:id", NewTxHandler)
//r.Handle("GET", "/transaction", ListTx)
//r.Handle("GET", "/transaction/:tid", ListTxInfo)
//r.Handle("POST", "/transaction/:tid", AddBlobHandler)
//r.Handle("GET", "/transaction/:tid/commands", GetCommands)
//r.Handle("PUT", "/transaction/:tid/commands", AddCommands)
//r.Handle("GET", "/transaction/:tid/blob/:bid", ListBlobInfo)
//r.Handle("PUT", "/transaction/:tid/blob/:bid", AddBlobHandler)
//r.Handle("POST", "/transaction/:tid/commit", CommitTx)
//r.Handle("POST", "/transaction/:tid/cancel", CancelTx)
