package server

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/julienschmidt/httprouter"

	"github.com/ndlib/bendo/fragment"
)

func NewTxHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	id := ps.ByName("id")
	tx, _ := TxStore.Create(id)
	w.Header().Set("Location", "/transaction/"+tx.ID)
}

func ListTxHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	enc := json.NewEncoder(w)
	enc.Encode(TxStore.List())
}

func TxInfoHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	id := ps.ByName("tid")
	tx := TxStore.Lookup(id)
	if tx == nil {
		fmt.Fprintln(w, "cannot find transaction")
		return
	}
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
	var f *fragment.File
	if bid == "" {
		var nohash []byte
		f = tx.NewFile(nohash, nohash)
	} else {
		f = TxStore.Files.Lookup(bid)
		if f == nil {
			fmt.Fprintln(w, "Bad file id")
			return
		}
	}
	if r.Body == nil {
		fmt.Fprintln(w, "No Body")
		return
	}
	wr, err := f.Append()
	if err != nil {
		fmt.Fprintln(w, err.Error())
		return
	}
	io.Copy(wr, r.Body)
	err = wr.Close()
	w.Header().Set("Location", "/transaction/"+tx.ID+"/blob/"+f.ID)
	if err != nil {
		fmt.Fprintln(w, err.Error())
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
