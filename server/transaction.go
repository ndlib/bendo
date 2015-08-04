package server

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/julienschmidt/httprouter"

	"github.com/ndlib/bendo/fragment"
)

//r.Handle("POST", "/item/:id", NewTxHandler)
func NewTxHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	id := ps.ByName("id")
	tx, err := TxStore.Create(id)
	if err != nil {
		fmt.Fprintln(w, err.Error())
		return
	}
	w.Header().Set("Location", "/transaction/"+tx.ID)
}

//r.Handle("GET", "/transaction", ListTx)
func ListTxHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	enc := json.NewEncoder(w)
	enc.Encode(TxStore.List())
}

//r.Handle("GET", "/transaction/:tid", ListTxInfo)
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

//r.Handle("POST", "/transaction/:tid", AddBlobHandler)
//r.Handle("PUT", "/transaction/:tid/blob/:bid", AddBlobHandler)
func AddBlobHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	tid := ps.ByName("tid")
	bid := ps.ByName("bid")
	tx := TxStore.Lookup(tid)
	if tx == nil {
		fmt.Fprintln(w, "cannot find transaction")
		return
	}
	var f *fragment.File // the file to append to
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

//r.Handle("GET", "/transaction/:tid/commands", GetCommands)
func GetCommandsHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	tid := ps.ByName("tid")
	tx := TxStore.Lookup(tid)
	if tx == nil {
		fmt.Fprintln(w, "cannot find transaction")
		return
	}
	enc := json.NewEncoder(w)
	enc.Encode(tx.Commands)
}

//r.Handle("PUT", "/transaction/:tid/commands", AddCommands)
func AddCommandsHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	tid := ps.ByName("tid")
	tx := TxStore.Lookup(tid)
	if tx == nil {
		fmt.Fprintln(w, "cannot find transaction")
		return
	}
	// TODO(dbrower): use a limit reader to 1MB(?) for this
	var cmds [][]string
	dec := json.NewDecoder(r.Body)
	err := dec.Decode(&cmds)
	if err != nil {
		fmt.Fprintln(w, err.Error())
		return
	}
	//for _,cmd := range cmds {
	//	c :=
	//}
}

//r.Handle("GET", "/transaction/:tid/blob/:bid", ListBlobInfo)
func ListBlobInfoHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	tid := ps.ByName("tid")
	bid := ps.ByName("bid")
	tx := TxStore.Lookup(tid)
	if tx == nil {
		fmt.Fprintln(w, "cannot find transaction")
		return
	}
	f := TxStore.Files.Lookup(bid)
	if f == nil {
		fmt.Fprintln(w, "Bad file id")
		return
	}
	enc := json.NewEncoder(w)
	enc.Encode(f)
}

//r.Handle("POST", "/transaction/:tid/commit", CommitTx)
func CommitTxHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	tid := ps.ByName("tid")
	tx := TxStore.Lookup(tid)
	if tx == nil {
		fmt.Fprintln(w, "cannot find transaction")
		return
	}
	tx.Commit(*Items, "nobody")
	if len(tx.Err) == 0 {
		err := TxStore.Delete(tid)
		if err != nil {
			fmt.Fprintln(w, err.Error())
		}
	} else {
		for _, err := range tx.Err {
			fmt.Fprintln(w, err.Error())
		}
	}
}

//r.Handle("POST", "/transaction/:tid/cancel", CancelTx)
func CancelTxHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	tid := ps.ByName("tid")
	err := TxStore.Delete(tid)
	fmt.Fprintf(w, err.Error())
}
