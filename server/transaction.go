package server

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"html/template"
	"io"
	"net/http"

	"github.com/julienschmidt/httprouter"

	"github.com/ndlib/bendo/fragment"
	"github.com/ndlib/bendo/util"
)

//r.Handle("POST", "/item/:id", NewTxHandler)
func NewTxHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	id := ps.ByName("id")
	tx, err := TxStore.Create(id)
	if err != nil {
		// the err is probably that there is already a transaction open
		// on the item
		w.WriteHeader(409)
		fmt.Fprintln(w, err.Error())
		return
	}
	w.Header().Set("Location", "/transaction/"+tx.ID)
}

func writeHTMLorJSON(w http.ResponseWriter,
	r *http.Request,
	tmpl *template.Template,
	val interface{}) {
	if r.Header.Get("Content-Type") == "application/json" {
		json.NewEncoder(w).Encode(val)
		return
	}
	tmpl.Execute(w, val)
}

//r.Handle("GET", "/transaction", ListTx)
func ListTxHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	writeHTMLorJSON(w, r, listTxTemplate, TxStore.List())
}

var (
	listTxTemplate = template.Must(template.New("listtx").Parse(`<html>
<h1>Transactions</h1>
<ol>
{{ range . }}
	<li><a href="/transaction/{{ . }}">{{ . }}</a></li>
{{ else }}
	<li>No Transactions</li>
{{ end }}
</ol>
</html>`))
)

//r.Handle("GET", "/transaction/:tid", ListTxInfo)
func TxInfoHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	id := ps.ByName("tid")
	tx := TxStore.Lookup(id)
	if tx == nil {
		w.WriteHeader(404)
		fmt.Fprintln(w, "cannot find transaction")
		return
	}
	writeHTMLorJSON(w, r, txInfoTemplate, tx)
}

var (
	txInfoTemplate = template.Must(template.New("txinfo").Parse(`<html>
	<h1>Transaction Info</h1>
	{{ $txid := .ID }}
	<dl>
	<dt>ID</dt><dd>{{ .ID }}</dd>
	<dt>For Item</dt><dd>{{ .ItemID }}</dd>
	<dt>Status</dt><dd>{{ .Status }}</dd>
	<dt>Started</dt><dd>{{ .Started }}</dd>
	<dt>Errors</dt><dd>{{ range .Err }}{{ . }}<br/>{{ end }}</dd>
	<dt>Commands</dt><dd>{{ range .Commands }}{{ . }}<br/>{{ end }}</dd>
	<dt>New Blobs</dt><dd>
		{{ range .NewBlobs }}
			<b>PID</b> <a href="/transaction/{{ $txid }}/blob/{{ .PID }}">{{ .PID }}</a>
			<b>md5</b> {{ .MD5 }}
			<b>sha256</b> {{ .SHA256 }}<br/>
		{{ end }}
		</dd>
	</dl>
	<form action="/transaction/{{ .ID }}/commit" method="post">
		<button type="submit">Commit</button>
	</form>
	<a href="/transaction">Back</a>
	</html>`))
)

//r.Handle("POST", "/transaction/:tid", AddBlobHandler)
//r.Handle("PUT", "/transaction/:tid/blob/:bid", AddBlobHandler)
func AddBlobHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	md5hash64 := r.Header.Get("X-Upload-Md5")
	sha256hash64 := r.Header.Get("X-Upload-Sha256")
	if md5hash64 == "" && sha256hash64 == "" {
		w.WriteHeader(400)
		fmt.Fprintf(w, "Need at least one of X-Upload-Md5 or X-Upload-Sha256")
		return
	}
	var md5bytes []byte
	var err error
	if md5hash64 != "" {
		md5bytes, err = hex.DecodeString(md5hash64)
		if err != nil {
			w.WriteHeader(400)
			fmt.Fprintf(w, "bad MD5 string")
			return
		}
	}
	tid := ps.ByName("tid")
	bid := ps.ByName("bid")
	tx := TxStore.Lookup(tid)
	if tx == nil {
		w.WriteHeader(404)
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
			w.WriteHeader(404)
			fmt.Fprintln(w, "bad file id")
			return
		}
	}
	if r.Body == nil {
		w.WriteHeader(400)
		fmt.Fprintln(w, "no body")
		return
	}
	wr, err := f.Append()
	if err != nil {
		w.WriteHeader(500)
		fmt.Fprintln(w, err.Error())
		return
	}
	hw := util.NewMD5Writer(wr)
	_, err = io.Copy(hw, r.Body)
	err2 := wr.Close()
	r.Body.Close()
	w.Header().Set("Location", "/transaction/"+tx.ID+"/blob/"+f.ID)
	if err != nil {
		w.WriteHeader(500)
		fmt.Fprintln(w, err.Error())
		return
	}
	if err2 != nil {
		w.WriteHeader(500)
		fmt.Fprintln(w, err2.Error())
		return
	}
	if len(md5bytes) > 0 {
		_, ok := hw.CheckMD5(md5bytes)
		if !ok {
			w.WriteHeader(412)
			fmt.Fprintln(w, "MD5 mismatch")
			f.Rollback()
			return
		}
	}
}

// {"DELETE", "/transaction/:tid/blob/:bid", DeleteBlobHandler},
// This deletes a blob which has been uploaded to a transactions, but not committed
// into an item yet. If an item has already been committed, then a "delete"
// command is needed instead. I know, it is confusing.
func DeleteBlobHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	tid := ps.ByName("tid")
	bid := ps.ByName("bid")
	tx := TxStore.Lookup(tid)
	if tx == nil {
		w.WriteHeader(404)
		fmt.Fprintln(w, "cannot find transaction")
		return
	}
	err := tx.DeleteFile(bid)
	if err != nil {
		w.WriteHeader(500)
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
		w.WriteHeader(404)
		fmt.Fprintln(w, "cannot find transaction")
		return
	}
	// TODO(dbrower): use a limit reader to 1MB(?) for this
	var cmds [][]string
	err := json.NewDecoder(r.Body).Decode(&cmds)
	if err != nil {
		w.WriteHeader(400)
		fmt.Fprintln(w, err.Error())
		return
	}
	err = tx.AddCommandList(cmds)
	if err != nil {
		w.WriteHeader(400)
		fmt.Fprintln(w, err.Error())
		return
	}
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
	f.OutputJSON(w)
}

//r.Handle("POST", "/transaction/:tid/commit", CommitTx)
func CommitTxHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	tid := ps.ByName("tid")
	tx := TxStore.Lookup(tid)
	if tx == nil {
		w.WriteHeader(404)
		fmt.Fprintln(w, "cannot find transaction")
		return
	}
	go func() {
		gate.Enter()
		defer gate.Leave()
		tx.Commit(*Items, "nobody")
		if len(tx.Err) == 0 {
			// TODO(dbrower): what to do with this error? log it?
			_ = TxStore.Delete(tid)
		}
	}()
	w.WriteHeader(202)
}

// the number of active commits onto tape we allow at a given time
const MaxConcurrentCommits = 10

var gate = util.NewGate(MaxConcurrentCommits)

//r.Handle("POST", "/transaction/:tid/cancel", CancelTx)
func CancelTxHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	tid := ps.ByName("tid")
	err := TxStore.Delete(tid)
	fmt.Fprintf(w, err.Error())
}
