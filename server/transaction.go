package server

import (
	"encoding/json"
	"fmt"
	"html/template"
	"net/http"

	"github.com/julienschmidt/httprouter"

	"github.com/ndlib/bendo/transaction"
	"github.com/ndlib/bendo/util"
)

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
	<dt>Modified</dt><dd>{{ .Modified }}</dd>
	<dt>Errors</dt><dd>{{ range .Err }}{{ . }}<br/>{{ end }}</dd>
	<dt>Commands</dt><dd>{{ range .Commands }}{{ . }}<br/>{{ end }}</dd>
	</dl>
	<a href="/transaction">Back</a>
	</html>`))
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
	// TODO(dbrower): use a limit reader to 1MB(?) for this
	var cmds [][]string
	err = json.NewDecoder(r.Body).Decode(&cmds)
	if err != nil {
		tx.SetStatus(transaction.StatusError)
		w.WriteHeader(400)
		fmt.Fprintln(w, err.Error())
		return
	}
	err = tx.AddCommandList(cmds)
	if err != nil {
		tx.SetStatus(transaction.StatusError)
		w.WriteHeader(400)
		fmt.Fprintln(w, err.Error())
		return
	}
	tx.SetStatus(transaction.StatusWaiting)
	go processCommit(tx)
	w.WriteHeader(202)
}

func processCommit(tx *transaction.T) {
	gate.Enter()
	defer gate.Leave()
	tx.Commit(*Items, FileStore, "nobody")
}

// the number of active commits onto tape we allow at a given time
const MaxConcurrentCommits = 10

var gate = util.NewGate(MaxConcurrentCommits)

//r.Handle("POST", "/transaction/:tid/cancel", CancelTx)
func CancelTxHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	tid := ps.ByName("tid")
	// TODO(dbrower): only delete tx if it is modifiable
	//TODO(dbrower): how to remove waiting goroutine?
	err := TxStore.Delete(tid)
	fmt.Fprintf(w, err.Error())
}
