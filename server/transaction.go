package server

import (
	"encoding/json"
	"fmt"
	"html/template"
	"log"
	"net/http"
	"time"

	"github.com/julienschmidt/httprouter"

	"github.com/ndlib/bendo/transaction"
)

//r.Handle("GET", "/transaction", ListTx)
func (s *RESTServer) ListTxHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	writeHTMLorJSON(w, r, listTxTemplate, s.TxStore.List())
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
func (s *RESTServer) TxInfoHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	id := ps.ByName("tid")
	tx := s.TxStore.Lookup(id)
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
func (s *RESTServer) NewTxHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	id := ps.ByName("id")
	tx, err := s.TxStore.Create(id)
	if err != nil {
		// the err is probably that there is already a transaction open
		// on the item
		w.WriteHeader(409)
		fmt.Fprintln(w, err.Error())
		return
	}
	w.Header().Set("Location", "/transaction/"+tx.ID)
	tx.Creator = ps.ByName("username")
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
	go s.processCommit(tx)
	w.WriteHeader(202)
}

// The transaction's status must be set to StatusWaiting before entering,
// or nothing will happen to the transaction.
func (s *RESTServer) processCommit(tx *transaction.T) {
	// probably should hold lock on tx to access these fields directly
	log.Printf("Queued transaction %s on %s (%s)",
		tx.ID,
		tx.ItemID,
		tx.Status.String())
	if tx.Status == transaction.StatusOpen ||
		tx.Status == transaction.StatusFinished ||
		tx.Status == transaction.StatusError {
		return
	}

	ok := s.txgate.Enter()
	if !ok {
		// Gate was stopped
		return
	}
	defer s.txgate.Leave()

	log.Printf("Starting transaction %s on %s (%s)",
		tx.ID,
		tx.ItemID,
		tx.Status.String())
	start := time.Now()
	switch tx.Status {
	default:
		log.Printf("Unknown status %s", tx.Status.String())
	case transaction.StatusWaiting:
		tx.SetStatus(transaction.StatusChecking)
		fallthrough
	case transaction.StatusChecking:
		tx.VerifyFiles(s.FileStore)
		// check for len(tx.Err) > 0
		tx.SetStatus(transaction.StatusIngest)
		fallthrough
	case transaction.StatusIngest:
		tx.Commit(*s.Items, s.FileStore)
	}
	duration := time.Now().Sub(start)
	log.Printf("Finish transaction %s on %s (%s)", tx.ID, tx.ItemID, duration.String())
}

//r.Handle("POST", "/transaction/:tid/cancel", CancelTx)
func (s *RESTServer) CancelTxHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	tid := ps.ByName("tid")
	// TODO(dbrower): only delete tx if it is modifiable
	//TODO(dbrower): how to remove waiting goroutine?
	err := s.TxStore.Delete(tid)
	fmt.Fprintf(w, err.Error())
}
