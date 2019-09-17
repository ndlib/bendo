package transaction

import (
	"errors"
	"fmt"
	"log"
	"math/rand"
	"strconv"
	"sync"
	"time"

	"github.com/ndlib/bendo/blobcache"
	"github.com/ndlib/bendo/fragment"
	"github.com/ndlib/bendo/items"
	"github.com/ndlib/bendo/store"
)

// New creates a new transaction store using the given a store to save all the
// associated items.
// Make sure to call Load() on the returned structure to reload the metadata
// from the underlying store.
func New(s store.Store) *Store {
	return &Store{
		TxStore: fragment.NewJSON(s),
		txs:     make(map[string]*Transaction),
	}
}

// A Store tracks item transactions.
type Store struct {
	TxStore fragment.JSONStore
	m       sync.RWMutex            // protects txs
	txs     map[string]*Transaction // cache of transaction ID to transaction
}

// Load reads the underlying store and caches an inventory into memory.
func (r *Store) Load() error {
	r.m.Lock()
	defer r.m.Unlock()
	for key := range r.TxStore.List() {
		tx := new(Transaction)
		err := r.TxStore.Open(key, tx)
		if err != nil {
			log.Printf("TxLoad %s: %s\n", key, err.Error())
			continue
		}
		tx.txstore = &r.TxStore
		r.txs[tx.ID] = tx
	}
	return nil
}

// List returns an array containing the ids of all the stored transactions.
func (r *Store) List() []string {
	r.m.RLock()
	defer r.m.RUnlock()
	result := make([]string, 0, len(r.txs))
	for k := range r.txs {
		result = append(result, k)
	}
	return result
}

var (
	// ErrExistingTransaction occurs when trying to start a transaction on
	// an item already having a pending transaction.
	ErrExistingTransaction = errors.New("existing transaction for that item")

	// ErrBadCommand means a bad command was passed to the ingest routine.
	ErrBadCommand = errors.New("Bad command")
)

// Create a new transaction to update itemid. There can be at most one
// transaction per itemid.
func (r *Store) Create(itemid string) (*Transaction, error) {
	r.m.Lock()
	defer r.m.Unlock()
	// is there currently a open transaction for the item?
	for _, tx := range r.txs {
		tx.M.RLock()
		var inprocess = tx.ItemID == itemid &&
			tx.Status != StatusFinished &&
			tx.Status != StatusError
		tx.M.RUnlock()
		if inprocess {
			return nil, ErrExistingTransaction
		}
	}
	tx := &Transaction{
		ID:       r.makenewid(),
		Status:   StatusOpen,
		Started:  time.Now(),
		Modified: time.Now(),
		ItemID:   itemid,
		txstore:  &r.TxStore,
		BlobMap:  make(map[string]int),
	}
	r.txs[tx.ID] = tx
	tx.save()
	return tx, nil
}

// generate a new transaction id. Assumes caller holds r.m lock (either R or W)
func (r *Store) makenewid() string {
	for {
		id := randomid()
		// see if already being used
		if _, ok := r.txs[id]; !ok {
			return id
		}
	}
}

func randomid() string {
	var n = rand.Int31()
	return strconv.FormatInt(int64(n), 36)
}

// Lookup the given transaction identifier and return a pointer to the
// transaction. Returns nil if there is no transaction with that id.
func (r *Store) Lookup(txid string) *Transaction {
	r.m.RLock()
	defer r.m.RUnlock()
	return r.txs[txid]
}

// Delete a transaction
func (r *Store) Delete(id string) error {
	r.m.Lock()
	tx := r.txs[id]
	delete(r.txs, id)
	r.m.Unlock()

	if tx == nil {
		return nil
	}

	// don't need the lock for the following
	err := r.TxStore.Delete(tx.ID)
	return err
}

// Transaction Represents a single transaction.
type Transaction struct {
	txstore  *fragment.JSONStore // where this structure is stored
	files    *fragment.Store     // Where files are stored
	M        sync.RWMutex        // protects everything below
	ID       string              // the id of this transaction
	Status   Status              // one of Status*
	Started  time.Time           // time tx was created
	Modified time.Time           // last time user touch or added a file
	Err      []string            // list of errors (for StatusError)
	Creator  string              // username of the committer
	ItemID   string              // ID of the item this tx is modifying
	Commands []command           // commands to run on commit
	BlobMap  map[string]int      // tracks the blob id we used for uploaded files
}

// The Status of a transaction.
type Status int

// The possible status states for the processing of a transaction.
const (
	StatusUnknown  Status = iota // zero status value
	StatusOpen                   // transaction is being modified by user
	StatusWaiting                // transaction has been submitted to be committed
	StatusChecking               // files are being checksummed and verified
	StatusIngest                 // files are being written into bundles
	StatusFinished               // transaction is over, successful
	StatusError                  // transaction had an error
)

//go:generate stringer -type=Status

// AddCommandList changes the command list to process when committing this
// transaction to the one given.
func (tx *Transaction) AddCommandList(cmds [][]string) error {
	// first make sure commands are okay
	for _, cmd := range cmds {
		c := command(cmd)
		if !c.WellFormed() {
			return ErrBadCommand
		}
	}
	tx.M.Lock()
	defer tx.M.Unlock()
	for _, cmd := range cmds {
		tx.Commands = append(tx.Commands, command(cmd))
	}
	tx.save()
	return nil
}

// SetStatus updates the status of this transaction to s.
func (tx *Transaction) SetStatus(s Status) {
	tx.M.Lock()
	defer tx.M.Unlock()
	tx.Status = s
	tx.save()
}

// Commit this transaction to the given store, creating or updating the
// underlying item.
// Commit a creation/update of an item in s, possibly using files
// in files, and with the given creator name.
func (tx *Transaction) Commit(s items.Store, files *fragment.Store, cache blobcache.T) {
	// we hold the lock on tx for the duration of the commit.
	// That might be for a very long time.
	tx.M.Lock()
	defer tx.M.Unlock()
	tx.Status = StatusIngest
	iw, err := s.Open(tx.ItemID, tx.Creator)
	if err != nil {
		tx.Err = append(tx.Err, err.Error())
		return
	}
	tx.files = files
	// execute commands. Errors will be appended to tx.Err
	for _, cmd := range tx.Commands {
		cmd.Execute(iw, tx, cache)
	}
	err = iw.Close()
	if err != nil {
		tx.Err = append(tx.Err, err.Error())
	}
	tx.Status = StatusFinished
	if len(tx.Err) > 0 {
		tx.Status = StatusError
	}
	tx.save()
	// to do: delete files after successful upload
}

// ReferencedFiles returns a list of all the upload file ids associated with
// this transaction. That is, all the files referenced by an "add" command.
func (tx *Transaction) ReferencedFiles() []string {
	tx.M.RLock()
	defer tx.M.RUnlock()
	var result []string
	for _, cmd := range tx.Commands {
		if cmd[0] == "add" && len(cmd) == 2 {
			result = append(result, cmd[1])
		}
	}
	return result
}

// VerifyFiles verifies the checksums of all the files being added by this
// transaction.
// Pass in the fragment store containing the uploaded files. Any negative
// results are returned in tx.Err.
func (tx *Transaction) VerifyFiles(files *fragment.Store) {
	for _, fid := range tx.ReferencedFiles() {
		f := files.Lookup(fid)
		if f == nil {
			tx.AppendError("Missing file " + fid)
			continue
		}
		ok, err := f.Verify()
		if err != nil {
			tx.AppendError("Checking " + fid + ": " + err.Error())
		} else if !ok {
			tx.AppendError("Checksum mismatch for " + fid)
		}
	}
}

// AppendError appends the given error string to this transaction.
// It will acquire the write lock on tx.
func (tx *Transaction) AppendError(e string) {
	tx.M.Lock()
	tx.Err = append(tx.Err, e)
	tx.M.Unlock()
}

// must hold lock tx.M to call this
func (tx *Transaction) save() {
	if tx.txstore != nil {
		tx.Modified = time.Now()
		tx.txstore.Save(tx.ID, tx)
	}
}

// [
//   ["delete", 56],
//   ["slot", "/asdf/45", 4],
//   ["note", "blah blah"]
//   ["add", "vh567"]
//   ["sleep"]
// ]
type command []string

// Execute this command on the given item writer and transaction.
// Assumes the write mutex on tx is held on entry. Execute will
// give up and then reacquire the write mutex on tx during lengthy processing steps.
func (c command) Execute(iw *items.Writer, tx *Transaction, cache blobcache.T) {
	if !c.WellFormed() {
		tx.Err = append(tx.Err, "Command is not well formed")
		return
	}
	cmd := []string(c)
	switch cmd[0] {
	case "delete":
		// delete <blob id>, both in store and in cache
		id, err := strconv.Atoi(cmd[1])
		if err == nil {
			// key in blobcache is itemID+blobid
			cacheKey := fmt.Sprintf("%s+%04d", tx.ItemID, id)
			cache.Delete(cacheKey)
			iw.DeleteBlob(items.BlobID(id))
		}
	case "slot":
		// slot <label> <blob id/file id>
		// if the id resolves to a blob we have added
		// to the item, use that, otherwise try to interpret
		// it as a blob id.
		id, ok := tx.BlobMap[cmd[2]]
		if !ok {
			// is it a blob id?
			var err error
			id, err = strconv.Atoi(cmd[2])
			if err != nil {
				tx.Err = append(tx.Err, "Cannot resolve id "+cmd[2])
				break
			}
		}
		iw.SetSlot(cmd[1], items.BlobID(id))
	case "note":
		// note <text>
		iw.SetNote(cmd[1])
	case "add":
		// add <file id>
		f := tx.files.Lookup(cmd[1])
		if f == nil {
			tx.Err = append(tx.Err, "Cannot find "+cmd[1])
			break
		}
		tx.M.Unlock()
		reader := f.Open()
		fstat := f.Stat()
		bid, err := iw.WriteBlob(reader, fstat.Size, fstat.MD5, fstat.SHA256)
		reader.Close()
		tx.M.Lock()
		if err != nil {
			tx.Err = append(tx.Err, err.Error())
			break
		}
		tx.BlobMap[cmd[1]] = int(bid)
		iw.SetMimeType(bid, fstat.MimeType)
	case "mimetype":
		// mimetype <blob id> <new mime type>
		bid, err := strconv.ParseInt(cmd[1], 10, 64)
		if err != nil {
			tx.Err = append(tx.Err, "Cannot resolve id "+cmd[2])
			break
		}
		iw.SetMimeType(items.BlobID(bid), cmd[2])
	case "sleep":
		// sleep for some length of time. intended to be used for testing.
		// nothing magic about 1 sec. could be less
		tx.M.Unlock()
		time.Sleep(1 * time.Second)
		tx.M.Lock()
	default:
		tx.Err = append(tx.Err, "Bad command "+cmd[0])
	}
}

// WellFormed checks this command for well-formed-ness. It returns true if
// the command is well formed, false otherwise.
// Wellformedness is a weaker condition than being semantically meaningful.
// For example, the slot command may refer to a blob which doesn't exist,
// in which case the command is wellformed but semantically invalid.
// WellFormed() does not attempt to figure out semantic validity.
func (c command) WellFormed() bool {
	cmd := []string(c)
	if len(cmd) == 0 {
		return false
	}
	switch {
	case cmd[0] == "delete" && len(cmd) == 2:
		_, err := strconv.Atoi(cmd[1])
		if err == nil {
			return true
		}
	case cmd[0] == "slot" && len(cmd) == 3:
		return true
	case cmd[0] == "note" && len(cmd) == 2:
		return true
	case cmd[0] == "add" && len(cmd) == 2:
		return true
	case cmd[0] == "sleep" && len(cmd) == 1:
		return true
	case cmd[0] == "mimetype" && len(cmd) == 3:
		return true
	}
	return false
}
