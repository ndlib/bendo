package transaction

import (
	"errors"
	"math/rand"
	"strconv"
	"sync"
	"time"

	"github.com/ndlib/bendo/fragment"
	"github.com/ndlib/bendo/items"
	"github.com/ndlib/bendo/store"
)

// Given a store, create a new registry.
// Use the Load() option to reload the metadata from the store.
func New(s store.Store) *Store {
	return &Store{
		TxStore: fragment.NewJSON(store.NewWithPrefix(s, "tx:")),
		txs:     make(map[string]*T),
	}
}

// Tracks item commit transactions.
type Store struct {
	TxStore fragment.JSONStore
	m       sync.RWMutex  // protects txs
	txs     map[string]*T // cache of transaction ID to transaction
}

func (r *Store) Load() error {
	r.m.Lock()
	defer r.m.Unlock()
	for key := range r.TxStore.List() {
		tx := new(T)
		err := r.TxStore.Open(key, tx)
		if err != nil {
			continue
		}
		tx.txstore = &r.TxStore
		r.txs[tx.ID] = tx
	}
	return nil
}

// Return a list of all the stored transactions.
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
	ErrExistingTransaction = errors.New("existing transaction for that item")
	ErrBadCommand          = errors.New("Bad command")
)

// Create a new transaction to update itemid. There can only be at most one
// transaction per itemid.
func (r *Store) Create(itemid string) (*T, error) {
	r.m.Lock()
	defer r.m.Unlock()
	// is there currently a open transaction for the item?
	for _, tx := range r.txs {
		tx.m.RLock()
		var inprocess = tx.ItemID == itemid &&
			tx.Status != StatusFinished &&
			tx.Status != StatusError
		tx.m.RUnlock()
		if inprocess {
			return nil, ErrExistingTransaction
		}
	}
	tx := &T{
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

func (r *Store) Lookup(txid string) *T {
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

// T Represents a single transaction.
type T struct {
	txstore  *fragment.JSONStore // where this structure is stored
	files    *fragment.Store     // Where files are stored
	m        sync.RWMutex        // protects everything below
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

// The status of a transaction.
type Status int

const (
	StatusUnknown  Status = iota
	StatusOpen            // transaction is being modified by user
	StatusChecking        // files are being checksummed and verified
	StatusWaiting         // transaction has been submitted to be committed
	StatusIngest          // files are being written into bundles
	StatusFinished        // transaction is over, successful
	StatusError           // transaction had an error
)

//go:generate stringer -type=status

func (s Status) MarshalJSON() ([]byte, error) {
	return []byte(`"` + s.String() + `"`), nil
}

func (tx *T) AddCommandList(cmds [][]string) error {
	// first make sure commands are okay
	for _, cmd := range cmds {
		c := command(cmd)
		if !c.WellFormed() {
			return ErrBadCommand
		}
	}
	tx.m.Lock()
	defer tx.m.Unlock()
	for _, cmd := range cmds {
		tx.Commands = append(tx.Commands, command(cmd))
	}
	tx.save()
	return nil
}

func (tx *T) SetStatus(s Status) {
	tx.m.Lock()
	defer tx.m.Unlock()
	tx.Status = s
	tx.save()
}

// Commit this transaction to the given store, creating or updating the
// underlying item.
// Commit a creation/update of an item in s, possibly using files
// in files, and with the given creator name.
func (tx *T) Commit(s items.Store, files *fragment.Store) {
	// we hold the lock on tx for the duration of the commit.
	// That might be for a very long time.
	tx.m.Lock()
	defer tx.m.Unlock()
	tx.Status = StatusIngest
	iw := s.Open(tx.ItemID)
	iw.SetCreator(tx.Creator)
	tx.files = files
	// execute commands. Errors will be appended to tx.Err
	for _, cmd := range tx.Commands {
		cmd.Execute(iw, tx)
	}
	err := iw.Close()
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
func (tx *T) ReferencedFiles() []string {
	tx.m.RLock()
	defer tx.m.RUnlock()
	var result []string
	for _, cmd := range tx.Commands {
		if cmd[0] == "add" && len(cmd) == 2 {
			result = append(result, cmd[1])
		}
	}
	return result
}

// Verify the checksums of the files to be added in this transaction.
// Pass in the fragment store containing the uploaded files. Any negative
// results are returned in tx.Err.
func (tx *T) VerifyFiles(files *fragment.Store) {
	for _, fid := range tx.ReferencedFiles() {
		f := files.Lookup(fid)
		if f == nil {
			tx.m.Lock()
			tx.Err = append(tx.Err, "Missing file "+fid)
			tx.m.Unlock()
			continue
		}
		if !f.Verify() {
			tx.m.Lock()
			tx.Err = append(tx.Err, "Checksum mismatch for "+fid)
			tx.m.Unlock()
		}
	}
}

// must hold lock tx.m to call this
func (tx *T) save() {
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

// Execute the a command using the given item writer and transaction as
// necessary. Assumes the write mutex on tx is held on entry. Execute will
// give up and then reaquire it after lengthy processing steps.
func (c command) Execute(iw *items.Writer, tx *T) {
	if !c.WellFormed() {
		tx.Err = append(tx.Err, "Command is not well formed")
		return
	}
	cmd := []string(c)
	switch cmd[0] {
	case "delete":
		// delete <blob id>
		id, err := strconv.Atoi(cmd[1])
		if err == nil {
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
		tx.m.Unlock()
		reader := f.Open()
		fstat := f.Stat()
		bid, err := iw.WriteBlob(reader, fstat.Size, fstat.MD5, fstat.SHA256)
		reader.Close()
		tx.m.Lock()
		if err != nil {
			tx.Err = append(tx.Err, err.Error())
			break
		}
		tx.BlobMap[cmd[1]] = int(bid)
	case "sleep":
		// sleep for some length of time. intended to be used for testing.
		// nothing magic about 1 sec. could be less
		tx.m.Unlock()
		time.Sleep(1 * time.Second)
		tx.m.Lock()
	default:
		tx.Err = append(tx.Err, "Bad command "+cmd[0])
	}
}

// Is the given command well formed? This is a weaker condition than
// being semantically meaningful. For example, the slot command may refer
// to a blob which doesn't exist. WellFormed() does not attempt to figure
// that out.
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
	}
	return false
}
