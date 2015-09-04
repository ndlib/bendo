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
	txs     map[string]*T // transaction ID to transaction
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
		if tx.ItemID == itemid &&
			tx.Status != StatusFinished &&
			tx.Status != StatusError {
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
	Err      []error             // list of errors (for StatusError)
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
func (tx *T) Commit(s items.Store, files *fragment.Store, Creator string) {
	tx.m.Lock()
	defer tx.m.Unlock()
	tx.Status = StatusIngest
	iw := s.Open(tx.ItemID)
	iw.SetCreator(Creator)
	tx.files = files
	// execute commands. Errors will be appended to tx.Err
	for _, cmd := range tx.Commands {
		cmd.Execute(iw, tx)
	}
	err := iw.Close()
	if err != nil {
		tx.Err = append(tx.Err, err)
	}
	tx.Status = StatusFinished
	if len(tx.Err) > 0 {
		tx.Status = StatusError
	}
	tx.save()
	// to do: delete files after successful upload
}

func (tx *T) ReferencedFiles() []string {
	var result []string
	for _, cmd := range tx.Commands {
		if cmd[0] == "add" && len(cmd) == 2 {
			result = append(result, cmd[1])
		}
	}
	return result
}

func (tx *T) VerifyFiles() {
	/* needs to be implemented */
	tx.Status = StatusChecking
	tx.Status = StatusWaiting
	if len(tx.Err) > 0 {
		tx.Status = StatusError
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
// ]
type command []string

// assumes lock tx.m is held for writing
func (c command) Execute(iw *items.Writer, tx *T) {
	cmd := []string(c)
	if len(cmd) == 0 {
		return
	}
	switch {
	case cmd[0] == "delete" && len(cmd) == 2:
		// delete <blob id>
		id, err := strconv.Atoi(cmd[1])
		if err == nil {
			iw.DeleteBlob(items.BlobID(id))
		}
	case cmd[0] == "slot" && len(cmd) == 3:
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
				tx.Err = append(tx.Err, errors.New("Cannot resolve id "+cmd[2]))
				break
			}
		}
		iw.SetSlot(cmd[1], items.BlobID(id))
	case cmd[0] == "note" && len(cmd) == 2:
		// note <text>
		iw.SetNote(cmd[1])
	case cmd[0] == "add" && len(cmd) == 2:
		// add <file id>
		f := tx.files.Lookup(cmd[1])
		if f == nil {
			tx.Err = append(tx.Err, errors.New("Cannot find "+cmd[1]))
			break
		}
		reader := f.Open()
		fstat := f.Stat()
		bid, err := iw.WriteBlob(reader, fstat.Size, fstat.MD5, fstat.SHA256)
		reader.Close()
		if err != nil {
			tx.Err = append(tx.Err, err)
			break
		}
		tx.BlobMap[cmd[1]] = int(bid)
	default:
		tx.Err = append(tx.Err, errors.New("Bad command "+cmd[0]))
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
	}
	return false
}
