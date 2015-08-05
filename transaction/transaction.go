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
func New(s store.Store) *Registry {
	return &Registry{
		Files:   fragment.New(s),
		TxStore: fragment.NewJSON(store.NewWithPrefix(s, "tx:")),
		txs:     make(map[string]*T),
	}
}

type Registry struct {
	Files   *fragment.Store
	TxStore fragment.JSONStore
	m       sync.RWMutex  // protects txs
	txs     map[string]*T // transaction ID to transaction
}

func (r *Registry) Load() error {
	err := r.Files.Load()
	if err != nil {
		return err
	}
	r.m.Lock()
	defer r.m.Unlock()
	for key := range r.TxStore.List() {
		tx := new(T)
		err := r.TxStore.Open(key, tx)
		if err != nil {
			continue
		}
		tx.files = r.Files
		tx.txstore = &r.TxStore
		r.txs[tx.ID] = tx
	}
	return nil
}

// Return a list of all the stored transactions.
func (r *Registry) List() []string {
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
)

// Create a new transaction to update itemid. There can only be at most one
// transaction per itemid.
func (r *Registry) Create(itemid string) (*T, error) {
	r.m.Lock()
	defer r.m.Unlock()
	// is there currently a transaction for the item?
	for _, tx := range r.txs {
		if tx.ItemID == itemid {
			return nil, ErrExistingTransaction
		}
	}
	tx := &T{
		ID:       r.makenewid(),
		Status:   StatusOpen,
		Started:  time.Now(),
		Modified: time.Now(),
		ItemID:   itemid,
		files:    r.Files,
		txstore:  &r.TxStore,
	}
	r.txs[tx.ID] = tx
	r.TxStore.Save(tx.ID, tx)
	return tx, nil
}

// generate a new transaction id. Assumes caller holds r.m lock (either R or W)
func (r *Registry) makenewid() string {
	for {
		id := randomid()
		// see if already being used
		if _, ok := r.txs[id]; !ok {
			return id
		}
	}
}

func randomid() string {
	var n = rand.Int63()
	return strconv.FormatInt(n, 36)
}

func (r *Registry) Lookup(txid string) *T {
	r.m.RLock()
	defer r.m.RUnlock()
	return r.txs[txid]
}

// Delete a transaction
func (r *Registry) Delete(id string) error {
	r.m.Lock()
	tx := r.txs[id]
	delete(r.txs, id)
	r.m.Unlock()

	if tx == nil {
		return nil
	}

	// don't need the lock for the following
	err := r.TxStore.Delete(tx.ID)
	for _, bl := range tx.NewBlobs {
		er := r.Files.Delete(bl.PID)
		if err == nil {
			err = er
		}
	}
	return err
}

type T struct {
	txstore  *fragment.JSONStore // where this structure is stored
	files    *fragment.Store     // Where files are stored
	m        sync.RWMutex        // protects everything below
	ID       string              // the id of this transaction
	Status   int                 // one of Status*
	Started  time.Time           // time tx was created
	Modified time.Time           // last time user touch or added a file
	Err      []error             // list of errors (for StatusError)
	ItemID   string              // ID of the item this tx is modifying
	Commands []command           // commands to run on commit
	NewBlobs []*blob             // track the provisional blobs.
}

const (
	StatusUnknown  = iota
	StatusOpen     // transaction is being modified by user
	StatusChecking // files are being checksummed and verified
	StatusWaiting  // transaction has been submitted to be committed
	StatusIngest   // files are being written into bundles
	StatusFinished // transaction is over, successful
	StatusError    // transaction had an error
)

type blob struct {
	PID    string // provisional id, good until we ingest it
	ID     items.BlobID
	MD5    []byte
	SHA256 []byte
}

func (tx *T) NewFile(md5, sha256 []byte) *fragment.File {
	var id string
	var f *fragment.File
	for {
		id = randomid()
		f = tx.files.New(id)
		if f != nil {
			break
		}
	}
	nb := &blob{
		PID:    id,
		MD5:    md5,
		SHA256: sha256,
	}
	tx.m.Lock()
	defer tx.m.Unlock()
	tx.NewBlobs = append(tx.NewBlobs, nb)
	tx.Modified = time.Now()
	tx.save()
	return f
}

var (
	ErrBadCommand = errors.New("Bad command")
)

func (tx *T) AddCommandList(cmds [][]string) error {
	// first make sure commands are okay
	for _, cmd := range cmds {
		c := command(cmd)
		if !c.WellFormed() {
			return ErrBadCommand
		}
	}
	tx.m.Lock()
	for _, cmd := range cmds {
		tx.Commands = append(tx.Commands, command(cmd))
	}
	tx.Modified = time.Now()
	tx.m.Unlock()
	return nil
}

func (tx *T) SetSlot(slot, value string) {
	tx.addcommand("slot", slot, value)
}

func (tx *T) SetNote(note string) {
	tx.addcommand("note", note)
}

func (tx *T) DeleteBlob(blobid string) {
	tx.addcommand("delete", blobid)
}

func (tx *T) addcommand(cmd ...string) {
	tx.m.Lock()
	defer tx.m.Unlock()
	tx.Commands = append(tx.Commands, command(cmd))
	tx.Modified = time.Now()
	tx.save()
}

// Commit this transaction to the given store, creating or updating the
// underlying item.
func (tx *T) Commit(s items.Store, Creator string) {
	tx.m.Lock()
	defer tx.m.Unlock()
	tx.Status = StatusIngest
	iw := s.Open(tx.ItemID)
	iw.SetCreator(Creator)
	for _, bl := range tx.NewBlobs {
		f := tx.files.Lookup(bl.PID)
		reader := f.Open()
		bid, err := iw.WriteBlob(reader, f.Size, bl.MD5, bl.SHA256)
		reader.Close()
		if err != nil {
			tx.Err = append(tx.Err, err)
			continue
		}
		bl.ID = bid
	}
	// execute commands. Do this after adding any new blobs, so that they
	// will have already been given IDs.
	for _, cmd := range tx.Commands {
		// Execute will append errors to tx.Err
		cmd.Execute(iw, tx)
	}
	iw.Close()
	tx.Status = StatusFinished
	if len(tx.Err) > 0 {
		tx.Status = StatusError
	}
	tx.save()
}

func (tx *T) VerifyFiles() {
	/* needs to be implemented */
	tx.Status = StatusChecking
	for _, bl := range tx.NewBlobs {
		_ = tx.files.Lookup(bl.PID)
	}
	tx.Status = StatusWaiting
	if len(tx.Err) > 0 {
		tx.Status = StatusError
	}
}

// must hold lock tx.m to call this
func (tx *T) save() {
	if tx.txstore != nil {
		tx.txstore.Save(tx.ID, tx)
	}
}

// [
// ["delete", 56],
// ["slot", "/asdf/45", 4],
// ["note", "blah blah"]
// ]
//
// (update 78f7d8s (
//	(add tr78s
//		(SHA256 43gahfg3g4ga9989898a88b)
//		(MD5 4323434b3b4b342))
//	(slot "hello there" tr78s)
//	(slot "/volume" 0)
//	(slot "accessRights" 2)
//	(delete 4)
// ))
type command []string

// assumes lock tx.m is held for writing
func (c command) Execute(iw *items.Writer, tx *T) {
	cmd := []string(c)
	if len(cmd) == 0 {
		return
	}
	switch {
	case cmd[0] == "delete" && len(cmd) == 2:
		id, err := strconv.Atoi(cmd[1])
		if err == nil {
			iw.DeleteBlob(items.BlobID(id))
		}
	case cmd[0] == "slot" && len(cmd) == 3:
		id, err := strconv.Atoi(cmd[2])
		if err != nil {
			// can we resolve the id to a new blob?
			for _, bl := range tx.NewBlobs {
				if bl.PID == cmd[2] {
					id = int(bl.ID)
					err = nil
					break
				}
			}
		}
		if err == nil {
			iw.SetSlot(cmd[1], items.BlobID(id))
		}
	case cmd[0] == "note" && len(cmd) == 2:
		iw.SetNote(cmd[1])
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
	}
	return false
}
