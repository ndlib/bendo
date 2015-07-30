package transaction

import (
	"io"
	"math/rand"
	"strconv"
	"sync"
	"time"

	"github.com/ndlib/bendo/fragment"
	"github.com/ndlib/bendo/store"
)

// Given a store, create a new registry.
// Use the Load() option to reload the metadata from the store.
func New(s store.Store) *Registy {
	return Registy{
		Files:   fragment.New(s),
		TxStore: store.NewWithPrefix(s, "tx:"),
		files:   make(map[string]*T),
	}
}

type Registy struct {
	Files   *fragment.Store
	TxStore *store.Store
	m       sync.RWMutex  // protects txs
	txs     map[string]*T // transaction ID to transaction
}

func (r *Registry) Load() error {
	err := r.Files.Load()
	if err != nil {
		return err
	}
	keys := r.TxStore.List()
	s.m.Lock()
	defer s.m.Unlock()
	for _, key := range keys {
		f, _, err := r.TxStore.Open(key)
		if f == nil || err != nil {
			// huh?
			continue
		}
		tx, err := load(f)
		f.Close()
		if err != nil {
			continue
		}
		tx.r = r
		r.txs[tx.ID] = tx
	}
	return nil
}

// [
// ["DELETE", 56],
// ["SLOT", "/asdf/45", 4],
// ["NOTE", "blah blah"]
// ]
//
// update item 78f7d8s (
//  add blob tr78s (
//     SHA256 43gahfg3g4ga9989898a88b
//     MD5 4323434b3b4b342
//  )
//  set slot "hello there" tr78s
//  set slot "/volume" 0
//  set slot "accessRights" 2
//  delete 4
// )
//
type command struct {
	Command string
	Blob    int
	Slot    string
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

// Create a new transaction in this registry.
func (r *Registy) Create(itemid string) *T {
	r.m.Lock()
	defer r.m.Unlock()
	tx := &T{
		ID:       r.makenewid(),
		Status:   StatusOpen,
		Started:  time.Now(),
		Modified: time.Now(),
		ItemID:   itemid,
		Slots:    make(map[string]int),
		r:        r,
	}
	r.txs[tx.ID] = tx
	return tx
}

// generate a new transaction id. Assumes caller holds r.m lock (either R or W)
func (r *Registy) makenewid() string {
	for {
		id := randomid()
		// see if already being used
		if _, ok := r.txs[id]; ok {
			return id
		}
	}
}

func randomid() string {
	var day int64 = int64(time.Now().YearDay())
	var n = day<<32 | int64(rand.Int31())
	return strconv.FormatInt(n, 36)
}

func (r *Registy) Lookup(txid string) *T {
	r.m.RLock()
	defer r.m.RUnlock()
	return r.txs[txid]
}

// Delete a transaction
func (r *Registy) Delete(id string) {
	r.m.Lock()
	tx := r.txs[id]
	delete(r.txs, id)
	s.m.Unlock()

	if tx == nil {
		return
	}

	// don't need the lock for the following
	r.TxStore.Delete(tx.ID)
	for _, child := range tx.NewBlobs {
		r.Files.Delete(child.ID)
	}
}

type T struct {
	r        *Registry    // our parent registry
	m        sync.RWMutex // protects everything below
	ID       string       // the id of this transaction
	Status   int          // one of Status*
	Started  time.Time    // time tx was created
	Modified time.Time    // last time user touch or added a file
	Err      []error      // list of errors (for StatusError)
	ItemID   string       // ID of the item this tx is modifying
	Creator  string       // ultimate comitter of this tx
	Commands []command    // commands to run on commit
	NewBlobs []*blob      // track the provisional blobs.
	Slots    map[string]string
}

const (
	StatusUnknown  = iota
	StatusOpen     // transaction is being modified by user
	StatusWaiting  // transaction has been submitted to be committed
	StatusChecking // files are being checksummed and verified
	StatusIngest   // files are being written into bundles
	StatusFinished // transaction is over, successful
	StatusError    // transaction had an error
)

type blob struct {
	PID    string // provisional id, good until we ingest it
	ID     int    // actual id, valid once it is written to the bundle
	MD5    []byte
	SHA256 []byte
	Size   int64
}

func (tx *T) NewFile(md5, sha256 []byte) *fragment.File {
	var id string
	var f *fragment.File
	for {
		id = randomid()
		f = tx.r.Files.New(id)
		if f != nil {
			break
		}
	}
	nb := &blob{
		PID: id,
		MD5: md5,
		SHA: sha256,
	}
	tx.m.Lock()
	defer tx.m.Unlock()
	tx.NewBlobs = append(tx.NewBlobs, nb)
	tx.Modified = time.Now()
	tx.save()
	return f
}

func (tx *T) SetSlot(slot, value string) {
	tx.m.Lock()
	defer tx.m.Unlock()
	tx.Slots[slot] = value
	tx.Modified = time.Now()
	tx.save()
}

func (tx *T) SetCreator(creator string) {
	tx.m.Lock()
	defer tx.m.Unlock()
	tx.Creator = creator
	tx.Modified = time.Now()
	tx.save()
}

func (tx *T) Update(s items.Store) {
	if !tx.VerifyFiles() {
		return
	}
	// now create or update the item
	itmWriter := s.Open(tx.ItemID)
	itmWriter.SetCreator(tx.Creator)
	for _, bl := range tx.NewBlobs {
		bid, err := itmWriter.WriteBlob(f, 0, bl.MD5, bl.SHA256)
		if err != nil {
			//
		}
		bl.ID = bid
	}

}

func (tx *T) VerifyFiles() {
	tx.m.Lock()
	tx.Status = StatusChecking
	for _, bl := range tx.NewBlobs {
		f := tx.r.Files.Open(bl.ID)
		f.Close()
	}
	if len(tx.Err) > 0 {
		tx.Status = StatusError
	}
}

func load(r io.ReaderAt) (*T, error) {
	dec := json.NewDecoder(store.NewReader(r))
	tx := new(T)
	err := dec.Decode(tx)
	if err != nil {
		tx = nil
	}
	return tx, err
}

// assumes lock is being held on call
func (tx *T) save() error {
	key := tx.ID
	err := tx.r.TxStore.Delete(key)
	if err != nil {
		return err
	}
	w, err := tx.r.TxStore.Create(key)
	if err != nil {
		return err
	}
	defer w.Close()
	enc := json.NewEncoder(w)
	return enc.Encode(tx)
}
