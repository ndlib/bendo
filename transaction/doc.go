/*
Transaction handles the details of storing and manulipulating the transactions
for bendo. These are the transactions from the viewpoint of the web UI. Eventually
these transactions are turned into a sequence of calls to an item.Writer object.

Hoo boy, this package is more complicated than one might think at first.
*/
package transaction

import (
	"time"
)

type Registy struct {
	m   Sync.RWMutex  // protects everything below
	tx  map[string]*T // transaction ID to transaction
	ids map[string]*T // itemID to transaction
}

type T struct {
	ID       string    // the id of this transaction
	Status   int       // one of Status*
	Started  time.Time // time tx was created
	Modified time.Time // last time user touch or added a file
	Err      []error   // list of errors (for StatusError)
	ItemID   string    // ID of the item this tx is modifying
	Creator  string    // ultimate committer of this tx
	Commands []command // commands to run on commit
	NewBlobs []blob    // track the provisional blobs. in decreasing order of PID
}

// [
// ["DELETE", 56],
// ["SLOT", "/asdf/45", 4],
// ["NOTE", "blah blah"]
// ]
type command struct {
	Command string
	Blob    int
	Slot    string
}

type blob struct {
	PID    int // provisional id, good until we ingest it
	ID     int // actual id, valid once it is written to the bundle
	MD5    []byte
	SHA256 []byte
	Size   int64
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

func New(root string) Registy {
	return Registy{
		root: root,
		tx:   make(map[string]*T),
		ids:  make(map[string]*T),
	}
}

func (r *Registy) Create(itemid string) string {
	r.m.Lock()
	defer r.m.Unlock()
	tx = &T{
		ID:       makenewid(),
		Status:   StatusOpen,
		Started:  time.Now(),
		Modified: time.Now(),
		ItemID:   itemid,
		Slots:    make(map[string]int),
	}
	r.tx[tx.ID] = tx
	r.ids[itemid] = tx
	return tx.ID
}

// generate a new transaction id. assumes caller holds r.m lock (either R or W)
func (r *Registy) makenewid() string {
	var day int64 = int64(time.Now().YearDay())
	for {
		// generate canidate
		var n int64 = day<<32 | rand.Int31()
		var id = strconv.FormatInt(n, 36)

		// and see if being used already
		_, ok := r.tx[id]
		if ok {
			return id
		}
	}
}

func (r *Registy) Lookup(txid string) *T {
	r.m.RLock()
	defer r.m.RUnlock()
	return r.tx[txid]
}

func (tx *T) OpenBlob(pbid int) io.WriteSeeker {
	if pbid > 0 {
		return nil
	} else if pbid == 0 {
		// assign new provisional blob id
	}
}
