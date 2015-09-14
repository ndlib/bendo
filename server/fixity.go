package server

import (
	"errors"
	"io"
	"log"
	"sync"
	"time"

	"github.com/ndlib/bendo/util"
)

// Start a background goroutine to check item fixity at the given rate
// (in MB/hour). If the rate is 0, no background process is started.
func FixityCheck(rate int64) {
	fixityRate = float64(rate) * 1000000 / 3600
	if rate > 0 {
		go fixity()
	}
}

// StopFixity halts the background fixity checking process. The process is not
// resumable once stopped.
func StopFixity() {
	if fixityRate > 0 {
		close(fixitystop)
	}
}

var (
	// close this to stop the fixity process
	fixitystop chan struct{} = make(chan struct{})

	// rate to compute checksums in bytes/second
	fixityRate float64

	// do not checksum an item any more often than every 6 months
	minDurationChecksum = 180 * 24 * time.Hour
)

func fixity() {
	r := newRateCounter(fixityRate)
	c := make(chan string)
	go itemlist(c)
	for {
		select {
		case itemid := <-c:
			fixityItem(r, itemid)
		case <-fixitystop:
			return
		}
	}
}

func fixityItem(r *rateCounter, itemid string) {
	item, err := Items.Item(itemid)
	if err != nil {
		log.Printf("fixity for %s: %s", itemid, err.Error())
		return
	}
	// now checksum each blob listed in this item
	// TODO(dbrower): sort them by bundle number to optimize bundle loading
	for _, b := range item.Blobs {
		// open the blob stream
		blobreader, err := Items.Blob(itemid, b.ID)
		if err != nil {
			log.Printf("fixity for: (%s, %d): %s", itemid, b.ID, err.Error())
			continue
		}
		rr := &rateReader{reader: blobreader, rate: r}
		// VerifyStreamHash needs to pass back an error since the
		// rateReader indicates a timeout by returning an error
		newstatus := "ok"
		if !util.VerifyStreamHash(rr, b.MD5, b.SHA256) {
			// checksum error!
			newstatus = "error"
		}
		_ = UpdateChecksum(itemid, newstatus)
	}
}

// itemlist generates a list of item ids to checksum, and adds them to the
// provided channel.
func itemlist(c chan<- string) {
	for {
		id := OldestChecksum(time.Now().Add(-minDurationChecksum))
		if id == "" {
			// sleep if there are no ids available
			time.Sleep(time.Hour)
		} else {
			c <- id
		}
	}
}

var ErrTimeout = errors.New("fixitystop signaled")

type rateReader struct {
	reader io.Reader
	rate   *rateCounter
}

func (rr *rateReader) Read(p []byte) (int, error) {
	// wait for the rate limiter
	select {
	case <-rr.rate.OK():
	case <-fixitystop:
		return 0, ErrTimeout
	}
	n, err := rr.reader.Read(p)
	if err != nil {
		rr.rate.Use(int64(n))
	}
	return n, err
}

// A rateCounter tracks how many bytes we have checksummed and makes sure we
// keep under the rate limit given.
// Every so often we increment our pool. As we checksum we remove credits from
// the pool. If the pool goes negative, then we wait until it goes positive.
type rateCounter struct {
	c       chan time.Time // channel we use to signal credits is positive
	stop    chan struct{}  // close to signal adder goroutine to exit
	m       sync.Mutex     // protects below
	credits int64          // current credit balance
}

// Interval between adding credits to the pool. The shorter it is, the more
// waking and churning we do. The longer it is, the longer the process waits
// for credits to be added.
const rateInterval = 20 * time.Minute

// Make a new rater where credits accumulate at rate credits per second.
// However, the credits are not accumulated every second. Instead the entire
// amount due is added every 20 minutes.
func newRateCounter(rate float64) *rateCounter {
	amount := int64(rate * rateInterval.Seconds())
	r := &rateCounter{
		c:       make(chan time.Time),
		stop:    make(chan struct{}),
		credits: amount,
	}
	go r.adder(amount)
	return r
}

// Use some number of units, it is okay if it takes this counter negative.
func (r *rateCounter) Use(count int64) {
	r.m.Lock()
	r.credits -= count
	r.m.Unlock()
}

// Return a channel to wait on. The current time will be sent when it is OK
// to resume reading.
func (r *rateCounter) OK() <-chan time.Time {
	return r.c
}

// Stop the background goroutine refilling the rateCounter.
func (r *rateCounter) Stop() {
	close(r.stop)
}

// background goroutine. refills the rate counter based on the original rate
// it was created with.
func (r *rateCounter) adder(amount int64) {
	tick := time.NewTicker(rateInterval)
	for {
		var signal chan time.Time
		r.m.Lock()
		if r.credits > 0 {
			signal = r.c
		}
		r.m.Unlock()
		select {
		case <-tick.C:
			r.Use(-amount) // add amount to credits!
		case signal <- time.Now():
		case <-r.stop:
			close(r.c)
			return
		}
	}
}
