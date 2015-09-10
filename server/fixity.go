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
// (in MB/hour). If the rate is 0, no goroutine is started.
func FixityCheck(rate int64) {
	fixityRate = float64(rate) * 1000000 / 3600
	if rate > 0 {
		go fixity()
	}
}

// StopFixity halts the background fixity checking process. It is not resumable.
func StopFixity() {
	if fixityRate > 0 {
		close(fixitystop)
	}
}

var (
	// close this to stop the fixity process
	fixitystop chan struct{} = make(chan struct{})
	// rate the checksum in bytes/second
	fixityRate float64
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
		if !util.VerifyStreamHash(rr, b.MD5, b.SHA256) {
			// checksum error!
		}
	}
}

func itemlist(c chan<- string) {
	// first list everything in the Item store
	in := Items.List()
	for {
		select {
		case item := <-in:
			c <- item
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
	tick    *time.Ticker   // our tick source
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
		tick:    time.NewTicker(rateInterval),
		stop:    make(chan struct{}),
		credits: amount,
	}
	go r.adder(amount)
	return r
}

func (r *rateCounter) Use(count int64) {
	r.m.Lock()
	r.credits -= count
	r.m.Unlock()
}

func (r *rateCounter) OK() <-chan time.Time {
	return r.c
}

func (r *rateCounter) Stop() {
	close(r.stop)
}

func (r *rateCounter) adder(amount int64) {
	for {
		var signal chan time.Time
		r.m.Lock()
		if r.credits > 0 {
			signal = r.c
		}
		r.m.Unlock()
		select {
		case <-r.tick.C:
			r.Use(-amount) // add amount to credits!
		case signal <- time.Now():
		case <-r.stop:
			close(r.c)
			return
		}
	}
}
