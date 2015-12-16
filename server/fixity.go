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
func (s *RESTServer) FixityCheck(rate int64) {
	if rate > 0 {
		bytesPerSec := float64(rate) * 1000000 / 3600
		fixityratelimiter = newRateCounter(bytesPerSec)
		go s.fixity(fixityratelimiter)
	}
}

// StopFixity halts the background fixity checking process. The process is not
// resumable once stopped.
func (s *RESTServer) StopFixity() {
	fixityratelimiter.Stop()
}

var (
	// call Stop on this to quit the fixity process
	fixityratelimiter *rateCounter

	// do not checksum an item any more often than every 6 months
	minDurationChecksum = 180 * 24 * time.Hour
)

// start a goroutine to do fixity checking using the given rateCounter
func (s *RESTServer) fixity(r *rateCounter) {
	c := make(chan string)
	go itemlist(c)
	for itemid := range c {
		status, _, err := s.fixityItem(r, itemid)
		// TODO(dbrower): exit if fixityItem returns a timeout
		if err == ErrStopped {
			return
		}
		_ = UpdateChecksum(itemid, status)
	}
}

// fixityItem checksums all the blobs in a single item. It uses the passed-in
// rateCounter to limit its reads. When finished it returns a status of "ok",
// "error", or "mismatch", a (possibly empty) list of messages, and an actual
// error if there was a transient error (not a validation error).
func (s *RESTServer) fixityItem(r *rateCounter, itemid string) (status string, msgs []string, err error) {
	status = "error"
	item, err := s.Items.Item(itemid)
	if err != nil {
		msgs = append(msgs, err.Error())
		log.Printf("fixity for %s: %s", itemid, err.Error())
		return
	}
	// now checksum each blob listed in this item
	// TODO(dbrower): sort them by bundle number to optimize bundle loading
	for _, b := range item.Blobs {
		// open the blob stream
		var blobreader io.ReadCloser
		blobreader, err = s.Items.Blob(itemid, b.ID)
		if err != nil {
			msgs = append(msgs, err.Error())
			log.Printf("fixity for: (%s, %d): %s", itemid, b.ID, err.Error())
			continue
		}
		rr := r.Wrap(blobreader)
		var ok bool
		ok, err = util.VerifyStreamHash(rr, b.MD5, b.SHA256)
		if err == ErrStopped {
			// we were cancelled
			return
		} else if err != nil {
			msgs = append(msgs, err.Error())
			continue
		} else if !ok {
			// checksum error!
			status = "mismatch"
			msgs = append(msgs, "Checksum mismatch")
			log.Printf("fixity mismatch: (%s, %d)", itemid, b.ID)
		}
	}
	if len(msgs) == 0 {
		status = "ok"
	}
	return
}

// itemlist generates a list of item ids to checksum, and sends them to the
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

// A rateCounter tracks how many bytes we have checksummed and makes sure we
// keep under the rate limit given.
// Every so often we increment our pool. As we checksum we remove credits from
// the pool. If the pool goes negative, then we wait until it goes positive.
type rateCounter struct {
	c       chan struct{} // channel we use to signal credits is positive
	stop    chan struct{} // close to signal adder goroutine to exit
	m       sync.Mutex    // protects below
	credits int64         // current credit balance
}

// Interval between adding credits to the pool. The shorter it is, the more
// waking and churning we do. The longer it is, the longer the process waits
// for credits to be added.
const rateInterval = 1 * time.Minute

// Make a new rater where credits accumulate at rate credits per second.
// However, the credits are not accumulated every second. Instead the entire
// amount due is added every 20 minutes.
func newRateCounter(rate float64) *rateCounter {
	amount := int64(rate * rateInterval.Seconds())
	r := &rateCounter{
		c:       make(chan struct{}),
		stop:    make(chan struct{}),
		credits: amount,
	}
	go r.adder(amount)
	return r
}

// Use some number of units. It is okay if it takes this counter negative.
func (r *rateCounter) Use(count int64) {
	r.m.Lock()
	r.credits -= count
	r.m.Unlock()
}

// Return a channel to wait on. It will receive an empty struct when it is OK
// to resume reading. The channel will be closed if the rateCounter is Stopped.
func (r *rateCounter) OK() <-chan struct{} {
	return r.c
}

// Stop the background goroutine refilling the rateCounter. Will panic if
// called twice.
func (r *rateCounter) Stop() {
	// the background process will then close r.c, which will cancel any
	// readers
	close(r.stop)
}

// adder is the background goroutine that refills the rate counter based on the
// rate this rateCounter was created with.
func (r *rateCounter) adder(amount int64) {
	tick := time.NewTicker(rateInterval)
	for {
		var signal chan struct{}
		r.m.Lock()
		if r.credits > 0 {
			signal = r.c
		}
		r.m.Unlock()
		select {
		case <-tick.C:
			r.Use(-amount) // add amount to credits!
		case signal <- struct{}{}:
		case <-r.stop:
			close(r.c)
			return
		}
	}
}

// Wrap takes an io.Reader and returns a new one where reads are limited by
// this rateCounter. Reads will block until the rateCounter says the current
// usage is ok. It is okay for more than one goroutine to use the same
// rateCounter. If the rateCounter was stopped, the returned reader will
// cause an ErrStopped.
func (r *rateCounter) Wrap(reader io.Reader) io.Reader {
	return rateReader{reader: reader, rate: r}
}

var ErrStopped = errors.New("rateCounter stopped")

type rateReader struct {
	reader io.Reader
	rate   *rateCounter
}

func (r rateReader) Read(p []byte) (int, error) {
	// wait for the rate limiter
	_, ok := <-r.rate.OK()
	if !ok {
		// our rateCounter was stopped.
		return 0, ErrStopped
	}
	n, err := r.reader.Read(p)
	r.rate.Use(int64(n))
	return n, err
}
