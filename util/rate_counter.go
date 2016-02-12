package util

import (
	"errors"
	"io"
	"sync"
	"time"
)

// A RateCounter tracks how many bytes we have checksummed and makes sure we
// keep under the rate limit given.
// Every so often we increment our pool. As we checksum we remove credits from
// the pool. If the pool goes negative, then we wait until it goes positive.
type RateCounter struct {
	c       chan struct{} // channel we use to signal credits is positive
	stop    chan struct{} // close to signal adder goroutine to exit
	m       sync.Mutex    // protects below
	credits int64         // current credit balance
}

// Interval between adding credits to the pool. The shorter it is, the more
// waking and churning we do. The longer it is, the longer the process waits
// for credits to be added.
const rateInterval = 1 * time.Minute

// NewRateCounter returns a counter where credits accumulate
// at the given credits per second. However, the credits are
// not accumulated every second. Instead the entire amount
// due is added every 20 minutes.
func NewRateCounter(rate float64) *RateCounter {
	amount := int64(rate * rateInterval.Seconds())
	r := &RateCounter{
		c:       make(chan struct{}),
		stop:    make(chan struct{}),
		credits: amount,
	}
	go r.adder(amount)
	return r
}

// Use some number of units. It is okay if it takes this counter negative.
func (r *RateCounter) Use(count int64) {
	r.m.Lock()
	r.credits -= count
	r.m.Unlock()
}

// OK returns a channel to wait on. It will receive an empty struct when it is OK
// to resume reading. The channel will be closed if the RateCounter is Stopped.
func (r *RateCounter) OK() <-chan struct{} {
	return r.c
}

// Stop the background goroutine refilling the RateCounter. Will panic if
// called twice.
func (r *RateCounter) Stop() {
	// the background process will then close r.c, which will cancel any
	// readers
	close(r.stop)
}

// adder is the background goroutine that refills the rate counter based on the
// rate this RateCounter was created with.
func (r *RateCounter) adder(amount int64) {
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
// this RateCounter. Reads will block until the RateCounter says the current
// usage is ok. It is okay for more than one goroutine to use the same
// RateCounter. If the RateCounter was stopped, the returned reader will
// cause an ErrStopped.
func (r *RateCounter) Wrap(reader io.Reader) io.Reader {
	return rateReader{reader: reader, rate: r}
}

// ErrStopped means a read failed because the governing rate counter was stopped.
var ErrStopped = errors.New("RateCounter stopped")

type rateReader struct {
	reader io.Reader
	rate   *RateCounter
}

func (r rateReader) Read(p []byte) (int, error) {
	// wait for the rate limiter
	_, ok := <-r.rate.OK()
	if !ok {
		// our RateCounter was stopped.
		return 0, ErrStopped
	}
	n, err := r.reader.Read(p)
	r.rate.Use(int64(n))
	return n, err
}
