package util

import (
	"sync"
)

// A Gate limits concurrency. Every gate has a maximum number
// number of goroutines to allow through at a time. Goroutines enter the gate
// by calling Enter(), and signal that they are done by calling Leave().
// Enter() blocks until either there is room for the calling goroutine to enter,
// or Stop() is called on the Gate.
type Gate struct {
	c  chan struct{}
	wg sync.WaitGroup
}

// NewGate returns a Gate which accepts at most n entries at a time.
func NewGate(n int) *Gate {
	return &Gate{c: make(chan struct{}, n)}
}

// Enter is called at the beginning of the section to be protected by the gate,
// and will block the calling goroutine until there are less than n goroutines
// inside, or Stop() is called on the Gate. It is safe to call this from
// multiple goroutines. Enter returns false if the return is due to Stop being
// called and true otherwise.
func (g *Gate) Enter() bool {
	var ok = true
	defer func() {
		if recover() != nil {
			ok = false
		}
	}()
	g.c <- struct{}{}
	g.wg.Add(1)
	return ok
}

// Leave marks a goroutine outside the critical section. It is important to
// balance each call to Enter with a call to Leave. Enter and Leave do not need
// to be called from the same goroutine, necessarily.
func (g *Gate) Leave() {
	g.wg.Done()
	<-g.c
}

// Stop will cause all goroutines waiting on Enter() for this gate to exit with
// a false return value. Stop will then block until all the goroutines
// currently inside the Gate call Leave.
func (g *Gate) Stop() {
	close(g.c)
	g.wg.Wait()
}
