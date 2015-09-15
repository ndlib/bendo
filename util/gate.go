package util

// A Gate limits concurrency. Every gate has a maximum number
// number of goroutines to allow through at a time. Goroutines enter the gate
// by calling Enter(), and signal that they are done by calling Leave()
type Gate chan struct{}

// NewGate returns a Gate which accepts at most n entries at a time.
func NewGate(n int) Gate {
	return Gate(make(chan struct{}, n))
}

// Enter is called at the beginning of the section to be protected by
// the gate, and will block the calling goroutine until there are less than
// n goroutines inside.
// It is safe to call this from multiple goroutines.
func (g Gate) Enter() {
	g <- struct{}{}
}

// Leave marks a goroutine outside the critical section. It is important to
// balance each call to Enter with a call to Leave. Enter and Leave do not need
// to be called from the same goroutine, necessarily.
func (g Gate) Leave() {
	<-g
}
