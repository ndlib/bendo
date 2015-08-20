package util

// A gate limits concurrency. Every gate has a maximum number
// number of goroutines to allow through at a time. Goroutines enter the gate
// by calling Enter(), and signal that they are done by calling Leave()
type Gate chan struct{}

func NewGate(n int) Gate {
	return Gate(make(chan struct{}, n))
}

// A goroutine calls Enter at the beginning of the section to be protected by
// the gate, and it will add this goroutine insde the gate. If there are the
// maximum number of goroutines inside the gate already, Enter will block until
// there is room for this one.
func (g Gate) Enter() {
	g <- struct{}{}
}

// Leave marks a goroutine outside the critical section. It is important to
// balence each call to Enter with a call to Leave. Enter and Leave do not need
// to be called from the same goroutine, necessarily.
func (g Gate) Leave() {
	<-g
}
