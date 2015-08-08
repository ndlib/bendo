package util

// A gate limits concurrency. Every gate is created with a maximum number
// number of goroutines to allow through at a time. Goroutines enter the gate
// by calling Enter(), and signal that they are done by calling Leave()
type Gate chan struct{}

func NewGate(n int) Gate {
	return Gate(make(chan struct{}, n))
}

func (g Gate) Enter() {
	g <- struct{}{}
}

func (g Gate) Leave() {
	<-g
}
