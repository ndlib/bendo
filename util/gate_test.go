package util

import (
	"sync/atomic"
	"testing"
	"time"
)

func TestGateMaximum(t *testing.T) {
	// create 10 goroutines trying to enter a gate that can only hold 5
	g := NewGate(5)
	var nenter, nerr int64
	for i := 0; i < 10; i++ {
		go func() {
			ok := g.Enter()
			if ok {
				atomic.AddInt64(&nenter, 1)
			} else {
				atomic.AddInt64(&nerr, 1)
			}
		}()
	}

	time.Sleep(10 * time.Millisecond)
	// there should be 5 enters
	if nenter != 5 {
		t.Errorf("Received %d enters, expected %d", nenter, 5)
	}
	if nerr != 0 {
		t.Errorf("Received %d errors, expected %d", nerr, 0)
	}

	// call leave a few times and see what happens
	g.Leave()
	g.Leave()
	time.Sleep(10 * time.Millisecond)

	if nenter != 7 {
		t.Errorf("Received %d enters, expected %d", nenter, 7)
	}
	if nerr != 0 {
		t.Errorf("Received %d errors, expected %d", nerr, 0)
	}

	// need to balance out the 5 enters which have made it through
	// the gate. But need to do it AFTER we call Stop, so the enters
	// still waiting exit in error.
	go func() {
		time.Sleep(5 * time.Millisecond)
		for i := 0; i < 5; i++ {
			g.Leave()
		}
	}()
	g.Stop()

	if nenter != 7 {
		t.Errorf("Received %d enters, expected %d", nenter, 7)
	}
	if nerr != 3 {
		t.Errorf("Received %d errors, expected %d", nerr, 3)
	}
}
