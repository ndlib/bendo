package server

import (
	"sync"
)

type singleflight struct {
	F        func(string) (interface{}, error) // function to fetch data
	mu       sync.Mutex                        // controls everything below
	inflight map[string]*fetchrequest          // requests in progress
}

type fetchrequest struct {
	wg     sync.WaitGroup
	result interface{}
	err    error
}

func (s *singleflight) Get(key string) (interface{}, error) {
	// the first goroutine asking for a given key will do the work. Others will wait until
	// the data is ready.
	if s.F == nil {
		return nil, nil
	}
	s.mu.Lock()
	if r, ok := s.inflight[key]; ok {
		// item is already being worked on
		s.mu.Unlock()
		r.wg.Wait()
		return r.result, r.err
	}
	// set up a flight record and then call the function
	r := &fetchrequest{}
	r.wg.Add(1)
	if s.inflight == nil {
		s.inflight = make(map[string]*fetchrequest)
	}
	s.inflight[key] = r
	s.mu.Unlock()
	defer func() {
		// at end we signal and remove the inflight record
		r.wg.Done()
		s.mu.Lock()
		delete(s.inflight, key)
		s.mu.Unlock()
	}()

	r.result, r.err = s.F(key)
	return r.result, r.err
}
