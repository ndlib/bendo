package bclientapi

import (
	"log"
	"net/http"
	"sort"
	"sync"
)

// An ErrorServer wraps another http.Handler and injects errors as
// described by a given playbook. A playbook is given by calling
// Reset(). Each call to ServeHTTP on the server increments a count
// starting at 0. A play gives a count to activate, and when the
// server reaches that count it will return the given Status and
// Body. Otherwise, requests are passed on to the wrapped handler.
// This is safe for concurrent use.
type ErrorServer struct {
	h http.Handler

	m        sync.Mutex
	count    int
	playbook []Play
}

type Play struct {
	When   int
	Status int
	Body   string
}

func (s *ErrorServer) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	s.m.Lock()
	count := s.count
	s.count++
	log.Printf("(%d) %s %s\n", count, req.Method, req.URL)
	for len(s.playbook) > 0 && s.playbook[0].When <= count {
		p := s.playbook[0]
		s.playbook = s.playbook[1:]
		if p.When < count {
			// more than one play had same count. Ignore the rest.
			continue
		}
		s.m.Unlock()
		w.WriteHeader(p.Status)
		w.Write([]byte(p.Body))
		return
	}
	s.m.Unlock()
	s.h.ServeHTTP(w, req)
}

func (s *ErrorServer) Reset(playbook []Play) {
	s.m.Lock()
	s.count = 0
	s.playbook = playbook[:]
	sort.Sort(ByWhen(s.playbook))
	s.m.Unlock()
}

type ByWhen []Play

func (p ByWhen) Len() int           { return len(p) }
func (p ByWhen) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }
func (p ByWhen) Less(i, j int) bool { return p[i].When < p[j].When }
