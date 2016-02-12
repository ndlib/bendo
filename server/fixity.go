package server

import (
	"log"
	"math/rand"
	"strings"
	"time"
)

// A FixityDB tracks the information the fixity service needs to know what
// items have been checked, what needs to be checked, and any fixity errors
// found. It is presumed to be backed by a database, but that is not assumed.
// Methods should be safe to be called by multiple goroutines.
type FixityDB interface {
	// NextItem returns the item id of the item having the earliest pending
	// checksum that is before the cutoff time. If no items satisfy this
	// the empty string is returned. It will also not return anything in an
	// "error" state.
	NextFixity(cutoff time.Time) string

	// UpdateItem takes the id of an item and adjusts the earliest pending
	// fixity check for that item to have the given status and notes.
	// Notes contains general text providing details on any problems.
	// If there is no pending fixity check for the item, one is created.
	UpdateFixity(id string, status string, notes string) error

	// SetCheck takes an item id and schedules another fixity check at the
	// given time in the future (or past).
	SetCheck(id string, when time.Time) error

	// LookupCheck takes an item id and returns the time of earliest pending
	// fixity check for that item. If no fixity check is pending, returns
	// the zero time.
	LookupCheck(id string) (time.Time, error)
}

// StartFixity starts the background goroutines to check item fixity. It
// returns immediately and does not block.
func (s *RESTServer) StartFixity() {
	go s.fixity()

	// should scanfixity run periodically? or only at startup?
	// this will keep running it in a loop with 24 hour rest in between.
	go func() {
		for {
			s.scanfixity()
			time.Sleep(24 * time.Hour)
		}
	}()
}

const (
	// by default schedule the next fixity in 273 days (~9 months)
	// this duration is completely arbitrary.
	nextFixityDuration = 273 * 24 * time.Hour
)

// implements an infinite loop doing fixity checking. This function does not
// return.
func (s *RESTServer) fixity() {
	log.Println("Starting fixity loop")
	for {
		id := s.Fixity.NextFixity(time.Now())
		if id == "" {
			// sleep if there are no ids available.
			// an hour is arbitrary.
			time.Sleep(time.Hour)
			continue
		}
		log.Println("begin fixity check for", id)
		starttime := time.Now()
		_, problems, err := s.Items.Validate(id)
		var status = "ok"
		var notes string
		if err != nil {
			log.Println("fixity validate error", err.Error())
			status = "error"
			notes = err.Error()
		} else if len(problems) > 0 {
			status = "mismatch"
			notes = strings.Join(problems, "\n")
		}
		log.Println("Fixity for", id, "is", status, "duration = ", time.Now().Sub(starttime))
		err = s.Fixity.UpdateFixity(id, status, notes)
		// schedule the next check unless one is already scheduled
		when, _ := s.Fixity.LookupCheck(id)
		if when.IsZero() {
			s.Fixity.SetCheck(id, time.Now().Add(nextFixityDuration))
		}
	}
}

// scanfixity will make sure every item in the item store has a fixity
// scheduled. If not, it will schedule one at some random interval between
// now and the nextFixityDuration period in the future.
//
// This will scan each item in the store and then exit.
func (s *RESTServer) scanfixity() {
	log.Println("Starting scanfixity")
	rand.Seed(time.Now().Unix())
	var starttime = time.Now()
	for id := range s.Items.List() {
		when, err := s.Fixity.LookupCheck(id)
		if err != nil {
			// error? skip this id
			log.Println("scanfixity", id, err.Error())
			continue
		}
		if !when.IsZero() {
			// something is scheduled
			continue
		}
		// schedule something for some random period into the future
		log.Println("scanfixity adding", id)
		jitter := rand.Int63n(int64(nextFixityDuration))
		s.Fixity.SetCheck(id, time.Now().Add(time.Duration(jitter)))
	}
	log.Println("Ending scanfixity. duration = ", time.Now().Sub(starttime))
}
