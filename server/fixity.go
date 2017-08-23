package server

import (
	"encoding/json"
	"expvar"
	"log"
	"math/rand"
	"strings"
	"time"
        "net/http"

       "github.com/julienschmidt/httprouter"
)

// Strcture for fixity records
type fixity struct {
   Id string
   Scheduled_time time.Time
   Status string
   Notes string
}


// A Finformation the fixity service needs to know what
// items have been checked, what needs to be checked, and any fixity errors
// found. It is presumed to be backed by a database, but that is not assumed.
// Methods should be safe to be called by multiple goroutines.
type FixityDB interface {
	// NextItem returns the item id of the item having the earliest pending
	// checksum that is before the cutoff time. If no items satisfy this
	// the empty string is returned. It will also not return anything in an
	// "error" state.
	NextFixity(cutoff time.Time) string
        GetFixityById(id string)  *fixity

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

var (
	xFixityRunning      = expvar.NewInt("fixity.running")
	xFixityItemsChecked = expvar.NewInt("fixity.check.count")
	xFixityBytesChecked = expvar.NewInt("fixity.check.bytes")
	xFixityDuration     = expvar.NewFloat("fixity.check.seconds")
	xFixityError        = expvar.NewInt("fixity.check.error")
	xFixityMismatch     = expvar.NewInt("fixity.check.mismatch")
)

// StartFixity starts the background goroutines to check item fixity. It
// returns immediately and does not block.
func (s *RESTServer) StartFixity() {
	xFixityRunning.Add(1)

	go s.fixity()

	// should scanfixity run periodically? or only at startup?
	// this will keep running it in a loop with 24 hour rest in between.
	go func() {
		for {
			if s.useTape {
				s.scanfixity()
			}
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
		if id == "" || !s.useTape {
			// sleep if there are no ids available.
			// an hour is arbitrary.
			time.Sleep(time.Hour)
			continue
		}
		log.Println("begin fixity check for", id)
		starttime := time.Now()
		nbytes, problems, err := s.Items.Validate(id)
		var status = "ok"
		var notes string
		if err != nil {
			log.Println("fixity validate error", err.Error())
			status = "error"
			notes = err.Error()
			xFixityError.Add(1)
		} else if len(problems) > 0 {
			status = "mismatch"
			notes = strings.Join(problems, "\n")
			xFixityMismatch.Add(1)
		}
		d := time.Now().Sub(starttime)
		log.Println("Fixity for", id, "is", status, "duration = ", d)
		err = s.Fixity.UpdateFixity(id, status, notes)

		xFixityItemsChecked.Add(1)
		xFixityBytesChecked.Add(nbytes)
		xFixityDuration.Add(d.Seconds())

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

func (s *RESTServer) GetFixityHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {

	item := r.FormValue("item")
	start := r.FormValue("start")
	end := r.FormValue("end")
        status := r.FormValue("status")

	log.Println("GetFixityHandler called")
	log.Println("item= ", item)
	log.Println("start= ", start)
	log.Println("end= ", end)
	log.Println("status= ", status)
}

func (s *RESTServer) GetFixityIdHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	 id := ps.ByName("id")
	 log.Println("GetFixityIdHandler passed id =",id)
	 thisFixity := s.Fixity.GetFixityById(id)

	 if thisFixity == nil {
		w.WriteHeader(404)
	        log.Println("GetFixityIdHandler id =",id, " Returns 404")
		return
	 }

	 enc := json.NewEncoder(w)
	 enc.Encode(thisFixity)
}
