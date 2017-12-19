package server

import (
	"encoding/json"
	"errors"
	"expvar"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/julienschmidt/httprouter"
)

// A Fixity represents a single past or future fixity check.
type Fixity struct {
	ID            int64     // id of this record
	Item          string    // the item being verified
	ScheduledTime time.Time `json:"Scheduled_time"` // scheduled time
	Status        string    // "scheduled", "mismatch", "ok", "error"
	Notes         string    // mostly only used in case of an error
}

// A FixityDB tracks the information the fixity service needs to know what
// items have been checked, what needs to be checked, and any fixity errors
// found. It is presumed to be backed by a database, but that is not assumed.
// Methods should be safe to be called by multiple goroutines.
//
// The fixity records in the "scheduled" state are free to be modified. But
// records in other states should be considered immutable.
type FixityDB interface {
	// NextItem returns the fixity record id of the earliest pending record
	// that is before the cutoff time. If there are no such records 0 is returned.
	NextFixity(cutoff time.Time) int64

	// GetFixty retuens the fixity record with the given id.
	// Returns nil if no such record was found, or on error.
	GetFixity(id int64) *Fixity

	// SearchFixty returns the fixity records matching the provided arguments.
	// It returns an empty slice if there are no matching records or on error.
	// Status can be 'scheduled', 'error', 'ok', or 'mismatch'
	// Use the zero value for a parameter to represent a wildcard.
	SearchFixity(start time.Time, end time.Time, item string, status string) []*Fixity

	// UpdateItem will either update a fixity record or create a new one. If
	// the ID field is empty (i.e. = 0) then a new fixity record is created. If
	// an ID was passed, and the status of the record in the database is
	// "scheduled", then the database is updated to match the passed in record.
	// The id of the created/updated record is returned. If record has an empty
	// status, it will default to "scheduled".
	UpdateFixity(record Fixity) (int64, error)

	// Delete fixity record id, only if the status is "scheduled". Error otherwise.
	DeleteFixity(id int64) error

	// LookupCheck takes an item id and returns the time of earliest pending
	// fixity check for that item. If no fixity check is pending, returns
	// the zero time.
	LookupCheck(item string) (time.Time, error)
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
		id := s.FixityDatabase.NextFixity(time.Now())
		if id == 0 || !s.useTape {
			// sleep if there are no ids available.
			// an hour is arbitrary.
			time.Sleep(time.Hour)
			continue
		}
		fx := s.FixityDatabase.GetFixity(id)
		if fx == nil {
			log.Println("fixity received bad id", id)
			continue
		}
		log.Println("begin fixity check for", fx.Item)
		starttime := time.Now()
		nbytes, problems, err := s.Items.Validate(fx.Item)
		fx.Status = "ok"
		if err != nil {
			log.Println("fixity validate error", err)
			fx.Status = "error"
			fx.Notes = err.Error()
			xFixityError.Add(1)
		} else if len(problems) > 0 {
			fx.Status = "mismatch"
			fx.Notes = strings.Join(problems, "\n")
			xFixityMismatch.Add(1)
		}
		d := time.Now().Sub(starttime)
		log.Println("Fixity for", fx.Item, "is", fx.Status, "duration = ", d)
		_, err = s.FixityDatabase.UpdateFixity(*fx)
		if err != nil {
			log.Println("fixity:", err)
		}

		xFixityItemsChecked.Add(1)
		xFixityBytesChecked.Add(nbytes)
		xFixityDuration.Add(d.Seconds())

		// schedule the next check unless one is already scheduled
		when, _ := s.FixityDatabase.LookupCheck(fx.Item)
		if when.IsZero() {
			s.FixityDatabase.UpdateFixity(Fixity{
				Item:          fx.Item,
				ScheduledTime: time.Now().Add(nextFixityDuration),
			})
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
		when, err := s.FixityDatabase.LookupCheck(id)
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
		s.FixityDatabase.UpdateFixity(Fixity{
			Item:          id,
			ScheduledTime: time.Now().Add(time.Duration(jitter)),
		})
	}
	log.Println("Ending scanfixity. duration = ", time.Now().Sub(starttime))
}

func (s *RESTServer) GetFixityHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {

	item := r.FormValue("item")
	start := r.FormValue("start")
	end := r.FormValue("end")
	status := r.FormValue("status")

	now := time.Now()
	lastnight := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.Local)
	tonight := lastnight.Add(24 * time.Hour)

	startValue, err := timeValidate(start, lastnight)
	if err != nil {
		w.WriteHeader(400)
		fmt.Fprintln(w, err)
		return
	}

	endValue, err := timeValidate(end, tonight)
	if err != nil {
		w.WriteHeader(400)
		fmt.Fprintln(w, err)
		return
	}

	statusValue, err := statusValidate(status)
	if err != nil {
		w.WriteHeader(400)
		fmt.Fprintln(w, err)
		return
	}

	result := s.FixityDatabase.SearchFixity(startValue, endValue, item, statusValue)
	if result == nil {
		fmt.Fprintln(w, "[]")
		return
	}
	enc := json.NewEncoder(w)
	enc.Encode(result)
}

func (s *RESTServer) GetFixityIdHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	id := ps.ByName("id")
	id0, err := strconv.ParseInt(id, 10, 64)
	if err != nil {
		w.WriteHeader(404)
		return
	}
	result := s.FixityDatabase.GetFixity(id0)

	if result == nil {
		w.WriteHeader(404)
		fmt.Fprintln(w, "GET /fixity/", id, " Not Found")
		return
	}

	enc := json.NewEncoder(w)
	enc.Encode(result)
}

// DeleteFixity handles requests to DELETE /fixty/:id
// This deletes a scheduled fixity check for the given id . reurns 404 ff no request is found in the 'scheduled' state for the give id,
// 200 otherwise.
func (s *RESTServer) DeleteFixityHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	id := ps.ByName("id")
	id0, err := strconv.ParseInt(id, 10, 64)
	if err != nil {
		w.WriteHeader(404)
		return
	}
	err = s.FixityDatabase.DeleteFixity(id0)
	if err != nil {
		w.WriteHeader(500)
		fmt.Fprintln(w, err)
	}
}

func (s *RESTServer) PutFixityHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	id := ps.ByName("id")
	id0, err := strconv.ParseInt(id, 10, 64)
	if err != nil {
		w.WriteHeader(404)
		return
	}
	record := s.FixityDatabase.GetFixity(id0)
	if record == nil {
		w.WriteHeader(404)
		return
	}
	record.ScheduledTime = time.Now()
	_, err = s.FixityDatabase.UpdateFixity(*record)

	if err != nil {
		w.WriteHeader(500)
		fmt.Fprintln(w, err)
	}
}

// PostFixityHandler handles requests to POST /fixty/:item
func (s *RESTServer) PostFixityHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {

	item := ps.ByName("item")
	_, err := s.FixityDatabase.UpdateFixity(Fixity{
		Item:          item,
		ScheduledTime: time.Now(),
	})

	if err != nil {
		w.WriteHeader(500)
		fmt.Fprintln(w, err.Error())
	}
}

// Some validation routines for GET /fixity params

func timeValidate(s string, missing time.Time) (time.Time, error) {
	if s == "" {
		return missing, nil
	} else if s == "*" {
		return time.Time{}, nil
	}
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		t, err = time.Parse("2006-01-02", s)
	}
	return t, err
}

func statusValidate(param string) (string, error) {
	switch param {
	case "scheduled", "error", "mismatch", "ok", "":
		return param, nil
	}

	return "", errors.New("Invalid status value provided")
}
