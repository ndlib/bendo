package server

import (
	"fmt"
	"log"
	"net/http"

	"github.com/julienschmidt/httprouter"
)

// A FixityDB tracks the information the fixity service needs to know what

func (s *RESTServer) EnableTapeUse() {
	log.Println("Enabling Bendo Tape Use")
	s.useTape = true
	s.Items.SetUseStore(true)
}

func (s *RESTServer) DisableTapeUse() {
	log.Println("Disabling Bendo Tape Use")
	s.useTape = false
	s.Items.SetUseStore(false)
}

// SetTapeUseHandler handles requests to PUT /admin/use_tape/:status
func (s *RESTServer) SetTapeUseHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {

	status := ps.ByName("status")

	switch status {
	case "on":
		w.WriteHeader(201)
		// start Enable Process, unless already enabled
		s.EnableTapeUse()
	case "off":
		w.WriteHeader(201)
		// start Disable Process, unless already disabled
		s.DisableTapeUse()
	default:
		w.WriteHeader(503)
		log.Println("PUT /admin/user_tape: unknown parameter ", status)
	}
}

// GetTapeUseHandler handles requests from GET /admin/use_tape
func (s *RESTServer) GetTapeUseHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	switch s.useTape {
	case true:
		fmt.Fprintf(w, "On")
	case false:
		fmt.Fprintf(w, "Off")
	}
}
