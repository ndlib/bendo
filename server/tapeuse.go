package server

import (
	"fmt"
	"log"
	"net/http"

	"github.com/julienschmidt/httprouter"
)

// EnableTapeUse turns on the tape use flag. This allows the
// server to access the tape storage.
func (s *RESTServer) EnableTapeUse() {
	log.Println("Enabling Bendo Tape Use")
	s.useTape = true
	s.Items.SetUseStore(true)
}

// DisableTapeUse disables the tape use flag. The server will not
// try to access the tape device while tape use is turned off.
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
		s.EnableTapeUse()
	case "off":
		w.WriteHeader(201)
		s.DisableTapeUse()
	default:
		w.WriteHeader(500)
		fmt.Fprintf(w, "PUT /admin/user_tape: unknown parameter %s", status)
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
