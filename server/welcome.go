package server

import (
	"fmt"
	"net/http"

	"github.com/julienschmidt/httprouter"
)

// WelcomeHandler handles requests from GET /
func WelcomeHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	fmt.Fprintf(w, "Bendo (%s)\n", Version)
}
