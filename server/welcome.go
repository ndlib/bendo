package server

import (
	"fmt"
	"net/http"

	"github.com/julienschmidt/httprouter"
)

func WelcomeHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	fmt.Fprintf(w, "Bendo (%s)\n", Version)
}
