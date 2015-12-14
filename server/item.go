package server

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"

	_ "github.com/golang/groupcache/singleflight"
	"github.com/julienschmidt/httprouter"

	"github.com/ndlib/bendo/items"
)

func (s *RESTServer) BlobHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	id := ps.ByName("id")
	bid, err := strconv.ParseInt(ps.ByName("bid"), 10, 0)
	if err != nil || bid <= 0 {
		w.WriteHeader(404)
		fmt.Fprintln(w, err)
		return
	}
	w.Header().Set("ETag", fmt.Sprintf("%d", bid))
	s.getblob(w, r, id, items.BlobID(bid))
}

func (s *RESTServer) SlotHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	id := ps.ByName("id")
	item, err := s.Items.Item(id)
	if err != nil {
		fmt.Fprintln(w, err.Error())
		return
	}
	// see if there is a "@nnn" version present
	// the star parameter in httprouter returns the leading slash
	slot := strings.TrimPrefix(ps.ByName("slot"), "/")
	bid := item.BlobByExtendedSlot(slot)
	if bid == 0 {
		w.WriteHeader(404)
		fmt.Fprintf(w, "Invalid Version")
		return
	}
	w.Header().Set("Location", fmt.Sprintf("/blob/%s/%d", item.ID, bid))
	w.Header().Set("Etag", fmt.Sprintf("%d", bid))
	s.getblob(w, r, id, items.BlobID(bid))
}

func (s *RESTServer) getblob(w http.ResponseWriter, r *http.Request, id string, bid items.BlobID) {
	src, err := s.Items.Blob(id, bid)
	if err != nil {
		w.WriteHeader(404)
		fmt.Fprintln(w, err)
		return
	}
	io.Copy(w, src)
}

func (s *RESTServer) ItemHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	id := ps.ByName("id")
	item, err := s.Items.Item(id)
	if err != nil {
		w.WriteHeader(404)
		fmt.Fprintln(w, err.Error())
		return
	}
	vid := item.Versions[len(item.Versions)-1].ID
	w.Header().Set("ETag", fmt.Sprintf("%d", vid))
	enc := json.NewEncoder(w)
	enc.Encode(item)
}

// Wrap the item store in the singleflight so we only ask for metadata once

/*
// add  commit  button  to  item  info  page?
	<form action="/transaction/{{ .ID }}/commit" method="post">
		<button type="submit">Commit</button>
	</form>
*/
