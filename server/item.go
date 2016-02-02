package server

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"

	"github.com/julienschmidt/httprouter"

	"github.com/ndlib/bendo/items"
	"github.com/ndlib/bendo/store"
)

// GET /blob/:id/:bid
func (s *RESTServer) BlobHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	id := ps.ByName("id")
	bid, err := strconv.ParseInt(ps.ByName("bid"), 10, 0)
	if err != nil || bid <= 0 {
		w.WriteHeader(404)
		fmt.Fprintln(w, err)
		return
	}
	s.getblob(w, r, id, items.BlobID(bid))
}

// GET /item/:id/*slot
func (s *RESTServer) SlotHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	id := ps.ByName("id")
	item, err := s.Items.Item(id)
	if err != nil {
		fmt.Fprintln(w, err.Error())
		return
	}
	// the star parameter in httprouter returns the leading slash
	slot := strings.TrimPrefix(ps.ByName("slot"), "/")
	// slot might have a "@nnn" version prefix
	bid := item.BlobByExtendedSlot(slot)
	if bid == 0 {
		w.WriteHeader(404)
		fmt.Fprintf(w, "Invalid Version")
		return
	}
	w.Header().Set("Location", fmt.Sprintf("/blob/%s/%d", id, bid))
	s.getblob(w, r, id, items.BlobID(bid))
}

func (s *RESTServer) getblob(w http.ResponseWriter, r *http.Request, id string, bid items.BlobID) {
	key := fmt.Sprintf("%s+%04d", id, bid)
	cacheContents, _, err := s.Cache.Get(key)
	if err != nil {
		w.WriteHeader(500)
		fmt.Fprintln(w, err)
		return
	}
	var src io.Reader
	if cacheContents != nil {
		log.Printf("Cache Hit %s", key)
		defer cacheContents.Close()
		// need to wrap this since Cache.Get returns a ReadAtCloser
		src = store.NewReader(cacheContents)
	} else {
		// cache miss...load from main store, AND put into cache
		log.Printf("Cache Miss %s", key)
		realContents, err := s.Items.Blob(id, bid)
		if err != nil {
			w.WriteHeader(404)
			fmt.Fprintln(w, err)
			return
		}
		defer realContents.Close()
		src = realContents
		// copy into the cache in the background
		// TODO(dbrower): don't cache if blob is too large, say >= 50 GB?
		go func() {
			cw, err := s.Cache.Put(key)
			if err != nil {
				log.Printf("cache put %s: %s", key, err.Error())
				return
			}
			defer cw.Close()
			cr, err := s.Items.Blob(id, bid)
			if err != nil {
				log.Printf("cache items get %s: %s", key, err.Error())
				return
			}
			defer cr.Close()
			io.Copy(cw, cr)
		}()
	}
	w.Header().Set("ETag", fmt.Sprintf("%d", bid))
	n, err := io.Copy(w, src)
	if err != nil {
		log.Printf("getblob (%s,%d) %d,%s", id, bid, n, err.Error())
	}
}

// GET /item/:id
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
