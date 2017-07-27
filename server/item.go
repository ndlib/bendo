package server

import (
	"encoding/hex"
	"encoding/json"
	"expvar"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"

	"github.com/julienschmidt/httprouter"

	"github.com/ndlib/bendo/items"
	"github.com/ndlib/bendo/store"
)

var (
	nCacheHit  = expvar.NewInt("cache.hit")
	nCacheMiss = expvar.NewInt("cache.miss")
)

// BlobHandler handles requests to GET /blob/:id/:bid
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

// SlotHandler handles requests to GET /item/:id/*slot
//                and requests to HEAD /item/:id/*slot
func (s *RESTServer) SlotHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	id := ps.ByName("id")
	// the star parameter in httprouter returns the leading slash
	slot := ps.ByName("slot")[1:]

	item, err := s.Items.Item(id)

	if err != nil {
		// if item store use disabled, return 503
		if err == items.ErrNoStore {
			w.WriteHeader(503)
			log.Printf("GET/HEAD /item/%s/%s returns 503 - tape disabled", id, slot)
		} else {
			w.WriteHeader(404)
		}
		fmt.Fprintln(w, err.Error())
		return
	}
	// if we have the empty path, reroute to the item metadata handler
	if slot == "" {
		s.ItemHandler(w, r, ps)
		return
	}
	// slot might have a "@nnn" version prefix
	bid := item.BlobByExtendedSlot(slot)
	if bid == 0 {
		w.WriteHeader(404)
		fmt.Fprintf(w, "Invalid Version")
		return
	}
	w.Header().Set("X-Content-Sha256", hex.EncodeToString(item.Blobs[bid-1].SHA256))
	w.Header().Set("Location", fmt.Sprintf("/item/%s/@blob/%d", id, bid))
	s.getblob(w, r, id, items.BlobID(bid))
}

func (s *RESTServer) getblob(w http.ResponseWriter, r *http.Request, id string, bid items.BlobID) {
	key := fmt.Sprintf("%s+%04d", id, bid)
	cacheContents, length, err := s.Cache.Get(key)
	if err != nil {
		w.WriteHeader(500)
		fmt.Fprintln(w, err)
		return
	}
	var src io.Reader
	if cacheContents != nil {
		nCacheHit.Add(1)
		log.Printf("Cache Hit %s", key)
		w.Header().Set("X-Cached", "1")
		defer cacheContents.Close()
		// need to wrap this since Cache.Get returns a ReadAtCloser
		src = store.NewReader(cacheContents)
	} else {
		// cache miss...load from main store, AND put into cache
		nCacheMiss.Add(1)
		log.Printf("Cache Miss %s", key)

		// If Item Store Use disabled, and not in cache, return 503
		if !s.useTape {
			w.WriteHeader(503)
			fmt.Fprintln(w, items.ErrNoStore)
			return
		}

		blobinfo, err := s.Items.BlobInfo(id, bid)
		if err != nil {
			w.WriteHeader(404)
			fmt.Fprintln(w, err)
			return
		}
		length = blobinfo.Size
		// cache this item if it is not too large.
		// doing 1/8th of the cache size is arbitrary.
		// not sure what a good cutoff would be.
		if length < s.Cache.Size()/8 {
			w.Header().Set("X-Cached", "0")
			if r.Method == "GET" {
				go s.copyBlobIntoCache(key, id, bid)
			}
		} else {
			// item is too large to be cached
			w.Header().Set("X-Cached", "2")
		}

		// If this is a GET, retrieve the blob- if it's a HEAD, don't
		if r.Method == "GET" {
			realContents, _, err := s.Items.Blob(id, bid)
			if err != nil {
				w.WriteHeader(404)
				fmt.Fprintln(w, err)
				return
			}
			defer realContents.Close()
			src = realContents
		}
	}
	w.Header().Set("ETag", fmt.Sprintf(`"%d"`, bid))
	w.Header().Set("Content-Length", fmt.Sprintf("%d", length))

	// if it's a GET, copy the data into the response- if it's a HEAD, don't
	if r.Method == "HEAD" {
		return
	}
	n, err := io.Copy(w, src)
	if err != nil {
		log.Printf("getblob (%s,%d) %d,%s", id, bid, n, err.Error())
	}
}

// copyBlobIntoCache copies the given blob of the item id into s's blobcache
// under the given key.
func (s *RESTServer) copyBlobIntoCache(key, id string, bid items.BlobID) {
	var keepcopy bool
	// defer this first so it is the last to run at exit.
	// because cw needs to be Closed() before the Delete().
	// And defered funcs are run LIFO.
	defer func() {
		if !keepcopy {
			s.Cache.Delete(key)
		}
	}()
	cw, err := s.Cache.Put(key)
	if err != nil {
		log.Printf("cache put %s: %s", key, err.Error())
		keepcopy = true // in case someone else added a copy already
		return
	}
	defer cw.Close()
	cr, length, err := s.Items.Blob(id, bid)
	if err != nil {
		log.Printf("cache items get %s: %s", key, err.Error())
		return
	}
	defer cr.Close()
	n, err := io.Copy(cw, cr)
	if err != nil {
		log.Printf("cache copy %s: %s", key, err.Error())
		return
	}
	if n != length {
		log.Printf("cache length mismatch: read %d, expected %d", n, length)
		return
	}
	keepcopy = true
}

// ItemHandler handles requests to GET /item/:id
func (s *RESTServer) ItemHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	id := ps.ByName("id")
	item, err := s.Items.Item(id)
	if err != nil {
		// If Item Store Disable, return a 503
		if err == items.ErrNoStore {
			w.WriteHeader(503)
			log.Printf("GET /item/%s returns 503 - tape disabled", id)
		} else {
			w.WriteHeader(404)
		}
		fmt.Fprintln(w, err.Error())
		return
	}
	vid := item.Versions[len(item.Versions)-1].ID
	w.Header().Set("ETag", fmt.Sprintf(`"%d"`, vid))
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	enc := json.NewEncoder(w)
	enc.Encode(item)
}
