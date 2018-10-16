package server

import (
	"encoding/hex"
	"expvar"
	"fmt"
	"html/template"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"

	"github.com/julienschmidt/httprouter"

	"github.com/ndlib/bendo/items"
	"github.com/ndlib/bendo/store"
)

var (
	nCacheHit  = expvar.NewInt("cache.hit")
	nCacheMiss = expvar.NewInt("cache.miss")
)

type blobDB interface {
	FindBlob(item string, blobid int) (*items.Blob, error)
	// version = 0 means use most recent version
	FindBlobBySlot(item string, version int, slot string) (*items.Blob, error)
	IndexItem(item *items.Item) error
}

// BlobHandler handles requests to GET /blob/:id/:bid
func (s *RESTServer) BlobHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	id := ps.ByName("id")
	binfo, err := s.resolveblob(id, "@blob/"+ps.ByName("bid"))
	if binfo == nil || err != nil {
		w.WriteHeader(404)
		if err != nil {
			fmt.Fprintln(w, err)
		}
		return
	}
	s.getblob(w, r, id, binfo)
}

// SlotHandler handles requests to GET /item/:id/*slot
//                and requests to HEAD /item/:id/*slot
func (s *RESTServer) SlotHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	id := ps.ByName("id")
	// the star parameter in httprouter returns the leading slash
	slot := ps.ByName("slot")[1:]

	// if we have the empty path, reroute to the item metadata handler
	if slot == "" {
		s.ItemHandler(w, r, ps)
		return
	}

	binfo, err := s.resolveblob(id, slot)
	if binfo == nil && err == nil {
		// see if the item needs to be loaded into the database?
		var item *items.Item
		item, err = s.Items.Item(id)
		if item != nil {
			// this needs to be revised. since it will reindex the item
			// whether or not it is already in the database.
			s.BlobDB.IndexItem(item)
			binfo, err = s.resolveblob(id, slot)
		}
	}
	if binfo == nil || err != nil {
		// if item store use disabled, return 503
		if err == items.ErrNoStore {
			w.WriteHeader(503)
			log.Printf("GET/HEAD /item/%s/%s returns 503 - tape disabled", id, slot)
		} else {
			w.WriteHeader(404)
		}
		if err != nil {
			fmt.Fprintln(w, err.Error())
		}
		return
	}
	w.Header().Set("X-Content-Sha256", hex.EncodeToString(binfo.SHA256))
	w.Header().Set("Location", fmt.Sprintf("/item/%s/@blob/%d", id, binfo.ID))
	s.getblob(w, r, id, binfo)
}

// resolveblob tries to resolve the given item+slotpath identifier to a particular
// blob, and returns information for that blob. If there is an error doing the
// resoultion, the error is returned. If the item+slotpath did not resolve to a blob,
// a nil is returned with no error.
//
// Unlike item.BlobByExtendedSlot, this only uses the database to do the
// resolution. Returns nil if the slot couldn't be parsed or if the item/slot
// is not indexed.
func (s *RESTServer) resolveblob(itemID string, slot string) (*items.Blob, error) {
	if slot == "" {
		return nil, nil
	}
	// handle "@blob/nnn" path
	if strings.HasPrefix(slot, "@blob/") {
		// try to parse the blob number
		b, err := strconv.ParseInt(slot[6:], 10, 0)
		if err != nil || b <= 0 {
			return nil, nil
		}
		return s.BlobDB.FindBlob(itemID, int(b))
	}
	if slot[0] != '@' {
		// common case...no version
		return s.BlobDB.FindBlobBySlot(itemID, 0, slot)
	}
	// handle "@nnn/path/to/file" paths
	var err error
	var vid int64
	j := strings.Index(slot, "/")
	if j >= 1 {
		// start from index 1 to skip initial "@"
		vid, err = strconv.ParseInt(slot[1:j], 10, 0)
	}
	// if j was invalid, then vid == 0, so following will catch it
	if err != nil || vid <= 0 {
		return nil, nil
	}
	return s.BlobDB.FindBlobBySlot(itemID, int(vid), slot[j+1:])
}

func (s *RESTServer) getblob(w http.ResponseWriter, r *http.Request, id string, binfo *items.Blob) {
	key := fmt.Sprintf("%s+%04d", id, binfo.ID)
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

		length = binfo.Size
		// cache this item if it is not too large.
		// doing 1/8th of the cache size is arbitrary.
		// not sure what a good cutoff would be.
		// (remember maxsize == 0 means infinite)
		cacheMaxSize := s.Cache.MaxSize()
		if cacheMaxSize == 0 || length < cacheMaxSize/8 {
			w.Header().Set("X-Cached", "0")
			if r.Method == "GET" {
				go s.copyBlobIntoCache(key, id, binfo)
			}
		} else {
			// item is too large to be cached
			w.Header().Set("X-Cached", "2")
		}

		// If this is a GET, retrieve the blob- if it's a HEAD, don't
		if r.Method == "GET" {
			realContents, _, err := s.Items.BlobByBlob(id, binfo)
			if err != nil {
				w.WriteHeader(404)
				fmt.Fprintln(w, err)
				return
			}
			defer realContents.Close()
			src = realContents
		}
	}
	w.Header().Set("ETag", fmt.Sprintf(`"%d"`, binfo.ID))
	w.Header().Set("Content-Length", fmt.Sprintf("%d", length))

	// if it's a GET, copy the data into the response- if it's a HEAD, don't
	if r.Method == "HEAD" {
		return
	}
	n, err := io.Copy(w, src)
	if err != nil {
		log.Printf("getblob (%s,%d) %d,%s", id, binfo.ID, n, err.Error())
	}
}

// copyBlobIntoCache copies the given blob of the item id into s's blobcache
// under the given key.
func (s *RESTServer) copyBlobIntoCache(key, id string, binfo *items.Blob) {
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
	cr, length, err := s.Items.BlobByBlob(id, binfo)
	if err != nil {
		log.Printf("cache items get %s: %s", key, err)
		return
	}
	defer cr.Close()
	n, err := io.Copy(cw, cr)
	if err != nil {
		log.Printf("cache copy %s: %s", key, err)
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
	writeHTMLorJSON(w, r, itemTemplate, item)
}

func minus1(a interface{}) int {
	// the template calls this with something having type BlobID, so we make a
	// have type interface{}, and type switch to get the right value
	switch v := a.(type) {
	case int:
		return v - 1
	case items.BlobID:
		return int(v) - 1
	}
	return 0
}

var (
	itemfns = template.FuncMap{
		"minus1": minus1,
	}

	itemTemplate = template.Must(template.New("items").Funcs(itemfns).Parse(`
<html><head><style>
tbody tr:nth-child(even) { background-color: #eeeeee; }
</style></head><body>
<h1>Item {{ .ID }}</h1>
<dl>
<dt>Created</dt><dd>{{ (index .Versions 0).SaveDate }}</dd>
<dt>MaxBundle</dt><dd>{{ .MaxBundle }}</dd>
</dl>
{{ $blobs := .Blobs }}
{{ $id := .ID }}
{{ with index .Versions (len .Versions | minus1) }}
	<h2>Version {{ .ID }}</h2>
	<dl>
	<dt>SaveDate</dt><dd>{{ .SaveDate }}</dd>
	<dt>Creator</dt><dd>{{ .Creator }}</dd>
	<dt>Note</dt><dd>{{ .Note }}</dd>
	</dl>
	<table><thead><tr>
		<th>Bundle</th>
		<th>Blob</th>
		<th>Size</th>
		<th>Date</th>
		<th>MimeType</th>
		<th>MD5</th>
		<th>SHA256</th>
		<th>Filename</th>
	</tr></thead><tbody>
	{{ range $key, $value := .Slots }}
		<tr>
		{{ with index $blobs ($value | minus1) }}
			<td>{{ .Bundle }}</td>
			<td><a href="/item/{{ $id }}/@blob/{{ $value }}">{{ $value }}</a></td>
			<td>{{ .Size }}</td>
			<td>{{ .SaveDate }}</td>
			<td>{{ .MimeType }}</td>
			<td>{{ printf "%x" .MD5 }}</td>
			<td>{{ printf "%x" .SHA256 }}</td>
		{{ end }}
		<td><a href="/item/{{ $id }}/{{ $key }}">{{ $key }}</a></td>
		</tr>
	{{ end }}
	</tbody></table>
{{ end }}
</body></html>`))
)
