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

func BlobHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	id := ps.ByName("id")
	bid, err := strconv.ParseInt(ps.ByName("bid"), 10, 0)
	if err != nil || bid <= 0 {
		w.WriteHeader(404)
		fmt.Fprintln(w, err)
		return
	}
	w.Header().Set("ETag", fmt.Sprintf("%d", bid))
	getblob(w, r, id, items.BlobID(bid))
}

func SlotHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	id := ps.ByName("id")
	item, err := Items.Item(id)
	if err != nil {
		fmt.Fprintln(w, err.Error())
		return
	}
	version := ps.ByName("version")
	var vid int64
	if version == "latest" {
		vid = int64(item.Versions[len(item.Versions)-1].ID)
	} else {
		vid, err = strconv.ParseInt(ps.ByName("version"), 10, 0)
		if err != nil || vid <= 0 {
			w.WriteHeader(404)
			fmt.Fprintf(w, "Invalid version")
			return
		}
	}
	// the star parameter in httprouter returns the leading slash
	slot := strings.TrimPrefix(ps.ByName("slot"), "/")
	bid := item.BlobByVersionSlot(items.VersionID(vid), slot)
	if bid == 0 {
		w.WriteHeader(404)
		fmt.Fprintf(w, "Cannot resolve (%d, %s) pair", vid, slot)
		return
	}
	w.Header().Set("Location", fmt.Sprintf("/blob/%s/%d", item.ID, bid))
	w.Header().Set("Etag", fmt.Sprintf("%d", bid))
	getblob(w, r, id, items.BlobID(bid))
}

func getblob(w http.ResponseWriter, r *http.Request, id string, bid items.BlobID) {
	src, err := Items.Blob(id, bid)
	if err != nil {
		w.WriteHeader(404)
		fmt.Fprintln(w, err)
		return
	}
	io.Copy(w, src)
}

func ItemHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	id := ps.ByName("id")
	item, err := Items.Item(id)
	if err != nil {
		w.WriteHeader(404)
		fmt.Fprintln(w, err.Error())
		return
	}
	vid := int64(item.Versions[len(item.Versions)-1].ID)
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
