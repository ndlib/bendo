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
	fmt.Fprintf(w, "Blob %s/%s\n", id, ps.ByName("bid"))
	fmt.Fprintf(w, "Method: %s\n", r.Method)
	bid, err := strconv.ParseInt(ps.ByName("bid"), 10, 0)
	if err != nil || bid <= 0 {
		fmt.Fprintln(w, err)
		return
	}
	getblob(w, r, id, items.BlobID(bid))
}

func SlotHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	id := ps.ByName("id")
	item, err := Items.Item(id)
	if err != nil {
		fmt.Fprintln(w, err.Error())
	}
	vid, err := strconv.ParseInt(ps.ByName("version"), 10, 0)
	if err != nil || vid <= 0 {
		fmt.Fprintf(w, "Invalid version")
		return
	}
	slot := strings.TrimPrefix(ps.ByName("slot"), "/")
	bid := item.BlobByVersionSlot(items.VersionID(vid), slot)
	if bid == 0 {
		fmt.Fprintf(w, "Cannot resolve version/slot pair")
		return
	}
	getblob(w, r, id, items.BlobID(bid))
}

func getblob(w http.ResponseWriter, r *http.Request, id string, bid items.BlobID) {
	src, err := Items.Blob(id, bid)
	if err != nil {
		fmt.Fprintln(w, err)
		return
	}
	io.Copy(w, src)
}

func ItemHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	id := ps.ByName("id")
	item, err := Items.Item(id)
	if err != nil {
		fmt.Fprintln(w, err.Error())
	}
	enc := json.NewEncoder(w)
	enc.Encode(item)
}

// Wrap the item store in the singleflight so we only ask for metadata once
