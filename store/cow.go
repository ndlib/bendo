package store

import (
	"encoding/json"
	"io"
	"log"
	"net/http"
	"time"
)

// COW implements a Copy-on-Write store multiplexing between an external bendo
// host and a local store. The local store is used for writes and is the first
// checked for reads. Anything not in the local store will then be looked up in
// the external one. Hence, the local store appears to have everything in the
// external one, but all changes are local and nothing is ever written to the
// external store.
//
// Note: While simple, this is not the most efficient way of implementing a COW
// interface. To read a single file, this store will first copy an entire
// bundle from the external host, and then read out only the one file which was
// requested.
//
// I consider this approach a proof of concept. The "for real" approach will
// be to alter Item to make the appropriate calls into the remote bendo server.
type COW struct {
	local  Store        // where we write into
	client *http.Client // to reuse keep-alive connections
	host   string       // "http://hostname:port"
	token  string       // the access token for the bendo server
}

// NewCOW creates a COW store using the given local store and the given external
// host. Host should be in form "http://hostname:port". The optional token is
// the user token to use in requests to the server.
func NewCOW(local Store, host, token string) *COW {
	return &COW{
		local:  local,
		host:   host,
		token:  token,
		client: &http.Client{Timeout: 300 * time.Second},
	}
}

// List returns a channel enumerating everything in this store. It will
// combine the items in both the local store and the remote store.
func (c *COW) List() <-chan string {
	out := make(chan string)
	go mergechan(out, c.remoteList(), c.local.List())
	return out
}

// ListPrefix returns all items with a specified prefix. It will combine
// the items found from both stores.
func (c *COW) ListPrefix(prefix string) ([]string, error) {
	loc, err := c.local.ListPrefix(prefix)
	if err != nil {
		return loc, err
	}
	rmt, err := c.remoteListPrefix(prefix)
	if err != nil {
		return nil, err
	}
	return mergelist(loc, rmt), nil
}

// Open will return an item for reading. If the item is only in the remote
// store, it will first be copied into the local store.
func (c *COW) Open(key string) (ReadAtCloser, int64, error) {
	rac, n, err := c.local.Open(key)
	if err == nil {
		return rac, n, err
	}
	// on error, see if it is on remote
	rc, err := c.remoteOpen(key)
	if err != nil {
		return nil, 0, err
	}
	defer rc.Close()
	// copy rc into the local
	w, err := c.local.Create(key)
	if err != nil {
		return nil, 0, err
	}
	_, err = io.Copy(w, rc)
	w.Close()
	if err != nil {
		return nil, 0, err
	}
	return c.local.Open(key)
}

// Create will make a new item in the local store. It is acceptable to
// make an item in the local store with the same name as an item in the
// remote store. The local item will shadow the remote one.
func (c *COW) Create(key string) (io.WriteCloser, error) {
	return c.local.Create(key)
}

// Delete `key`. Items will only be deleted from the local store. Trying to
// delete a remote item will result in a nop (but not an error). Note: If there
// were a local item shadowing a remote item, doing a delete will delete the
// local one, but the remote one will still exist. So deleting an item may not
// remote it from the store. This semantics may cause problems with users
// expecting a standard store.
func (c *COW) Delete(key string) error {
	return c.local.Delete(key)
}

// merge in1 and in2 into c. Removes any duplicate entries. Closes c
// when both in1 and in2 are closed.
func mergechan(c chan<- string, in1, in2 <-chan string) {
	dedup := make(map[string]struct{})
	for in1 != nil || in2 != nil {
		var n string
		var ok bool
		select {
		case n, ok = <-in1:
			if !ok {
				in1 = nil
				continue
			}
		case n, ok = <-in2:
			if !ok {
				in2 = nil
				continue
			}
		}
		_, ok = dedup[n]
		if !ok {
			dedup[n] = struct{}{}
			c <- n
		}
	}
	close(c)
}

// merge two lists. should remove duplicate entries, but doesn't at the moment.
func mergelist(a, b []string) []string {
	result := append(a, b...)
	// remove duplicate entries here?
	return result
}

// the remote interface should morally be a ROStore. Except our Open returns a
// Reader instead of a ReadAtCloser, so we implement it as private methods here.

func (c *COW) remoteList() <-chan string {
	out := make(chan string)
	go func() {
		defer close(out)
		resp, err := c.get(c.host + "/bundle/list")
		if err != nil {
			log.Println(err.Error())
			return
		}
		defer resp.Body.Close()
		if resp.StatusCode != 200 {
			log.Println("COW remoteList received %d", resp.StatusCode)
			return
		}
		dec := json.NewDecoder(resp.Body)
		// the list may be long, so parse it as a stream
		// read open bracket
		_, err = dec.Token()
		if err != nil {
			return
		}
		var s string
		// while the array contains values
		for dec.More() {
			// decode an array value
			err = dec.Decode(&s)
			if err != nil {
				return
			}
			out <- s
		}
		// read closing bracket
		dec.Token()
	}()
	return out
}

func (c *COW) remoteListPrefix(prefix string) ([]string, error) {
	resp, err := c.get(c.host + "/bundle/list/" + prefix)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return nil, err
	}
	var s []string
	dec := json.NewDecoder(resp.Body)
	err = dec.Decode(&s)
	if err != nil && err != io.EOF {
		return nil, err
	}
	return s, nil
}

func (c *COW) remoteOpen(key string) (io.ReadCloser, error) {
	resp, err := c.get(c.host + "/bundle/open/" + key)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != 200 {
		resp.Body.Close()
		return nil, err
	}
	return resp.Body, nil
}

// get a url, like client.Get(), but also add the token header if needed
func (c *COW) get(url string) (*http.Response, error) {
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}
	if c.token != "" {
		req.Header.Add("X-Api-Key", c.token)
	}
	return c.client.Do(req)
}
