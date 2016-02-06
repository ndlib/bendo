package store

import (
	"encoding/json"
	"io"
	"log"
	"net/http"
)

// COW implements a Copy-on-Write method. It takes an external bendo host, and
// will read data from it. It will cache the external data in a local Store,
// as well as saving any new files to the local store.
//
// When used with the package item, this will work at the level of Bundles,
// which while a simple interface, is not the most efficient way of pulling
// content from an external bendo server. Since, to read a single file, the COW
// will first copy an entire bundle from the external host, only to read out
// the one file which was requested. Advantages to our approach: we don't need
// to change any code in the Items package.
//
// I consider this approach a proof of concept. The "for real" approach will
// be to alter Item to make the appropriate calls into the remote bendo server.
type COW struct {
	host   string       // "http://hostname:port"
	local  Store        // where we write into
	client *http.Client // to reuse keep-alive connections
}

func (c *COW) List() <-chan string {
	out := make(chan string)
	go mergechan(out, c.remoteList(), c.local.List())
	return out
}

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

func (c *COW) Create(key string) (io.WriteCloser, error) {
	return c.local.Create(key)
}

// Delete `key`. Cannot delete things on remote server. Trying to do that will
// result in a nop.
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
		resp, err := c.client.Get(c.host + "/bundle/list")
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
		// the list may be long, parse it as a stream
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
	resp, err := c.client.Get(c.host + "/bundle/list/" + prefix)
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
	resp, err := c.client.Get(c.host + "/bundle/open/" + key)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != 200 {
		resp.Body.Close()
		return nil, err
	}
	return resp.Body, nil
}
