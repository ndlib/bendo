package fragment

import (
	"encoding/json"

	"github.com/ndlib/bendo/store"
)

// Wraps a Store and lets items be serialized as JSON instead of being streams.
// Does not cache the results of serialization/deserialization.
// By necessity, the result does not match the store.Store interface.
type JSONStore struct {
	store.Store
}

func NewJSON(s store.Store) JSONStore {
	return JSONStore{s}
}

func (js JSONStore) Open(key string, val interface{}) error {
	r, _, err := js.Store.Open(key)
	if err != nil {
		return err
	}
	defer r.Close()
	dec := json.NewDecoder(store.NewReader(r))
	return dec.Decode(val)
}

// Synomym for Save().
func (js JSONStore) Create(key string, val interface{}) error {
	// this is here to overried js.Store.Create()
	return js.Save(key, val)
}

// Save the value val under the given key. It will delete any existing value
// for the key before doing the save.
func (js JSONStore) Save(key string, val interface{}) error {
	err := js.Delete(key)
	if err != nil {
		return err
	}
	w, err := js.Store.Create(key)
	if err != nil {
		return err
	}
	defer w.Close()
	enc := json.NewEncoder(w)
	return enc.Encode(val)
}
