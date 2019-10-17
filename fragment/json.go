package fragment

import (
	"encoding/json"
	"log"

	"github.com/ndlib/bendo/store"
)

// A JSONStore wraps a Store and provides a store which serializes its items as
// JSON instead of using streams. It does not cache the results of
// serialization/deserialization. Since it deals with interface{} instead of
// readers and writers, a JSONStore does not match the store.Store interface.
type JSONStore struct {
	store.Store
}

// NewJSON creates a new JSONStore using the provided store for its storage.
func NewJSON(s store.Store) JSONStore {
	return JSONStore{s}
}

// Open the item having the given key and unserialize it into value.
func (js JSONStore) Open(key string, value interface{}) error {
	r, _, err := js.Store.Open(key)
	if err != nil {
		return err
	}
	dec := json.NewDecoder(store.NewReader(r))
	err = dec.Decode(value)
	err2 := r.Close()
	if err == nil {
		err = err2
	} else {
		log.Println(key, err2)
	}
	return err
}

// Create is a synonym for Save(). (Why do we have this?)
// This is here to override js.Store.Create()
func (js JSONStore) Create(key string, value interface{}) error {
	return js.Save(key, value)
}

// Save the value val under the given key. It will delete any existing value
// for the key before doing the save.
func (js JSONStore) Save(key string, value interface{}) error {
	err := js.Delete(key)
	if err != nil {
		return err
	}
	w, err := js.Store.Create(key)
	if err != nil {
		return err
	}
	enc := json.NewEncoder(w)
	err = enc.Encode(value)
	err2 := w.Close()
	if err == nil {
		err = err2
	} else {
		log.Println(key, err2)
	}
	return err
}
