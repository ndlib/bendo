package server

import (
	"bytes"
	"database/sql"
	"encoding/gob"

	"github.com/ndlib/bendo/items"
)

// Keep our metadata cache in a database
// implements the item.ItemCache interface.
type mdcache struct {
	db *sql.DB
}

var initdb = `
CREATE TABLE IF NOT EXISTS items (
	id VARCHAR(64) PRIMARY KEY,
	gob BLOB,
	last_checksum DATETIME,
	checksum_status STRING)
`

func (c *mdcache) Lookup(id string) *items.Item {
	var binary []byte
	err := c.db.QueryRow("SELECT gob FROM items WHERE id = ?", id).Scan(&binary)
	if err != nil {
		return nil
	}
	dec := gob.NewDecoder(bytes.NewReader(binary))
	var item *items.Item
	err = dec.Decode(&item)
	if err != nil {
		return nil
	}
	return item
}

func (c *mdcache) Set(id string, item *items.Item) {
	var w = new(bytes.Buffer)
	enc := gob.NewEncoder(w)
	err := enc.Encode(item)
	if err != nil {
		return
	}
	// need to check for error??
	// this might error if the item is already in the database.
	// so in that case we would then need to do an UPDATE
	c.db.Exec("INSERT INTO items (id, gob) VALUES (?, ?)", id, w.Bytes())
	// UPDATE items SET gob = ? WHERE id = ?
}
