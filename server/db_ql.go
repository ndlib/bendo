package server

import (
	"database/sql"
	"encoding/json"
	"log"
	"time"

	_ "github.com/cznic/ql/driver"

	"github.com/ndlib/bendo/items"
)

// This file implements various caches which use the QL
// embedded database. These are intended to be used only
// in development.

type qlCache struct {
	db *sql.DB
}

var _ items.ItemCache = &qlCache{}
var _ FixityDB = &qlCache{}

// Only keep enough information as needed by the fixity checking routine.
const qlItemInit = `
	CREATE TABLE IF NOT EXISTS items (
		id string,
		created time,
		modified time,
		size int,
		value blob
	);
	CREATE INDEX IF NOT EXISTS itemid ON items (id);
	CREATE INDEX IF NOT EXISTS itemmodified ON items (modified);
`

const qlFixityInit = `
	CREATE TABLE IF NOT EXISTS fixity (
		id string,
		scheduled_time time,
		status string,
		notes string
	);
	CREATE INDEX IF NOT EXISTS fixityid ON fixity (id);
	CREATE INDEX IF NOT EXISTS fixitytime ON fixity (scheduled_time);
	CREATE INDEX IF NOT EXISTS fixitystatus ON fixity (status);
`

// NewQlDatabase makes a QL database cache. filename is
// the name of the file to save the database to. The filname "memory" means to keep everything in memory.
func NewQlCache(filename string) *qlCache {
	var db *sql.DB
	var err error
	if filename == "memory" {
		db, err = sql.Open("ql-mem", "mem.db")
	} else {
		db, err = sql.Open("ql", filename)
	}
	if err == nil {
		_, err = performExec(db, qlItemInit)
	}
	if err == nil {
		_, err = performExec(db, qlFixityInit)
	}
	if err != nil {
		log.Printf("Open QL: %s", err.Error())
		return nil
	}
	return &qlCache{db: db}
}

func (qc *qlCache) Lookup(id string) *items.Item {
	const dbLookup = `SELECT value FROM items WHERE id == ?1 LIMIT 1`

	var value string
	err := qc.db.QueryRow(dbLookup, id).Scan(&value)
	if err != nil {
		if err != sql.ErrNoRows {
			log.Printf("Item Cache QL: %s", err.Error())
		}
		return nil
	}
	var item = new(items.Item)
	err = json.Unmarshal([]byte(value), item)
	if err != nil {
		return nil
	}
	return item
}

func (qc *qlCache) Set(id string, item *items.Item) {
	const dbUpdate = `UPDATE items SET created = ?2, modified = ?3, size = ?4, value = ?5 WHERE id == ?1`
	const dbInsert = `INSERT INTO items VALUES (?1, ?2, ?3, ?4, ?5)`
	var created, modified time.Time
	var size int64
	for i := range item.Blobs {
		size += item.Blobs[i].Size
	}
	if len(item.Versions) > 0 {
		created = item.Versions[0].SaveDate
		modified = item.Versions[len(item.Versions)-1].SaveDate
	}
	value, err := json.Marshal(item)
	if err != nil {
		log.Printf("Item Cache QL: %s", err.Error())
		return
	}
	result, err := performExec(qc.db, dbUpdate, id, created, modified, size, value)
	if err != nil {
		log.Printf("Item Cache QL: %s", err.Error())
		return
	}
	nrows, err := result.RowsAffected()
	if err != nil {
		log.Printf("Item Cache QL: %s", err.Error())
		return
	}
	if nrows == 0 {
		// record didn't exist. create it
		_, err = performExec(qc.db, dbInsert, id, created, modified, size, value)
		if err != nil {
			log.Printf("Item Cache QL: %s", err.Error())
		}
	}
}

func (qc *qlCache) NextFixity(cutoff time.Time) string {
	const query = `
		SELECT id, scheduled_time
		FROM fixity
		WHERE status == "scheduled" AND scheduled_time <= ?1
		ORDER BY scheduled_time
		LIMIT 1;`

	var id string
	var when time.Time
	err := qc.db.QueryRow(query, cutoff).Scan(&id, &when)
	if err == sql.ErrNoRows {
		// no next record
		return ""
	} else if err != nil {
		log.Println("nextfixity QL", err.Error())
		return ""
	}
	return id
}

func (qc *qlCache) UpdateFixity(id string, status string, notes string) error {
	const query = `
		UPDATE fixity
		SET status = ?2, notes = ?3
		WHERE id() in
			(SELECT id from
				(SELECT id() as id, scheduled_time
				FROM fixity
				WHERE id == ?1 and status == "scheduled"
				ORDER BY scheduled_time
				LIMIT 1))`

	result, err := performExec(qc.db, query, id, status, notes)
	if err != nil {
		return err
	}
	nrows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if nrows == 0 {
		// record didn't exist. create it
		const newquery = `INSERT INTO fixity VALUES (?1,?2,?3,?4)`

		_, err = performExec(qc.db, newquery, id, time.Now(), status, notes)
	}
	return err
}

func (qc *qlCache) SetCheck(id string, when time.Time) error {
	const query = `INSERT INTO fixity VALUES (?1,?2,?3,?4)`

	_, err := performExec(qc.db, query, id, when, "scheduled", "")
	return err
}

func (qc *qlCache) LookupCheck(id string) (time.Time, error) {
	const query = `
		SELECT scheduled_time
		FROM fixity
		WHERE id == ?1 AND status == "scheduled"
		ORDER BY scheduled_time ASC
		LIMIT 1`

	var when time.Time
	err := qc.db.QueryRow(query, id).Scan(&when)
	if err == sql.ErrNoRows {
		err = nil
	}
	return when, err
}

func performExec(db *sql.DB, query string, args ...interface{}) (sql.Result, error) {
	tx, err := db.Begin()
	if err != nil {
		return nil, err
	}
	var result sql.Result
	result, err = tx.Exec(query, args...)
	if err != nil {
		_ = tx.Rollback()
		return nil, err
	}
	err = tx.Commit()
	return result, err
}
