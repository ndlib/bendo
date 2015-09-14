package server

import (
	"database/sql"
	"encoding/json"
	"log"
	"time"

	_ "github.com/cznic/ql/driver"
)

// Only keep enough information as needed by the fixity checking routine.
const dbInitQL = `
	CREATE TABLE IF NOT EXISTS items (
		name string,
		checksumDate time,
		checksumStatus string,
		data string,
		updated time
	);
	CREATE INDEX IF NOT EXISTS itemname ON items (name);
	CREATE INDEX IF NOT EXISTS itemdate ON items (checksumDate);
	CREATE INDEX IF NOT EXISTS itemstatus ON items (checksumStatus)
`

var (
	db *sql.DB
)

func openDatabase(filename string) error {
	var err error
	if filename == "memory" {
		db, err = sql.Open("ql-mem", "mem.db")
	} else {
		db, err = sql.Open("ql", filename)
	}
	if err != nil {
		return err
	}
	return performExec(dbInitQL)
}

// OldestChecksum returns the item id of the item last having a checksum
// done the furthest in the past. However, it will not return an item which
// has had a checksum since the cutoff time. If there are no items to checksum,
// the empty string is returned. It will also not return anything in an
// "error" state.
func OldestChecksum(cutoff time.Time) string {
	// need to select checksumDate since otherwise QL does not propagate
	// it to the ORDER BY clause, making it unable to do the sort.
	const dbOldestQL = `
		SELECT name, checksumDate
		FROM items
		WHERE checksumDate < ?1 AND checksumStatus != "error"
		ORDER BY checksumDate
		LIMIT 1;`

	var result string
	var cksumStamp time.Time
	err := db.QueryRow(dbOldestQL, cutoff).Scan(&result, &cksumStamp)
	if err != nil {
		// either there is no such record or there was some other error
		// in any case, there is nothing for us to do
		log.Println(err.Error())
		return ""
	}
	return result
}

// SyncItems will try to synchronize the database with the item list in the
// store (which is the canonical source of information). Items which are
// missing in the database will be added, and items in the database missing
// from the store are removed from the database.
func SyncItems() {
	const dbItemAddQL = `INSERT INTO items VALUES (?1, now(), "", ?2, now())`
	const dbItemUpdateQL = `UPDATE items
		data = ?2,
		updated = now(),
	WHERE name == ?1`

	// take our cutoff back an hour in case the database server time is
	// not synced with ours. Also do this before we start loading items
	// since we do not know how long that will take.
	updateTime := time.Now().Add(-1 * time.Hour)
	for id := range Items.List() {
		// we force a reload of the JSON
		item, err := Items.Item(id)
		if err != nil {
			log.Printf(err.Error())
			continue
		}
		text, _ := json.Marshal(item)
		if lookupItemRecord(id) {
			performExec(dbItemUpdateQL, id, text)
		} else {
			performExec(dbItemAddQL, id, text)
		}
	}

	// now remove anything which has not been updated
	const dbRemoveOldQL = `DELETE FROM items WHERE updated < ?1`
	err := performExec(dbRemoveOldQL, updateTime)
	if err != nil {
		log.Println(err.Error())
	}
}

func lookupItemRecord(id string) bool {
	const dbItemSearch = `SELECT data FROM items WHERE name == ?1`
	var text string

	err := db.QueryRow(dbItemSearch, id).Scan(&text)
	if err != nil {
		log.Println(err.Error())
		return false
	}
	return true
}

func performExec(query string, args ...interface{}) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	_, err = tx.Exec(query, args...)
	if err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}
