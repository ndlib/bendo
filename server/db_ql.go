package server

import (
	"database/sql"
	"encoding/json"
	"log"
	"time"

	"github.com/BurntSushi/migration"
	_ "github.com/cznic/ql/driver" // load the ql sql driver

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

// List of migrations to perform. Add new ones to the end.
// DO NOT change the order of items already in this list.
var qlMigrations = []migration.Migrator{
	qlschema1,
}

// adapt schema versioning for QL

var qlVersioning = dbVersion{
	GetSQL:    `SELECT max(version) FROM migration_version`,
	SetSQL:    `INSERT INTO migration_version VALUES (?1, now())`,
	CreateSQL: `CREATE TABLE migration_version (version int, applied time)`,
}

// NewQlCache makes a QL database cache. filename is
// the name of the file to save the database to. The filname "memory" means to keep everything in memory.
func NewQlCache(filename string) (*qlCache, error) {
	var driver = "ql"
	if filename == "memory" {
		driver = "ql-mem"
		filename = "mem.db"
	}
	db, err := migration.OpenWith(
		driver,
		filename,
		qlMigrations,
		qlVersioning.Get,
		qlVersioning.Set)
	if err != nil {
		log.Printf("Open QL: %s", err.Error())
		return nil, err
	}
	return &qlCache{db: db}, nil
}

func (qc *qlCache) Lookup(item string) *items.Item {
	const dbLookup = `SELECT value FROM items WHERE id == ?1 LIMIT 1`

	var value string
	err := qc.db.QueryRow(dbLookup, item).Scan(&value)
	if err != nil {
		if err != sql.ErrNoRows {
			log.Printf("Item Cache QL: %s", err.Error())
		}
		return nil
	}
	var thisItem = new(items.Item)
	err = json.Unmarshal([]byte(value), thisItem)
	if err != nil {
		return nil
	}
	return thisItem
}

func (qc *qlCache) Set(item string, thisItem *items.Item) {
	const dbUpdate = `UPDATE items SET created = ?2, modified = ?3, size = ?4, value = ?5 WHERE id == ?1`
	const dbInsert = `INSERT INTO items (id, created, modified, size, value) VALUES (?1, ?2, ?3, ?4, ?5)`
	var created, modified time.Time
	var size int64
	for i := range thisItem.Blobs {
		size += thisItem.Blobs[i].Size
	}
	if len(thisItem.Versions) > 0 {
		created = thisItem.Versions[0].SaveDate
		modified = thisItem.Versions[len(thisItem.Versions)-1].SaveDate
	}
	value, err := json.Marshal(thisItem)
	if err != nil {
		log.Printf("Item Cache QL: %s", err.Error())
		return
	}
	result, err := performExec(qc.db, dbUpdate, item, created, modified, size, value)
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
		_, err = performExec(qc.db, dbInsert, item, created, modified, size, value)
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
		LIMIT 1`

	var item string
	var when time.Time
	err := qc.db.QueryRow(query, cutoff).Scan(&item, &when)
	if err == sql.ErrNoRows {
		// no next record
		return ""
	} else if err != nil {
		log.Println("nextfixity QL", err.Error())
		return ""
	}
	return item
}

func (qc *qlCache) UpdateFixity(item string, status string, notes string) error {
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

	result, err := performExec(qc.db, query, item, status, notes)
	if err != nil {
		return err
	}
	nrows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if nrows == 0 {
		// record didn't exist. create it
		const newquery = `INSERT INTO fixity (id, scheduled_time, status, notes) VALUES (?1,?2,?3,?4)`

		_, err = performExec(qc.db, newquery, item, time.Now(), status, notes)
	}
	return err
}

func (qc *qlCache) SetCheck(item string, when time.Time) error {
	const query = `INSERT INTO fixity (id, scheduled_time, status, notes) VALUES (?1,?2,?3,?4)`

	_, err := performExec(qc.db, query, item, when, "scheduled", "")
	return err
}

func (qc *qlCache) LookupCheck(item string) (time.Time, error) {
	const query = `
		SELECT scheduled_time
		FROM fixity
		WHERE id == ?1 AND status == "scheduled"
		ORDER BY scheduled_time ASC
		LIMIT 1`

	var when time.Time
	err := qc.db.QueryRow(query, item).Scan(&when)
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

// QL Migrations. Each migration gets its own go function.
// Add function to list qlmigrations at top of file.

func qlschema1(tx migration.LimitedTx) error {
	const s = `
	CREATE TABLE IF NOT EXISTS items (
		id string,
		created time,
		modified time,
		size int,
		value blob
	);
	CREATE INDEX IF NOT EXISTS itemid ON items (id);
	CREATE INDEX IF NOT EXISTS itemmodified ON items (modified);
	CREATE TABLE IF NOT EXISTS fixity (
		id string,
		scheduled_time time,
		status string,
		notes string
	);
	CREATE INDEX IF NOT EXISTS fixityid ON fixity (id);
	CREATE INDEX IF NOT EXISTS fixitytime ON fixity (scheduled_time);
	CREATE INDEX IF NOT EXISTS fixitystatus ON fixity (status);`

	_, err := tx.Exec(s)
	return err
}

func qlschema2(tx migration.LimitedTx) error {
	var s = []string  { 
	`ALTER TABLE items MODIFY COLUMN id item varchar(255)`,
	`ALTER TABLE fixity MODIFY COLUMN id item varchar(255)`,
	`ALTER TABLE fixity ADD COLUMN id int PRIMARY KEY AUTO_INCREMENT FIRST`,
	`ALTER TABLE items ADD COLUMN id int PRIMARY KEY AUTO_INCREMENT FIRST`,
	`DROP INDEX fixityid`,
	`DROP INDEX itemid`,
	`CREATE INDEX fixityid ON fixity (item)`,
	`CREATE INDEX itemid ON items (item)`,
	}

	return execlist(tx, s)
}
