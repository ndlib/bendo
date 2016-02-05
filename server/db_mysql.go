package server

import (
	"database/sql"
	"encoding/json"
	"log"
	"time"

	// no _ in import mysql since we need mysql.NullTime
	"github.com/BurntSushi/migration"
	"github.com/go-sql-driver/mysql"

	"github.com/ndlib/bendo/items"
)

// This file contains code implementing various caching interfaces to use
// MySQL as a storage medium.

// implements the items.ItemCache interface and the FixityDB interface
// using MySQL as the backing store.
type msqlCache struct {
	db *sql.DB
}

var _ items.ItemCache = &msqlCache{}
var _ FixityDB = &msqlCache{}

// List of migrations to perform. Add new ones to the end.
// DO NOT change the order of items already in this list.
var mysqlMigrations = []migration.Migrator{
	mysqlschema1,
}

// Adapt the schema versioning for MySQL

var mysqlVersioning = dbVersion{
	GetSQL:    `SELECT max(version) FROM migration_version`,
	SetSQL:    `INSERT INTO migration_version (version, applied) VALUES (?, now())`,
	CreateSQL: `CREATE TABLE migration_version (version INTEGER, applied datetime)`,
}

func NewMysqlCache(dial string) (*msqlCache, error) {
	db, err := migration.OpenWith(
		"mysql",
		dial,
		mysqlMigrations,
		mysqlVersioning.Get,
		mysqlVersioning.Set)
	if err != nil {
		log.Printf("Open Mysql: %s", err.Error())
		return nil, err
	}
	return &msqlCache{db: db}, nil
}

func (ms *msqlCache) Lookup(id string) *items.Item {
	const dbLookup = `SELECT value FROM items WHERE id = ? LIMIT 1`

	var value string
	err := ms.db.QueryRow(dbLookup, id).Scan(&value)
	if err != nil {
		if err != sql.ErrNoRows {
			// some kind of error...treat it as a miss
			log.Printf("Item Cache: %s", err.Error())
		}
		return nil
	}
	// unserialize the json string
	var item = new(items.Item)
	err = json.Unmarshal([]byte(value), item)
	if err != nil {
		return nil
	}
	return item
}

func (ms *msqlCache) Set(id string, item *items.Item) {
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
		log.Printf("Item Cache: %s", err.Error())
		return
	}
	result, err := ms.db.Exec(`UPDATE items SET created = ?, modified = ?, size = ?, value = ? WHERE id = ?`, created, modified, size, value, id)
	if err != nil {
		log.Printf("Item Cache: %s", err.Error())
		return
	}
	nrows, err := result.RowsAffected()
	if err != nil {
		log.Printf("Item Cache: %s", err.Error())
		return
	}
	if nrows == 0 {
		// record didn't exist. create it
		ms.db.Exec(`INSERT INTO items VALUES (?, ?, ?, ?, ?)`, id, created, modified, size, value)
	}
}

func (ms *msqlCache) NextFixity(cutoff time.Time) string {
	const query = `
		SELECT id
		FROM fixity
		WHERE status = "scheduled" AND scheduled_time <= ?
		ORDER BY scheduled_time
		LIMIT 1`

	var id string
	err := ms.db.QueryRow(query, cutoff).Scan(&id)
	if err == sql.ErrNoRows {
		// no next record
		return ""
	} else if err != nil {
		log.Println("nextfixity", err.Error())
		return ""
	}
	return id
}

func (ms *msqlCache) UpdateFixity(id string, status string, notes string) error {
	const query = `
		UPDATE fixity
		SET status = ?, notes = ?
		WHERE id = ? and status = "scheduled"
		ORDER BY scheduled_time
		LIMIT 1`
	result, err := ms.db.Exec(query, status, notes, id)
	if err != nil {
		return err
	}
	nrows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if nrows == 0 {
		// record didn't exist. create it
		const newquery = `INSERT INTO fixity VALUES (?,?,?,?)`

		_, err = ms.db.Exec(newquery, id, time.Now(), status, notes)
	}
	return err
}

func (ms *msqlCache) SetCheck(id string, when time.Time) error {
	const query = `INSERT INTO fixity VALUES (?,?,?,?)`

	_, err := ms.db.Exec(query, id, when, "scheduled", "")
	return err
}

func (ms *msqlCache) LookupCheck(id string) (time.Time, error) {
	const query = `
		SELECT scheduled_time
		FROM fixity
		WHERE id = ? AND status = "scheduled"
		ORDER BY scheduled_time
		LIMIT 1`

	var when mysql.NullTime
	err := ms.db.QueryRow(query, id).Scan(&when)
	if err == sql.ErrNoRows {
		err = nil
	}
	if when.Valid {
		return when.Time, err
	}
	return time.Time{}, err
}

// database migrations. each one is a go function. Add them to the
// list mysqlMigrations at top of this file for them to be run.

func mysqlschema1(tx migration.LimitedTx) error {
	var s = []string{
		`CREATE TABLE IF NOT EXISTS items (
		id varchar(255),
		created datetime,
		modified datetime,
		size int,
		value text)`,

		`CREATE TABLE IF NOT EXISTS fixity (
		id varchar(255),
		scheduled_time datetime,
		status varchar(32),
		notes text)`,
	}
	return execlist(tx, s)
}

// execlist exec's each item in the list, return if there is an error.
// Used to work around mysql driver not handling compound exec statements.
func execlist(tx migration.LimitedTx, stms []string) error {
	var err error
	for _, s := range stms {
		_, err = tx.Exec(s)
		if err != nil {
			break
		}
	}
	return err
}
