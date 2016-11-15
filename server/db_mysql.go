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
	mysqlschema2,
	mysqlschema3,
}

// Adapt the schema versioning for MySQL

var mysqlVersioning = dbVersion{
	GetSQL:    `SELECT max(version) FROM migration_version`,
	SetSQL:    `INSERT INTO migration_version (version, applied) VALUES (?, now())`,
	CreateSQL: `CREATE TABLE migration_version (version INTEGER, applied datetime)`,
}

// NewMysqlCache connects to a MySQL database and returns an item satisifying
// both the ItemCache and FixityDB interfaces.
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
	const dbLookup = `SELECT value FROM items WHERE item = ? LIMIT 1`

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
	var thisItem = new(items.Item)
	err = json.Unmarshal([]byte(value), thisItem)
	if err != nil {
		log.Printf("Item Cache: error in lookup: %s", err.Error())
		return nil
	}
	return thisItem
}

func (ms *msqlCache) Set(id string, thisItem *items.Item) {
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
		log.Printf("Item Cache: %s", err.Error())
		return
	}
	stmt := `INSERT INTO items (item, created, modified, size, value) VALUES (?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE created=?, modified=?, size=?, value=?`

	_, err = ms.db.Exec(stmt, id, created, modified, size, value, created, modified, size, value)
	if err != nil {
		log.Printf("Item Cache: %s", err.Error())
		return
	}
}

func (ms *msqlCache) NextFixity(cutoff time.Time) string {
	const query = `
		SELECT item
		FROM fixity
		WHERE status = "scheduled" AND scheduled_time <= ?
		ORDER BY scheduled_time
		LIMIT 1`

	var item string
	err := ms.db.QueryRow(query, cutoff).Scan(&item)
	if err == sql.ErrNoRows {
		// no next record
		return ""
	} else if err != nil {
		log.Println("nextfixity", err.Error())
		return ""
	}
	return item
}

func (ms *msqlCache) UpdateFixity(item string, status string, notes string) error {
	const query = `
		UPDATE fixity
		SET status = ?, notes = ?
		WHERE item = ? and status = "scheduled"
		ORDER BY scheduled_time
		LIMIT 1`
	result, err := ms.db.Exec(query, status, notes, item)
	if err != nil {
		return err
	}
	nrows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if nrows == 0 {
		// record didn't exist. create it
		const newquery = `INSERT INTO fixity ( item, scheduled_time, status, notes) VALUES (?,?,?,?)`

		_, err = ms.db.Exec(newquery, item, time.Now(), status, notes)
	}
	return err
}

func (ms *msqlCache) SetCheck(item string, when time.Time) error {
	const query = `INSERT INTO fixity (item, scheduled_time, status, notes) VALUES (?,?,?,?)`

	_, err := ms.db.Exec(query, item, when, "scheduled", "")
	return err
}

func (ms *msqlCache) LookupCheck(item string) (time.Time, error) {
	const query = `
		SELECT scheduled_time
		FROM fixity
		WHERE item = ? AND status = "scheduled"
		ORDER BY scheduled_time
		LIMIT 1`

	var when mysql.NullTime
	err := ms.db.QueryRow(query, item).Scan(&when)
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

func mysqlschema2(tx migration.LimitedTx) error {
	var s = []string{
		`ALTER TABLE items CHANGE COLUMN id item varchar(255)`,
		`ALTER TABLE fixity CHANGE COLUMN id item varchar(255)`,
		`ALTER TABLE fixity ADD COLUMN id int PRIMARY KEY AUTO_INCREMENT FIRST`,
		`ALTER TABLE items ADD COLUMN id int PRIMARY KEY AUTO_INCREMENT FIRST`,
	}

	return execlist(tx, s)
}

func mysqlschema3(tx migration.LimitedTx) error {
	var s = []string{
		`CREATE TEMPORARY TABLE mult_ids AS SELECT item FROM items GROUP BY item HAVING count(*) > 1`,
		`DELETE FROM items WHERE item IN (SELECT * from mult_ids)`,
		`ALTER TABLE items ADD UNIQUE INDEX items_item (item), CHANGE COLUMN value value LONGTEXT, CHANGE COLUMN size size BIGINT`,
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
