package server

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"log"
	"strings"
	"time"

	"github.com/BurntSushi/migration"
	_ "github.com/cznic/ql/driver" // load the ql sql driver

	"github.com/ndlib/bendo/items"
)

// This file implements various caches which use the QL
// embedded database. These are intended to be used only
// in development.

// A QlCache implements an item.ItemCache and a FixityDB interface
// backed by a QL database.
type QlCache struct {
	db *sql.DB
}

var _ items.ItemCache = &QlCache{}
var _ FixityDB = &QlCache{}

// List of migrations to perform. Add new ones to the end.
// DO NOT change the order of items already in this list.
var qlMigrations = []migration.Migrator{
	qlschema1,
	qlschema2,
}

// adapt schema versioning for QL

var qlVersioning = dbVersion{
	GetSQL:    `SELECT max(version) FROM migration_version`,
	SetSQL:    `INSERT INTO migration_version VALUES (?1, now())`,
	CreateSQL: `CREATE TABLE migration_version (version int, applied time)`,
}

// NewQlCache makes a QL database cache. filename is
// the name of the file to save the database to. The filname beginning with
// "mem--" means to keep everything in memory.
func NewQlCache(filename string) (*QlCache, error) {
	var driver = "ql"
	if strings.HasPrefix(filename, "mem--") {
		driver = "ql-mem"
		// ql-mem uses filename to distinugish databases in memory.
		// so pass it through unchanged.
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
	return &QlCache{db: db}, nil
}

// Lookup returns an item from the cache if it exists. If there
// is nothing under that key, it will return nil.
func (qc *QlCache) Lookup(item string) *items.Item {
	const dbLookup = `SELECT value FROM items WHERE item == ?1 LIMIT 1`

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

// Set adds the given item to the cache under the key item.
func (qc *QlCache) Set(item string, thisItem *items.Item) {
	const dbUpdate = `UPDATE items SET created = ?2, modified = ?3, size = ?4, value = ?5 WHERE item == ?1`
	const dbInsert = `INSERT INTO items (item, created, modified, size, value) VALUES (?1, ?2, ?3, ?4, ?5)`
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

// NextFixity will return the item id of the earliest scheduled fixity check
// that is before the cutoff time. If there is no such record 0 is returned.
func (qc *QlCache) NextFixity(cutoff time.Time) int64 {
	const query = `
		SELECT id(), scheduled_time
		FROM fixity
		WHERE status == "scheduled" AND scheduled_time <= ?1
		ORDER BY scheduled_time
		LIMIT 1`

	var id int64
	var when time.Time
	err := qc.db.QueryRow(query, cutoff).Scan(&id, &when)
	if err != nil && err != sql.ErrNoRows {
		log.Println("nextfixity QL", err.Error())
	}
	return id
}

// GetFixityById
func (qc *QlCache) GetFixity(id int64) *Fixity {
	const query = `
		SELECT id(), item, scheduled_time, status, notes
		FROM fixity
		WHERE id() == ?1
		LIMIT 1`

	var record Fixity
	err := qc.db.QueryRow(query, id).Scan(&record.ID, &record.Item, &record.ScheduledTime, &record.Status, &record.Notes)
	if err == sql.ErrNoRows {
		// no next record
		return nil
	} else if err != nil {
		log.Println("GetFixity", err)
		return nil
	}
	return &record
}

// SearchFixity
func (qc *QlCache) SearchFixity(start, end time.Time, item string, status string) []*Fixity {
	query := buildQLQuery(start, end, item, status)
	var result []*Fixity

	rows, err := qc.db.Query(query, start, end, item, status)
	if err == sql.ErrNoRows {
		return nil
	} else if err != nil {
		log.Println("GetFixity QL Query:", err)
		return nil
	}
	defer rows.Close()

	for rows.Next() {
		var record = new(Fixity)
		scanErr := rows.Scan(&record.ID, &record.Item, &record.ScheduledTime, &record.Status, &record.Notes)
		if scanErr != nil {
			log.Println("GetFixity QL Scan", err)
			continue
		}
		result = append(result, record)
	}
	return result
}

// construct an return an sql query, using the parameters passed
func buildQLQuery(start, end time.Time, item string, status string) string {
	var query bytes.Buffer
	// the ql driver has positional parameters, so we build the query and the exec
	// will pass every parameter. Then the driver can choose the ones it needs.
	query.WriteString("SELECT id(), item, scheduled_time, status, notes FROM fixity")

	conjunction := " WHERE "

	if !start.IsZero() {
		query.WriteString(conjunction + "scheduled_time >= ?1")
		conjunction = " AND "
	}
	if !end.IsZero() {
		query.WriteString(conjunction + "scheduled_time <= ?2")
		conjunction = " AND "
	}
	if item != "" {
		query.WriteString(conjunction + "item = ?3")
		conjunction = " AND "
	}
	if status != "" {
		query.WriteString(conjunction + "status = ?4")
	}
	query.WriteString(" ORDER BY scheduled_time")
	return query.String()
}

// UpdateFixity updates or creates the given fixity record. The record is created if
// ID is == 0. Otherwise the given record is updated so long as
// the record in the database has status "scheduled".
func (qc *QlCache) UpdateFixity(record Fixity) (int64, error) {
	if record.Status == "" {
		record.Status = "scheduled"
	}

	if record.ID == 0 {
		// new record
		const command = `INSERT INTO fixity (item, scheduled_time, status, notes) VALUES (?1,?2,?3,?4)`

		result, err := performExec(qc.db, command, record.Item, record.ScheduledTime, record.Status, record.Notes)
		var id int64
		if err == nil {
			id, _ = result.LastInsertId()
		}
		return id, err
	}

	// try to update existing record
	const command = `
		UPDATE fixity
		SET item = ?2, scheduled_time = ?3, status = ?4, notes = ?5
		WHERE id() == ?1 AND status == "scheduled"`

	_, err := performExec(qc.db, command, record.ID, record.Item, record.ScheduledTime, record.Status, record.Notes)
	return record.ID, err
}

//
func (qc *QlCache) DeleteFixity(id int64) error {
	const query = `
		DELETE FROM fixity
		WHERE id() == ?1 and status == "scheduled"`

	_, err := performExec(qc.db, query, id)
	return err
}

// LookupCheck returns the earliest scheduled fixity check for the given
// item. If there is no scheduled fixity check, it returns the zero time.
func (qc *QlCache) LookupCheck(item string) (time.Time, error) {
	const query = `
		SELECT scheduled_time
		FROM fixity
		WHERE item == ?1 AND status == "scheduled"
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
	// make the field names mirror the mysql names.
	// since ql has id() built in, we use that for the autoincrement field
	var s = []string{
		`ALTER TABLE items ADD item string`,
		`UPDATE items item = id`,
		`ALTER TABLE items DROP COLUMN id`,
		`ALTER TABLE fixity ADD item string`,
		`UPDATE fixity item = id`,
		`ALTER TABLE fixity DROP COLUMN id`,
		`CREATE INDEX fixityid ON fixity (item)`,
		`CREATE INDEX itemid ON items (item)`,
	}

	return execlist(tx, s)
}
