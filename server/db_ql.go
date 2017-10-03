package server

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"errors"
	"log"
	"strconv"
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
// the name of the file to save the database to. The filname "memory" means to keep everything in memory.
func NewQlCache(filename string) (*QlCache, error) {
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
// that is before the cutoff time. If there is no such record, the
// empty string is returned.
func (qc *QlCache) NextFixity(cutoff time.Time) string {
	const query = `
		SELECT item, scheduled_time
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

// GetFixityById
func (qc *QlCache) GetFixityById(id string) *Fixity {
	const query = `
		SELECT id(), item, scheduled_time, status, notes
		FROM fixity
		WHERE id() == ?1
		LIMIT 1`

	var thisFixity = new(Fixity)

	intId, _ := strconv.Atoi(id)

	err := qc.db.QueryRow(query, intId).Scan(&thisFixity.Id, &thisFixity.Item, &thisFixity.Scheduled_time, &thisFixity.Status, &thisFixity.Notes)
	if err == sql.ErrNoRows {
		// no next record
		log.Println("GetFixityById retruns NoRows")
		return nil
	} else if err != nil {
		log.Println("GetFixityById QL", err.Error())
		return nil
	}
	log.Println("id= ", thisFixity.Id, "item= ", thisFixity.Item, "scheduled_time= ", thisFixity.Scheduled_time, "status= ", thisFixity.Status, "notes= ", thisFixity.Notes)
	return thisFixity
}

// GetFixity
func (qc *QlCache) GetFixity(start string, end string, item string, status string) []*Fixity {
	query := buildQLQuery(start, end, item, status)
	log.Println("GET /fixity query= ", query)
	var fixityResults []*Fixity

	rows, err := qc.db.Query(query)
	if err == sql.ErrNoRows {
		// no next record
		return nil
	} else if err != nil {
		log.Println("GetFixity QL Query:", err.Error())
		return nil
	}
	defer rows.Close()

	for rows.Next() {
		var thisFixity = new(Fixity)
		scanErr := rows.Scan(&thisFixity.Id, &thisFixity.Item, &thisFixity.Scheduled_time, &thisFixity.Status, &thisFixity.Notes)
		if scanErr != nil {
			log.Println("GetFixity QL Scan", err.Error())
			return nil
		}
		log.Println("id= ", thisFixity.Id, "item= ", thisFixity.Item, "scheduled_time= ", thisFixity.Item, thisFixity.Scheduled_time, "status= ", thisFixity.Status, "notes= ", thisFixity.Notes)
		fixityResults = append(fixityResults, thisFixity)
	}

	return fixityResults
}

// POST /fixity/:item
func (qc *QlCache) ScheduleFixityForItem(item string) error {
	err := qc.UpdateFixity(item, "scheduled", "", time.Now())
	if err != nil {
		return err
	}
	return nil
}

//PUT /fixity/:id
func (qc *QlCache) PutFixity(id string) error {
	fixity := qc.GetFixityById(id)
	if fixity == nil {
		return errors.New("No fixity check found for ID")
	}
	// Record Exists- Update it.
	const query = ` UPDATE fixity SET scheduled_time = ?1 WHERE id() == ?2`

	intId, _ := strconv.Atoi(id)
	_, err := performExec(qc.db, query, time.Now(), intId)
	if err != nil {
		return err
	}

	return nil
}

// construct an return an sql query, using the parameters passed
func buildQLQuery(start string, end string, item string, status string) string {
	var query bytes.Buffer
	query.WriteString("SELECT  id(), item, scheduled_time, status, notes FROM fixity")

	params := []string{"start", "end", "item", "status"}

	conjunction := " WHERE "

	for _, param := range params {

		switch param {
		case "start":
			if start == "*" {
				continue
			} else {
				startQuery := []string{conjunction, "scheduled_time >= \"", start, "\""}
				query.WriteString(strings.Join(startQuery, ""))
				continue
			}

		case "end":
			if start == "*" {
				continue
			} else {
				endQuery := []string{conjunction, "scheduled_time <= \"", end, "\""}
				query.WriteString(strings.Join(endQuery, ""))
			}
		case "item":
			if item == "" {
				continue
			} else {
				itemQuery := []string{conjunction, "item == \"", item, "\""}
				query.WriteString(strings.Join(itemQuery, ""))
			}
		case "status":
			if status == "" {
				continue
			} else {
				statusQuery := []string{conjunction, "status == \"", status, "\""}
				query.WriteString(strings.Join(statusQuery, ""))
			}
		}

		conjunction = " AND "
	}

	query.WriteString(" ORDER BY scheduled_time")
	return query.String()
}

// UpdateFixity will update the earliest scheduled fixity record for the given item.
// If there is no such record, one will be created.
func (qc *QlCache) UpdateFixity(item string, status string, notes string, scheduled_time time.Time) error {
	queryParts := []string{"UPDATE fixity",
		"SET status = ?2, notes = ?3, scheduled_time = ?4",
		"SET status = ?2, notes = ?3",
		`WHERE id() in
			(SELECT id from
				(SELECT id() as id, scheduled_time
				FROM fixity
				WHERE item == ?1 and status == "scheduled"
				ORDER BY scheduled_time
				LIMIT 1))`}

	queryWithTime := []string{queryParts[0], queryParts[1], queryParts[3]}
	querySansTime := []string{queryParts[0], queryParts[2], queryParts[3]}

	var err error
	var result sql.Result

	if scheduled_time == time.Unix(0, 0) {
		result, err = performExec(qc.db, strings.Join(querySansTime, " "), item, status, notes)
	} else {
		result, err = performExec(qc.db, strings.Join(queryWithTime, " "), item, status, notes, scheduled_time)
	}

	if err != nil {
		return err
	}
	nrows, err := result.RowsAffected()
	if nrows == 0 {
		// record didn't exist. create it
		const newquery = `INSERT INTO fixity (item, scheduled_time, status, notes) VALUES (?1,?2,?3,?4)`

		_, err = performExec(qc.db, newquery, item, time.Now(), status, notes)
	}
	return err
}

//
func (qc *QlCache) DeleteFixity(id string) error {
	const query = `
                DELETE FROM fixity
                WHERE id() == ?1 and status == "scheduled"`

	intId, _ := strconv.Atoi(id)

	_, err := performExec(qc.db, query, intId)
	if err != nil {
		return err
	}
	return nil
}

// SetCheck adds a new fixity record for the given item. The new
// record will have the status of "scheduled".
func (qc *QlCache) SetCheck(item string, when time.Time) error {
	const query = `INSERT INTO fixity (item, scheduled_time, status, notes) VALUES (?1,?2,?3,?4)`

	_, err := performExec(qc.db, query, item, when, "scheduled", "")
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
