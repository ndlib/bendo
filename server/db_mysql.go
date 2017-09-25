package server

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"errors"
	"log"
	"strings"
	"time"

	// no _ in import mysql since we need mysql.NullTime
	"github.com/BurntSushi/migration"
	"github.com/go-sql-driver/mysql"

	"github.com/ndlib/bendo/items"
)

// This file contains code implementing various caching interfaces to use
// MySQL as a storage medium.

// MsqlCache implements the items.ItemCache interface and the FixityDB interface
// using MySQL as the backing store.
type MsqlCache struct {
	db *sql.DB
}

var _ items.ItemCache = &MsqlCache{}
var _ FixityDB = &MsqlCache{}

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
func NewMysqlCache(dial string) (*MsqlCache, error) {
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
	return &MsqlCache{db: db}, nil
}

// Lookup returns a cached Item, if one exists in the database.
// Otherwise it returns nil.
func (ms *MsqlCache) Lookup(id string) *items.Item {
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

// Set adds the given item to the cache under the key id.
func (ms *MsqlCache) Set(id string, thisItem *items.Item) {
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

// NextFixity returns the earliest scheduled fixity record
// that is before the cutoff time. If there is no such record
// it returns the empty string.
func (ms *MsqlCache) NextFixity(cutoff time.Time) string {
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

// GetFixityById
func (ms *MsqlCache) GetFixityById(id string) *Fixity {
	const query = `
                SELECT  id, item, scheduled_time, status, notes
                FROM fixity
                WHERE id = ?
                LIMIT 1`

	var thisFixity = new(Fixity)
	var thisWhen mysql.NullTime

	err := ms.db.QueryRow(query, id).Scan(&thisFixity.Id, &thisFixity.Item, &thisWhen, &thisFixity.Status, &thisFixity.Notes)
	if err == sql.ErrNoRows {
		// no next record
		return nil
	} else if err != nil {
		log.Println("GetFixtyByID  MySQL queryrow", err.Error())
		return nil
	}

	// Handle for nil time value
	if thisWhen.Valid {
		thisFixity.Scheduled_time = thisWhen.Time
	}

	log.Println("id= ", thisFixity.Id, "scheduled_time= ", thisFixity.Scheduled_time, "status= ", thisFixity.Status, "notes= ", thisFixity.Notes)
	return thisFixity
}

// GetFixity
func (ms *MsqlCache) GetFixity(start string, end string, item string, status string) []*Fixity {
	query := buildQuery(start, end, item, status)
	log.Println("GET /fixity query= ", query)
	var thisWhen mysql.NullTime
	results := make([]*Fixity, 0)

	rows, err := ms.db.Query(query)
	if err == sql.ErrNoRows {
		// no next record
		return nil
	} else if err != nil {
		log.Println("GetFixity Query MySQL", err.Error())
		return nil
	}
	defer rows.Close()

	for rows.Next() {
		var thisFixity = new(Fixity)
		scanErr := rows.Scan(&thisFixity.Id, &thisFixity.Item, &thisWhen, &thisFixity.Status, &thisFixity.Notes)
		if scanErr != nil {
			log.Println("GetFixity Scan MySQL", err.Error())
			return nil
		}
		// Handle for nil time value
		if thisWhen.Valid {
			thisFixity.Scheduled_time = thisWhen.Time
		}
		log.Println("id= ", thisFixity.Id, "item =", thisFixity.Item, "scheduled_time", thisFixity.Scheduled_time, "status= ", thisFixity.Status, "notes= ", thisFixity.Notes)
		results = append(results, thisFixity)
	}

	return results
}

func (ms *MsqlCache) PostFixity(item string) (int, error)  {
        var err error
        fixity_results := ms.GetFixity("*", "*", item, "scheduled")
        if len(fixity_results) > 0 {
                err = ms.DeleteFixity(fixity_results[0].Id)
                if err != nil {
                        return 500, err
                }
        }

        err = ms.UpdateFixity(item, "scheduled", "")
        if err != nil {
                       return 500, err
        }
        return 200, nil
}

func (qc *MsqlCache) PutFixity(id string) ( int, error)  {
        fixity := qc.GetFixityById(id)
        if  fixity == nil {
                return 404, errors.New("No fixity check found for ID")
        }
        // Record Exists- Update it.
       const query = ` UPDATE fixity SET scheduled_time = ? WHERE id = ?`

        _, err := performExec(qc.db, query, time.Now(), id)
        if err != nil {
                return 500, err
        }

        return 200, nil
}

// construct an return an sql query, using the parameters passed
func buildQuery(start string, end string, item string, status string) string {
	var query bytes.Buffer
	query.WriteString("SELECT  id, item, scheduled_time, status, notes FROM fixity")

	params := []string{"start", "end", "item", "status"}

	conjunction := " WHERE "

	for _, param := range params {

		switch param {
		case "start":
			if start != "*" {
				startQuery := []string{conjunction, "scheduled_time >= '", start, "'"}
				query.WriteString(strings.Join(startQuery, ""))
			} else {
				continue
			}
		case "end":
			if end != "*" {
				endQuery := []string{conjunction, "scheduled_time <= '", end, "'"}
				query.WriteString(strings.Join(endQuery, ""))
			} else {
				continue
			}
		case "item":
			if item != "" {
				itemQuery := []string{conjunction, "item = '", item, "'"}
				query.WriteString(strings.Join(itemQuery, ""))
			} else {
				continue
			}
		case "status":
			if status != "" {
				statusQuery := []string{conjunction, "status = '", status, "'"}
				query.WriteString(strings.Join(statusQuery, ""))
			} else {
				continue
			}
		}

		conjunction = " AND "
	}

	query.WriteString(" ORDER BY scheduled_time")
	return query.String()
}

// UpdateFixity updates the earliest scheduled fixity record for
// the given item. If there is no such fixity record, it will create one.
func (ms *MsqlCache) UpdateFixity(item string, status string, notes string) error {
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

func (ms *MsqlCache) DeleteFixity(id string) error {
        const query = `DELETE FROM fixity WHERE id = ?`

        result, err := performExec(ms.db, query, id)
        if err != nil {
                return err
        }
        _, err = result.RowsAffected()
        if err != nil {
                return err
        }
        return nil
}

// SetCheck adds a fixity record for the given item. The created fixity
// record will have the status of "scheduled".
func (ms *MsqlCache) SetCheck(item string, when time.Time) error {
	const query = `INSERT INTO fixity (item, scheduled_time, status, notes) VALUES (?,?,?,?)`

	_, err := ms.db.Exec(query, item, when, "scheduled", "")
	return err
}

// LookupCheck will return the time of the earliest scheduled fixity
// check for the given item. If there is no pending fixity check for
// the item, it returns the zero time.
func (ms *MsqlCache) LookupCheck(item string) (time.Time, error) {
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
