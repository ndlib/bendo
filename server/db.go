package server

import (
	"log"

	"github.com/BurntSushi/migration"
)

// we need to adapt the migration version functions to work with MySQL and QL
// This code is slightly modified from github.com/BurntSushi/migration

type dbVersion struct {
	// SQL to get the version of this db, returns one row and one column
	GetSQL string
	// SQL to insert a new version of this db. takes one parameter, the new
	// version
	SetSQL string
	// the SQL to create the version table for this db
	CreateSQL string
}

func (d dbVersion) Get(tx migration.LimitedTx) (int, error) {
	v, err := d.get(tx)
	if err != nil {
		// we assume error means there is no migration table
		log.Println(err.Error())
		return 0, nil
	}
	return v, nil
}

func (d dbVersion) Set(tx migration.LimitedTx, version int) error {
	if err := d.set(tx, version); err != nil {
		if err := d.createTable(tx); err != nil {
			return err
		}
		return d.set(tx, version)
	}
	return nil
}

func (d dbVersion) get(tx migration.LimitedTx) (int, error) {
	var version int
	r := tx.QueryRow(d.GetSQL)
	if err := r.Scan(&version); err != nil {
		return 0, err
	}
	return version, nil
}

func (d dbVersion) set(tx migration.LimitedTx, version int) error {
	_, err := tx.Exec(d.SetSQL, version)
	return err
}

func (d dbVersion) createTable(tx migration.LimitedTx) error {
	_, err := tx.Exec(d.CreateSQL)
	if err == nil {
		err = d.set(tx, 0)
	}
	return err
}
