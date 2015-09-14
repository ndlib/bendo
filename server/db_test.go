package server

import (
	"testing"
	"time"
)

func TestOldestChecksum(t *testing.T) {
	tx, _ := db.Begin()
	tx.Exec(`INSERT INTO items VALUES
		("qwer", parseTime("2006-01-02", "2014-12-28"), "error", "", now()),
		("abcd", parseTime("2006-01-02", "2015-01-01"), "ok", "", now()),
		("zxcv", parseTime("2006-01-02", "2015-06-01"), "ok", "", now())`)
	tx.Commit()

	cutoff, _ := time.Parse("2006-01-02", "2015-05-01")
	item := OldestChecksum(cutoff)
	if item != "abcd" {
		t.Errorf("Received %s, expected abcd", item)
	}
}
