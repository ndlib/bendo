// +build integration

package server

import (
	"math"
	"os"
	"testing"
	"time"

	"github.com/ndlib/bendo/items"
)

var dialmysql string

func init() {
	// check for external config for mysql, default to /test
	dialmysql = os.Getenv("MYSQL_CONNECTION")
	if dialmysql == "" {
		dialmysql = "/test"
	}
}

func TestMySQLItemCache(t *testing.T) {
	qc, err := NewMysqlCache(dialmysql)
	if err != nil {
		t.Fatalf("Received %s", err.Error())
	}

	testItem := &items.Item{
		Versions: []*items.Version{
			{SaveDate: time.Now()},
		},
	}
	qc.Set("qwe", testItem)
	result := qc.Lookup("qwe")
	// should do deep equal. we are fudging it for now
	if result == nil {
		t.Errorf("Received nil, expected non-nil")
	}
}

func TestMySQLFixity(t *testing.T) {
	qc, err := NewMysqlCache(dialmysql)
	if err != nil {
		t.Fatalf("Received %s", err.Error())
	}
	now := time.Now()
	nowPlusHour := now.Add(time.Hour)
	var table = []struct {
		command string
		id      string
		when    time.Time
	}{
		{"NextFixity", "", time.Time{}},
		{"SetCheck", "qwe", now},
		{"SetCheck", "qwe", nowPlusHour},
		{"LookupCheck", "qwe", now},
		{"LookupCheck", "zxc", time.Time{}},
		{"LookupCheck", "qwe", now},
		{"UpdateFixity", "zxc", now},
		{"NextFixity", "qwe", nowPlusHour},
		{"LookupCheck", "zxc", time.Time{}},
		{"FixityTests", "qwe", now},
		{"UpdateFixity", "qwe", now},
		{"PostAndPutFixity", "zzb", nowPlusHour},
	}

	for _, tab := range table {
		t.Logf("%v", tab)
		switch tab.command {
		case "NextFixity":
			id := qc.NextFixity(tab.when)
			if id != tab.id {
				t.Errorf("Received %s, expected %s", id, tab.id)
			}
		case "PostAndPutFixity":
			err := qc.ScheduleFixityForItem(tab.id)
			if err != nil {
				t.Errorf("ScheduleFixityForItem MYSQL returned error")
				continue
			}
			postedRecord := qc.GetFixity("*", "*", tab.id, "scheduled")
			if len(postedRecord) == 0 {
				t.Errorf("PostAndPutFixity:GetFixity MYSQL returned 0 length")
			}
			err = qc.PutFixity(postedRecord[0].Id)
			if err != nil {
				t.Errorf("PutFixity MYSQL returned error", err.Error())
				continue
			}
		case "SetCheck":
			err := qc.SetCheck(tab.id, tab.when)
			if err != nil {
				t.Errorf("error %s", err.Error())
			}
		case "UpdateFixity":
			err := qc.UpdateFixity(tab.id, "ok", "", nowPlusHour)
			if err != nil {
				t.Errorf("error %s", err.Error())
			}
			err = qc.UpdateFixity(tab.id, "ok", "", time.Unix(0, 0))
			if err != nil {
				t.Errorf("error %s", err.Error())
			}
		case "LookupCheck":
			when, err := qc.LookupCheck(tab.id)
			if err != nil {
				t.Errorf("error %s", err.Error())
			} else if math.Abs(when.Sub(tab.when).Seconds()) >= 1 {
				t.Errorf("Received %v, expected %v", when, tab.when)
			}
		case "FixityTests":
			record := qc.GetFixity("*", "*", tab.id, "scheduled")
			if len(record) == 0 {
				t.Errorf("GetFixity QL returned 0 length")
			}
			// use id returned from GetFixity Test to test GetFixtyById, DeleteFixity
			recordById := qc.GetFixityById(record[0].Id)
			if recordById == nil {
				t.Errorf("GetFixityById QL returned nil")
			} else if recordById.Id != record[0].Id {
				t.Errorf("GetFixityById QL id mismatch ")
			}
			err := qc.DeleteFixity(record[0].Id)
			if err != nil {
				t.Errorf("DeleteFixity QL returned error")
			}
		}
	}
}
