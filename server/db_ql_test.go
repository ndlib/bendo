package server

import (
	"testing"
	"time"

	"github.com/ndlib/bendo/items"
)

func TestQlItemCache(t *testing.T) {
	qc, err := NewQlCache("memory")
	if err != nil {
		t.Fatalf("Received %s", err.Error())
	}

	testItem := new(items.Item)
	qc.Set("qwe", testItem)
	result := qc.Lookup("qwe")
	// should do deep equal. we are fudging it for now
	if result == nil {
		t.Errorf("Received nil, expected non-nil")
	}
}

func TestQlFixity(t *testing.T) {
	qc, err := NewQlCache("memory")
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
		{"UpdateFixity", "qwe", now},
		{"LookupCheck", "qwe", nowPlusHour},
		{"UpdateFixity", "zxc", now},
		{"NextFixity", "", now},
		{"NextFixity", "qwe", nowPlusHour},
		{"LookupCheck", "qwe", nowPlusHour},
		{"LookupCheck", "zxc", time.Time{}},
		{"FixityTests", "qwe", now},
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
			_, err := qc.PostFixity(tab.id)
			if err != nil {
				t.Errorf("PostFixity QL returned error")
				continue
			}
			postedRecord := qc.GetFixity("*","*",tab.id,"scheduled")
			if len(postedRecord) == 0 {
				t.Errorf("PostAndPutFixity:GetFixity QL returned 0 length")
			}
			_, err = qc.PutFixity(postedRecord[0].Id)
			if err != nil {
				t.Errorf("PutFixity QL returned error", err.Error())
				continue
			}
		case "SetCheck":
			err := qc.SetCheck(tab.id, tab.when)
			if err != nil {
				t.Errorf("error %s", err.Error())
			}
		case "UpdateFixity":
			err := qc.UpdateFixity(tab.id, "ok", "")
			if err != nil {
				t.Errorf("error %s", err.Error())
			}
		case "LookupCheck":
			when, err := qc.LookupCheck(tab.id)
			if err != nil {
				t.Errorf("error %s", err.Error())
			} else if when != tab.when {
				t.Errorf("Received %v, expected %v", when, tab.when)
			}
		case "FixityTests":
			record := qc.GetFixity("*","*",tab.id,"scheduled")
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
