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
		{"GetFixityById", "qwe", now},
	}

	for _, tab := range table {
		t.Logf("%v", tab)
		switch tab.command {
		case "NextFixity":
			id := qc.NextFixity(tab.when)
			if id != tab.id {
				t.Errorf("Received %s, expected %s", id, tab.id)
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
		case "GetFixityById":
			thisId := qc.GetFixityById(tab.id)
			if thisId == nil {
				t.Error("GetFixityById returned nil ")
			} else if thisId.id != tab.id {
				t.Errorf("Received %s, expected %s", thisId.id, tab.id)
			}
		}
	}
}
