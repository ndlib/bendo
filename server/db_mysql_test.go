// +build integration

package server

import (
	"flag"
	"math"
	"testing"
	"time"

	"github.com/ndlib/bendo/items"
)

var dialmysql = flag.String("mysql", "/test", "Dial for mysql")

func TestMySQLItemCache(t *testing.T) {
	qc, err := NewMysqlCache(*dialmysql)
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
	qc, err := NewMysqlCache(*dialmysql)
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
			} else if math.Abs(when.Sub(tab.when).Seconds()) >= 1 {
				t.Errorf("Received %v, expected %v", when, tab.when)
			}
		}
	}
}
