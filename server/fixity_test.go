package server

import (
	"testing"
	"time"
)

func TestFixityStatusValidtion(t *testing.T) {
	var table = []struct {
		input string
		valid bool
	}{
		{"ok", true},
		{"scheduled", true},
		{"error", true},
		{"mismatch", true},
		{"something", false},
		{"OK", false},
		{"mismatches", false},
	}

	// test for https://github.com/ndlib/bendo/issues/164
	t.Log("testing fixity validation routines")

	for _, tab := range table {
		v, err := statusValidate(tab.input)
		if tab.valid && (err != nil || v != tab.input) {
			t.Errorf("Expected %s to be valid, Received (%s, %v)", tab.input, v, err)
		} else if !tab.valid && err == nil {
			t.Errorf("Expected %s to be invalid, Received (%s, %v)", tab.input, v, err)
		}
	}
}

func TestFixityTimeValidation(t *testing.T) {
	var table = []struct {
		input  string
		valid  bool
		output time.Time
	}{
		{"", true, time.Time{}},
		{"*", true, time.Time{}},
		{"2017-10-01", true, time.Date(2017, time.October, 1, 0, 0, 0, 0, time.UTC)},
		{"2017-10", false, time.Time{}},
		{"2017", false, time.Time{}},
		{"2017-10-01T05:10:15Z", true, time.Date(2017, time.October, 1, 5, 10, 15, 0, time.UTC)},
		{"not a time", false, time.Time{}},
		{"Sep 5, 2017", false, time.Time{}},
	}

	for _, tab := range table {
		got, err := timeValidate(tab.input)
		if !tab.valid && err == nil {
			t.Errorf("For %s expected error", tab.input)
		}
		if tab.valid && (err != nil || !tab.output.Equal(got)) {
			t.Errorf("For %s expected %s, got %s %s", tab.input, tab.output, got, err)
		}
	}
}

// General tests against a FixityDB interface
//
// The function names are not in the form TestXxxx since they are intended to
// be called from a test routine that has already created a FixityDB to be
// tested. This lets us run them against different database backends.

func runFixitySequence(t *testing.T, fx FixityDB) {
	now := time.Now()
	nowPlusHour := now.Add(time.Hour)
	var z time.Time
	var table = []struct {
		command string
		fx      Fixity
		store   int
	}{
		{"NextFixity", Fixity{}, 0}, // nothing to start with

		{"UpdateFixity", Fixity{Item: "fixity-seq-1", ScheduledTime: now}, 1},         // add check for item
		{"UpdateFixity", Fixity{Item: "fixity-seq-1", ScheduledTime: nowPlusHour}, 2}, // and another check for item
		{"GetFixity", Fixity{Item: "fixity-seq-1", Status: "scheduled", ScheduledTime: now}, 1},
		{"GetFixity", Fixity{Item: "fixity-seq-1", Status: "scheduled", ScheduledTime: nowPlusHour}, 2},
		{"UpdateFixity", Fixity{Item: "fixity-seq-1", Status: "ok", ScheduledTime: now}, 1},
		{"GetFixity", Fixity{Item: "fixity-seq-1", Status: "ok", ScheduledTime: now}, 1},
		{"UpdateFixity", Fixity{Item: "fixity-seq-1", Status: "whatever", ScheduledTime: now}, 1}, // now try to change status again
		{"GetFixity", Fixity{Item: "fixity-seq-1", Status: "ok", ScheduledTime: now}, 1},          // should not have changed
		{"LookupCheck", Fixity{Item: "fixity-seq-1", ScheduledTime: nowPlusHour}, 0},              // is next check for item correct?

		{"LookupCheck", Fixity{Item: "not-there", ScheduledTime: z}, 0}, // lookup non-existent item
		{"NextFixity", Fixity{ScheduledTime: nowPlusHour}, 2},           // the next item to be checked is qwe

		{"UpdateFixity", Fixity{Item: "fixity-2", Status: "ok", ScheduledTime: now}, 0},
		{"LookupCheck", Fixity{Item: "fixity-2", ScheduledTime: z}, 0}, // this should ignore the "ok" record we made above
	}

	register := make(map[int]int64)
	for _, tab := range table {
		t.Logf("%v", tab)
		switch tab.command {
		case "NextFixity":
			// use ScheduledTime, see if record id matches
			id := fx.NextFixity(tab.fx.ScheduledTime.Add(1 * time.Minute))
			if id != register[tab.store] {
				t.Errorf("Expected %v, got %v", register[tab.store], id)
			}
		case "LookupCheck":
			// look up fx.Item, see if time matches fx.ScheduledTime
			when, err := fx.LookupCheck(tab.fx.Item)
			if err != nil {
				t.Errorf("error %s", err.Error())
			} else if !within(when, tab.fx.ScheduledTime, time.Second) {
				t.Errorf("Expected %v, got %v", tab.fx.ScheduledTime, when)
			}
		case "GetFixity":
			// see if id matches fx
			record := fx.GetFixity(register[tab.store])
			if record == nil {
				t.Errorf("GetFixity(%v) returned nil", register[tab.store])
				continue
			}
			if record.Item != tab.fx.Item {
				t.Errorf("Expected %v, got %v", tab.fx.Item, record.Item)
			}
			if record.Status != tab.fx.Status {
				t.Errorf("Expected %v, got %v", tab.fx.Status, record.Status)
			}
			if !within(record.ScheduledTime, tab.fx.ScheduledTime, time.Second) {
				t.Errorf("Expected %v, got %v", tab.fx.ScheduledTime, record.ScheduledTime)
			}
		case "UpdateFixity":
			// update the given record
			record := tab.fx
			record.ID = register[tab.store] // relies on being == 0 by default
			id, err := fx.UpdateFixity(record)
			if err != nil {
				t.Errorf("UpdateFixity(%#v) returned %s", record, err)
			}
			if tab.store > 0 {
				t.Logf("= %d", id)
				register[tab.store] = id
			}
		}
	}
}

// are times `a` and `b` within duration `d` of each other?
func within(a, b time.Time, d time.Duration) bool {
	diff := a.Sub(b)
	if diff < 0 {
		diff = -diff
	}
	return diff <= d
}

// runSearchFixity takes something having the FixityDB interface and
// tests the interface. This lets the test be shared between the
// database adapters.
func runSearchFixity(t *testing.T, fx FixityDB) {
	var seeds = []Fixity{
		{Item: "abc", Status: "ok"},
		{Item: "abc", Status: "error"},
		{Item: "abc", Status: "scheduled"},
		{Item: "def", Status: "scheduled"},
	}

	now := time.Now()
	nowMinusHour := now.Add(-time.Hour)
	nowPlusHour := now.Add(time.Hour)

	for _, record := range seeds {
		record.ScheduledTime = now
		_, err := fx.UpdateFixity(record)
		if err != nil {
			t.Fatal(err)
		}
	}

	var z time.Time // zero time to simplify the table
	var table = []struct {
		start, end   time.Time
		item, status string
		nresults     int
	}{
		{nowPlusHour, z, "", "", 0},            // everything for an hour from now on
		{z, nowMinusHour, "", "", 0},           // get everything before hour before now
		{z, z, "abc", "", 3},                   // get all for abc
		{z, z, "def", "", 1},                   // all for def
		{z, z, "def", "scheduled", 1},          // all scheduled for def
		{z, z, "abc", "ok", 1},                 // all ok for abc
		{z, z, "def", "ok", 0},                 // all ok for def
		{z, z, "", "", 4},                      // get everything
		{nowMinusHour, nowPlusHour, "", "", 4}, // get everything between hour before and hour after now
		{z, z, "", "scheduled", 2},             // all scheduled
		{z, z, "", "ok", 1},                    // all ok
	}

	for _, tab := range table {
		t.Logf("%v", tab)
		records := fx.SearchFixity(tab.start, tab.end, tab.item, tab.status)
		if len(records) != tab.nresults {
			t.Errorf("Expected %d records, got %d\n", tab.nresults, len(records))
			for i := range records {
				t.Logf("%v\n", records[i])
			}
		}
	}
}

func runDeleteFixity(t *testing.T, fx FixityDB) {
	// add fixity record of different transactions
	var table = []struct {
		status    string
		deletable bool
	}{
		{"scheduled", true},
		{"ok", false},
		{"error", false},
		{"mismatch", false},
	}

	now := time.Now()
	for _, tab := range table {
		t.Log(tab)
		id, err := fx.UpdateFixity(Fixity{Item: "delete-test", Status: tab.status, ScheduledTime: now})
		if err != nil {
			t.Errorf("Got %s", err)
			continue
		}
		fx.DeleteFixity(id) // try to delete it
		record := fx.GetFixity(id)
		if tab.deletable && record != nil {
			t.Errorf("Expected %d to delete, but it still exists", id)
		} else if !tab.deletable && record == nil {
			t.Errorf("Expected %d to not be deleted, but it was", id)
		}
	}

}
