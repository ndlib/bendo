package server

import (
	"testing"

	"github.com/ndlib/bendo/items"
)

func TestQlItemCache(t *testing.T) {
	qc, err := NewQlCache("mem--item")
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
	qc, err := NewQlCache("mem--fixity")
	if err != nil {
		t.Fatalf("Received %s", err.Error())
	}
	runFixitySequence(t, qc)
	qc.db.Close()
}

func TestQlSearchFixity(t *testing.T) {
	qc, err := NewQlCache("mem--searchfixity")
	if err != nil {
		t.Fatalf("Received %s", err.Error())
	}
	runSearchFixity(t, qc)
	qc.db.Close()
}

func TestQLDeleteFixity(t *testing.T) {
	qc, err := NewQlCache("mem--deletefixity")
	if err != nil {
		t.Fatalf("Received %s", err.Error())
	}
	runDeleteFixity(t, qc)
	qc.db.Close()
}
