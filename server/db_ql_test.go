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

func TestQLIndexItem(t *testing.T) {
	qc, err := NewQlCache("mem--indexitem")
	if err != nil {
		t.Fatal(err)
	}
	testitem := &items.Item{
		ID:        "abcd",
		MaxBundle: 2,
		Blobs: []*items.Blob{
			&items.Blob{ID: 1, Size: 5, Bundle: 1},
			&items.Blob{ID: 2, Size: 10, Bundle: 1},
			&items.Blob{ID: 3, Size: 9, Bundle: 2},
		},
		Versions: []*items.Version{
			&items.Version{ID: 1, Creator: "me", Note: "initial commit",
				Slots: map[string]items.BlobID{"files/hello.txt": items.BlobID(1), "goodbye.txt": items.BlobID(2)}},
			&items.Version{ID: 2, Creator: "me", Note: "update",
				Slots: map[string]items.BlobID{"hello.txt": items.BlobID(1), "goodbye.txt": items.BlobID(3)}},
		},
	}
	const itemid = "abcd"
	qc.IndexItem(itemid, testitem)

	for _, blob := range testitem.Blobs {
		b, err := qc.FindBlob(itemid, int(blob.ID))
		t.Log(b, err)
		if err != nil {
			t.Error(err)
		}
		if b.ID != blob.ID {
			t.Error("Received ID", b.ID, "expected", blob.ID)
		}
		if b.Size != blob.Size {
			t.Error("Received Size", b.Size, "expected", blob.Size)
		}
		if b.Bundle != blob.Bundle {
			t.Error("Received Bundle", b.Bundle, "expected", blob.Bundle)
		}
	}

	n, err := qc.getMaxBlob(itemid)
	if err != nil || n != 3 {
		t.Error("Received max blob", n, "expected", 3, err)
	}

	n, err = qc.getMaxVersion(itemid)
	if err != nil || n != 2 {
		t.Error("Received max version", n, "expected", 2, err)
	}

	for _, version := range testitem.Versions {
		for slot, bid := range version.Slots {
			blob, err := qc.FindBlobBySlot(itemid, int(version.ID), slot)
			if err != nil || blob.ID != bid {
				t.Error("For version", version.ID, slot, "received", blob, "/", err)
			}
		}
	}
}
