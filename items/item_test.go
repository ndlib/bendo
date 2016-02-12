package items

import (
	"testing"
)

func TestBlobByExtendedSlot(t *testing.T) {
	m := Item{
		Blobs: []*Blob{&Blob{}, &Blob{}, &Blob{}, &Blob{}, &Blob{}},
		Versions: []*Version{
			&Version{
				ID:    1,
				Slots: map[string]BlobID{"a": 1, "b": 2, "c": 3},
			},
			&Version{
				ID:    2,
				Slots: map[string]BlobID{"b": 2, "c": 4, "d": 5},
			},
		},
	}
	table := []struct {
		input  string
		output BlobID
	}{
		{"a", 0},
		{"@1/a", 1},
		{"b", 2},
		{"@1/b", 2},
		{"c", 4},
		{"@1/d", 0},
		{"@blob/4", 4},
		{"@blob/04", 4}, // octal?
		{"@blob/0x4", 0},
		{"@blob/", 0},
	}

	for _, tab := range table {
		r := m.BlobByExtendedSlot(tab.input)
		if r != tab.output {
			t.Errorf("Input: %s. Received %d, expected %d",
				tab.input,
				r,
				tab.output)
		}
	}

}
