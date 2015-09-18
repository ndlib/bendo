package store

import (
	"sort"
	"testing"
)

func TestPrefixSmoke(t *testing.T) {
	var memoryitems = []string{
		"qwerty",
		"zabc",
		"zzed",
	}
	var prefixlists = []struct {
		input  string
		result []string
	}{
		{"", []string{"abc", "zed"}},
		{"a", []string{"abc"}},
		{"b", []string{}},
		{"z", []string{"zed"}},
	}
	m := NewMemory()
	ps := NewWithPrefix(m, "z")

	add(t, ps, "abc", "text 1")
	add(t, ps, "zed", "text 2")

	// add one to the memory store
	add(t, m, "qwerty", "text 3")

	for _, test := range prefixlists {
		t.Logf("doing prefix '%s'", test.input)
		ids, err := ps.ListPrefix(test.input)
		if err != nil {
			t.Errorf("Received error %s", err.Error())
		}
		sort.Strings(ids)
		if !equal(ids, test.result) {
			t.Errorf("Received ids %v", ids)
		}
	}

	ids, err := m.ListPrefix("")
	if err != nil {
		t.Errorf("Received error %s", err.Error())
	}
	sort.Strings(ids)
	if !equal(ids, memoryitems) {
		t.Errorf("Received ids %v", ids)
	}
}

func add(t *testing.T, s Store, id string, data string) {
	t.Logf("add(%s,%.10s)", id, data)
	w, err := s.Create(id)
	if err != nil {
		t.Fatalf("Couldn't make %s, %s", id, err.Error())
	}
	_, err = w.Write([]byte(data))
	if err != nil {
		t.Fatalf("Couldn't make %s, %s", id, err.Error())
	}
	err = w.Close()
	if err != nil {
		t.Fatalf("Couldn't make %s, %s", id, err.Error())
	}
}
