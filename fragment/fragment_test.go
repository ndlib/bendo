package fragment

import (
	"io/ioutil"
	"strings"
	"testing"

	"github.com/ndlib/bendo/store"
)

func TestFileWriting(t *testing.T) {
	var table = []struct {
		name string
		data string // split appends on "|", writes on "^"
	}{
		{"a", "single write"},
		{"b", "two ^writes"},
		{"c", "a write|and ^append"},
		{"d", "quite a number| of appends| in a row^maybe some^extra|writes for good measure"},
	}
	memory := store.NewMemory()
	registry, err := New(memory)
	if err != nil {
		t.Fatalf("received %s, expected nil", err.Error())
	}
	for _, test := range table {
		f := registry.New(test.name)
		expected := insertString(t, f, test.data)
		r := f.Open()
		result, _ := ioutil.ReadAll(r)
		r.Close()
		if string(result) != expected {
			t.Fatalf("Read %s, expected %s", string(result), expected)
		}
		if int64(len(result)) != f.Size {
			t.Fatalf("Got f.Size = %d, expected %d", f.Size, len(result))
		}
	}
	// Now test reloading
	registry, err = New(memory)
	if err != nil {
		t.Fatalf("received %s, expected nil", err.Error())
	}
	for _, test := range table {
		expected := expectedText(test.data)
		f := registry.Lookup(test.name)
		if f == nil {
			t.Fatalf("Lookup of key %s failed", test.name)
			continue
		}
		r := f.Open()
		result, _ := ioutil.ReadAll(r)
		if string(result) != expected {
			t.Fatalf("Read %s, expected %s", string(result), expected)
		}
		if int64(len(result)) != f.Size {
			t.Fatalf("Got f.Size = %d, expected %d", f.Size, len(result))
		}
	}
	// now delete things
	for _, test := range table {
		registry.Delete(test.name)
	}
	// should have no keys in memory
	keys, _ := memory.ListPrefix("")
	if len(keys) > 0 {
		t.Fatalf("Got %v, expected empty list", keys)
	}
}

func insertString(t *testing.T, f *File, text string) string {
	var expected string
	segments := strings.Split(text, "|")
	for _, segment := range segments {
		w, err := f.Append()
		if err != nil {
			t.Fatalf("got %s, expected nil", err.Error())
		}
		for _, word := range strings.Split(segment, "^") {
			expected += word
			w.Write([]byte(word))
		}
		w.Close()
	}
	return expected
}

func expectedText(text string) string {
	return strings.Map(func(in rune) rune {
		if in == '|' || in == '^' {
			return rune(-1)
		}
		return in
	}, text)
}

func TestRollback(t *testing.T) {
	var table = []struct {
		name string
		data string // split appends on "|", writes on "^"
	}{
		{"aaaaaa", "single write"},
		{"aaaaab", "two ^writes"},
		{"aaaaac", "a write|and ^append"},
		{"aaaaad", "quite a number| of appends| in a row^maybe some^extra|writes for good measure"},
	}
	memory := store.NewMemory()
	registry, err := New(memory)
	if err != nil {
		t.Fatalf("received %s, expected nil", err.Error())
	}
	for _, test := range table {
		var expected string
		f := registry.New(test.name)
		insertString(t, f, test.data)
		f.Rollback()
		n := strings.LastIndex(test.data, "|")
		if n != -1 {
			expected = expectedText(test.data[:n])
		} else {
			expected = ""
		}
		r := f.Open()
		result, _ := ioutil.ReadAll(r)
		if string(result) != expected {
			t.Fatalf("Read %s, expected %s", string(result), expected)
		}
		if int64(len(result)) != f.Size {
			t.Fatalf("Got f.Size = %d, expected %d", f.Size, len(result))
		}
	}
}
