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
	registry := New(memory)
	err := registry.Load()
	if err != nil {
		t.Fatalf("received %s, expected nil", err.Error())
	}
	for _, test := range table {
		f := registry.New(test.name)
		expected := insertString(t, f, test.data)
		readAndCheck(t, f, expected)
	}
	// Now test reloading
	registry = New(memory)
	err = registry.Load()
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
		readAndCheck(t, f, expected)
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

func insertString(t *testing.T, f FileEntry, text string) string {
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

// strip out our ad-hoc formatting characters and return what text should
// have been actually uploaded.
func expectedText(text string) string {
	return strings.Map(func(in rune) rune {
		if in == '|' || in == '^' {
			return rune(-1)
		}
		return in
	}, text)
}

// read the given FileEntry, and compare what was read against the expected
// string. Raises any errors with t.
func readAndCheck(t *testing.T, f FileEntry, expected string) {
	t.Logf("readAndCheck: expect %0.10s", expected)
	r := f.Open()
	result, _ := ioutil.ReadAll(r)
	if string(result) != expected {
		t.Errorf("Read %s, expected %s", string(result), expected)
	}
	fstat := f.Stat()
	if int64(len(result)) != fstat.Size {
		t.Errorf("Got f.Size = %d, expected %d", fstat.Size, len(result))
	}
	err := r.Close()
	if err != nil {
		t.Errorf("Received error %s", err.Error())
	}
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
	registry := New(memory)
	err := registry.Load()
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
		readAndCheck(t, f, expected)
	}
}

func TestLargeFile(t *testing.T) {
	memory := store.NewMemory()
	registry := New(memory)
	err := registry.Load()
	if err != nil {
		t.Fatalf("received %s, expected nil", err.Error())
	}
	f := registry.New("large")
	// must be larger than 32k
	text := strings.Repeat("hello world", 5000)
	insertString(t, f, text)
	readAndCheck(t, f, text)
}

func listsEqual(s1, s2 []string) bool {
	if len(s1) != len(s2) {
		return false
	}
	for i := range s1 {
		if s1[i] != s2[i] {
			return false
		}
	}
	return true
}

func TestLookupUnknown(t *testing.T) {
	// get something which doesn't exist...make sure nil is returned
	memory := store.NewMemory()
	registry := New(memory)
	err := registry.Load()
	if err != nil {
		t.Fatalf("received %s, expected nil", err.Error())
	}
	f := registry.Lookup("does-not-exist")
	if f != nil {
		t.Errorf("Lookup returned %#v, expected nil", f)
	}
}
