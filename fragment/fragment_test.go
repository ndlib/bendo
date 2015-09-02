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

func expectedText(text string) string {
	return strings.Map(func(in rune) rune {
		if in == '|' || in == '^' {
			return rune(-1)
		}
		return in
	}, text)
}

func readAndCheck(t *testing.T, f FileEntry, expected string) {
	r := f.Open()
	result, _ := ioutil.ReadAll(r)
	if string(result) != expected {
		t.Fatalf("Read %s, expected %s", string(result), expected)
	}
	fstat := f.Stat()
	if int64(len(result)) != fstat.Size {
		t.Fatalf("Got f.Size = %d, expected %d", fstat.Size, len(result))
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

func TestCombineCommon(t *testing.T) {
	var tests = []struct {
		input  [][]string
		output []string
	}{
		{
			input:  [][]string{},
			output: []string{},
		},
		{
			input:  [][]string{{"a", "b", "c"}},
			output: []string{"a", "b", "c"},
		},
		{
			input: [][]string{{"a", "c", "e"},
				{"b", "c", "e"}},
			output: []string{"c", "e"},
		},
		{
			input: [][]string{{"a", "c", "e"},
				{"b", "d", "f"}},
			output: []string{},
		},
		{
			input: [][]string{{"a", "c", "e"},
				{"f", "g", "h"}},
			output: []string{},
		},
		{
			input: [][]string{{"a", "c", "e"},
				{"b", "c", "e"},
				{"c", "d"}},
			output: []string{"c"},
		},
		{
			input: [][]string{{"a", "c", "e"},
				{"b", "c", "e"},
				{"d", "e"},
				{"d"}},
			output: []string{},
		},
		{
			input: [][]string{{"a", "e", "i", "o", "u"},
				{"b", "c", "h", "i", "l", "z"},
				{"c", "d", "i"},
				{"i", "j", "l", "z"}},
			output: []string{"i"},
		},
		{
			input: [][]string{{"a", "b"},
				{"a", "b"},
				{"a", "c"},
				{"b"}},
			output: []string{},
		},
	}

	for _, test := range tests {
		result := combineCommon(test.input)
		ok := true
		if len(result) != len(test.output) {
			ok = false
		} else {
			for i := range result {
				if result[i] != test.output[i] {
					ok = false
					break
				}
			}
		}
		if !ok {
			t.Fatalf("On input %v, got %v, expected %v",
				test.input, result, test.output)
		}
	}
}

func TestSetLabels(t *testing.T) {
	memory := store.NewMemory()
	registry := New(memory)
	err := registry.Load()
	if err != nil {
		t.Fatalf("received %s, expected nil", err.Error())
	}
	f := registry.New("zzz")
	var labels = []string{"qwerty", "asdfg", "zxcvb", "asdfg"}
	f.SetLabels(labels)
	fstat := f.Stat()
	if !listsEqual(fstat.Labels, []string{"asdfg", "qwerty", "zxcvb"}) {
		t.Fatalf("Received %#v", fstat.Labels)
	}
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

func TestListFiltered(t *testing.T) {
	var items = []struct {
		id     string
		labels []string
	}{
		{"zzz", []string{"qwerty", "asdfg", "zxcvb"}},
		{"yyy", []string{"qwerty", "asdfg"}},
		{"xxx", []string{"qwerty"}},
		{"www", []string{}},
		{"vvv", []string{"zxcvb"}},
	}
	var tests = []struct {
		labels []string
		result []string
	}{
		{[]string{}, []string{"vvv", "www", "xxx", "yyy", "zzz"}},
		{[]string{"qwerty"}, []string{"xxx", "yyy", "zzz"}},
		{[]string{"asdfg"}, []string{"yyy", "zzz"}},
		{[]string{"qwerty", "asdfg"}, []string{"yyy", "zzz"}},
		{[]string{"zxcvb"}, []string{"vvv", "zzz"}},
		{[]string{"qwerty", "zxcvb"}, []string{"zzz"}},
		{[]string{"12345"}, []string{}},
	}

	memory := store.NewMemory()
	registry := New(memory)
	err := registry.Load()
	if err != nil {
		t.Fatalf("received %s, expected nil", err.Error())
	}
	for _, item := range items {
		f := registry.New(item.id)
		f.SetLabels(item.labels)
	}
	for _, test := range tests {
		result := registry.ListFiltered(test.labels)
		if !listsEqual(result, test.result) {
			t.Errorf("For %v received %v, expected %v",
				test.labels,
				result,
				test.result)
		}
	}
}
