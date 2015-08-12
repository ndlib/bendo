package store

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestItemSubdir(t *testing.T) {
	var table = []struct{ input, output string }{
		{"x", "x/"},
		{"xy", "xy/"},
		{"xyz", "xy/z/"},
		{"wxyz", "wx/yz/"},
		{"vwxyz", "vw/xy/"},
		{"b930agg8z", "b9/30/"},
	}
	for _, s := range table {
		result := itemSubdir(s.input)
		if result != s.output {
			t.Errorf("Got %s, expected %s", result, s.output)
		}
	}
}

func TestListPrefix(t *testing.T) {
	var files = []string{
		"ab/",
		"ab/cd/",
		"ab/cd/abcd-0001",
		"ab/cd/abcd-0002",
		"ab/cd/abcdef-0001",
		"ab/ce/",
		"ab/ce/abcez-0001",
		"ab/qw/",
		"ab/qw/abqw-0001",
		"ac/",
		"ac/zx/",
		"ac/zx/aczx-0001",
		"bc/",
		"bc/de/",
		"bc/de/bcde-0001",
	}
	var table = []struct {
		prefix   string
		expected []string
	}{
		{"", []string{
			"abcd-0001",
			"abcd-0002",
			"abcdef-0001",
			"abcez-0001",
			"abqw-0001",
			"aczx-0001",
			"bcde-0001",
		}},
		{"a", []string{
			"abcd-0001",
			"abcd-0002",
			"abcdef-0001",
			"abcez-0001",
			"abqw-0001",
			"aczx-0001",
		}},
		{"ab", []string{
			"abcd-0001",
			"abcd-0002",
			"abcdef-0001",
			"abcez-0001",
			"abqw-0001",
		}},
		{"abc", []string{
			"abcd-0001",
			"abcd-0002",
			"abcdef-0001",
			"abcez-0001",
		}},
		{"abcd", []string{
			"abcd-0001",
			"abcd-0002",
			"abcdef-0001",
		}},
		{"abcde", []string{
			"abcdef-0001",
		}},
	}
	dir := makeTmpTree(files)
	defer os.RemoveAll(dir)
	s := &FileSystem{root: dir}
	for _, tab := range table {
		t.Logf("Trying prefix %s", tab.prefix)
		result, err := s.ListPrefix(tab.prefix)
		if err != nil {
			t.Errorf("Got unexpected error: %s", err.Error())
		} else if !equal(tab.expected, result) {
			t.Errorf("Got result %v, expected %v", result, tab.expected)
		}
	}
}

func TestWalkTree(t *testing.T) {
	var files = []string{
		"a/",
		"a/b/",
		"a/b/xyz-0001-1",
		"a/b/xyz-0002-1",
		"a/b/qwe-0001-2",
		"a/b/qwe-0002-1",
		"a/c/",
		"a/c/asd-0001-1",
		"a/c/asd-0002-1",
		"a/c/asd-0003-2",
	}
	var goal = []string{
		"xyz-0001-1",
		"xyz-0002-1",
		"qwe-0001-2",
		"qwe-0002-1",
		"asd-0001-1",
		"asd-0002-1",
		"asd-0003-2",
	}
	dir := makeTmpTree(files)
	defer os.RemoveAll(dir)
	c := make(chan string)
	go walkTree(c, dir, true)
	var result []string
	for name := range c {
		result = append(result, name)
		t.Log(name)
	}
	if len(result) != len(goal) {
		t.Fail()
	}
}

// returns abs path to the root of the new tree.
// remember to delete the new directory when finished.
func makeTmpTree(files []string) string {
	var data []byte
	root, _ := ioutil.TempDir("", "")
	for _, s := range files {
		var err error
		p := filepath.Join(root, s)
		if strings.HasSuffix(s, "/") {
			err = os.Mkdir(p, 0777)
		} else {
			err = ioutil.WriteFile(p, data, 0777)
		}
		if err != nil {
			fmt.Println(err)
		}
	}
	return root
}

func equal(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
