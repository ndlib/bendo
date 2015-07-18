package bendo

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

func TestGetPrefix(t *testing.T) {
	var files = []string{
		"ab/",
		"ab/cd/",
		"ab/cd/abcd-0001.zip",
		"ab/cd/abcd-0002.zip",
		"ab/cd/abcdef-0001.zip",
		"ab/qw/",
		"ab/qw/abqw-0001.zip",
	}
	var expected = []string{
		"abcd-0001",
		"abcd-0002",
		"abcdef-0001",
	}
	dir := makeTmpTree(files)
	defer os.RemoveAll(dir)
	s := &store{root: dir}
	result, err := s.getprefix("abcd")
	if err != nil {
		t.Errorf("Got unexpected error: %s", err.Error())
	} else if !equal(expected, result) {
		t.Errorf("Got result %v, expected %v", result, expected)
	}
}

func TestWalkTree(t *testing.T) {
	var files = []string{
		"a/",
		"a/b/",
		"a/b/xyz-0001-1.zip",
		"a/b/xyz-0002-1.zip",
		"a/b/qwe-0001-2.zip",
		"a/b/qwe-0002-1.zip",
		"a/c/",
		"a/c/asd-0001-1.zip",
		"a/c/asd-0002-1.zip",
		"a/c/asd-0003-2.zip",
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
