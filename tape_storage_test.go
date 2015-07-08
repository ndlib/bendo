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
