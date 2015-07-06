package bendo

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestDecodeID(t *testing.T) {
	var table = []struct{ input, output string }{
		{"xyz-0001-1.zip", "xyz"},
		{"b930agg8z-002003-30.zip", "b930agg8z"},
	}
	for _, s := range table {
		result := decodeID(s.input)
		if result != s.output {
			t.Errorf("Got %s, expected %s", result, s.output)
		}
	}
}

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
		"xyz",
		"qwe",
		"asd",
	}
	dir, _ := ioutil.TempDir("", "")
	makeTmpTree(dir, files)
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

func makeTmpTree(root string, files []string) {
	var data []byte
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
}

func TestMostRecent(t *testing.T) {
	var files = []string{
		"wx/",
		"wx/yz/",
		"wx/yz/wxyz55-0001-2.zip",
		"wx/yz/wxyz55-0002-1.zip",
		"wx/yz/wxyz55-0003-1.zip",
		"wx/yz/wxyz55-0004-30.zip",
		"wx/yz/wxyz55-0005-1.zip",
		"wx/yz/wxyz66-0001-1.zip",
	}
	var table = []struct {
		input, output string
		err           error
	}{
		{"wxyz55", "wx/yz/wxyz55-0005-1.zip", nil},
		{"wxyz66", "wx/yz/wxyz66-0001-1.zip", nil},
		{"wxyz67", "", ErrNoItem},
	}
	dir, _ := ioutil.TempDir("", "")
	makeTmpTree(dir, files)
	defer os.RemoveAll(dir)
	store := &store{root: dir}
	for _, tab := range table {
		result, err := store.mostRecent(tab.input)
		fullPath := filepath.Join(dir, tab.output)
		if err != tab.err ||
			(tab.output != "" && result != fullPath) {
			t.Errorf("Got (%s,%s) for %s expected (%s, %s)",
				result, err, tab.input, fullPath, tab.err)
		}
	}
}
