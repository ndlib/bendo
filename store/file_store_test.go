package store

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestItemSubdir(t *testing.T) {
	var table = []struct{ input, output string }{
		{"", "./"},
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

func TestCreate(t *testing.T) {
	root, _ := ioutil.TempDir("", "")
	defer os.RemoveAll(root)
	s := NewFileSystem(root)

	const text = "hello abc"
	// make an object
	add(t, s, "abc", text)

	r, n, err := s.Open("abc")
	if err != nil {
		t.Errorf("Received error %s", err.Error())
	}
	if n != int64(len(text)) {
		t.Errorf("Received length %d, expected %d", n, len(text))
	}
	var buf = make([]byte, 32)
	n64, err := r.ReadAt(buf, 0)
	if err != nil && err != io.EOF {
		t.Errorf("Received error %s", err.Error())
	}
	t.Logf("n = %d", n64)
	if string(buf[:n64]) != text {
		t.Errorf("Received %v, expected %s", buf, text)
	}
	err = r.Close()
	if err != nil {
		t.Errorf("Received error %s", err.Error())
	}

	// make object again....should error
	_, err = s.Create("abc")
	if err != ErrKeyExists {
		t.Errorf("Received error %s", err.Error())
	}

	// make sure file ab/c/abc exists and file scratch/abc doesn't
	if !exists(root, itemSubdir("abc"), "abc") {
		t.Errorf("File abc does not exist")
	}
	if exists(root, scratchdir, "abc") {
		t.Errorf("File scratch abc exists")
	}
}

func TestOpenTwice(t *testing.T) {
	// should not be able to open an object twice for writing
	root, _ := ioutil.TempDir("", "")
	defer os.RemoveAll(root)
	s := NewFileSystem(root)

	const text = "hello abc"
	// make an object
	w1, err := s.Create("abc")
	if err != nil {
		t.Errorf("Received error %s", err.Error())
	}

	// now try to open a second time, before closing w1
	w2, err := s.Create("abc")
	if err == nil {
		w2.Close()
		t.Errorf("Received error %s", err.Error())
	}
	w1.Close()
}

func TestFileDisappear(t *testing.T) {
	root, _ := ioutil.TempDir("", "")
	defer os.RemoveAll(root)
	s := NewFileSystem(root)

	const text = "hello abc"
	const text2 = "Second time abc"
	// make an object
	w, err := s.Create("abc")
	if err != nil {
		t.Errorf("Received error %s", err.Error())
	}
	_, err = w.Write([]byte(text))
	if err != nil {
		t.Errorf("Received error %s", err.Error())
	}
	// touch the target file, then see if closing w complains
	second, err := os.Create(filepath.Join(root, itemSubdir("abc"), "abc"))
	second.Write([]byte("second file"))
	second.Close()

	// now close our original file, expect error
	err = w.Close()
	if err != ErrKeyExists {
		t.Errorf("Received error %s", err.Error())
	}

	// the temp file is still in place?
	if !exists(root, scratchdir, "abc") {
		t.Errorf("File scratch abc does not exist")
	}

	// delete our foil file
	s.Delete("abc")

	// now we should NOT be able to create another "abc",
	// since the file remnant is still in the scratch space
	w, err = s.Create("abc")
	if err == nil {
		t.Fatalf("Received no error", err.Error())
	}
}

func TestDelete(t *testing.T) {
	root, _ := ioutil.TempDir("", "")
	defer os.RemoveAll(root)
	s := NewFileSystem(root)

	// it is not an error to delete an object which is not present
	err := s.Delete("abc")
	if err != nil {
		t.Errorf("Received error %s", err.Error())
	}

	// make object
	add(t, s, "abc", "hello abc from test delete")

	err = s.Delete("abc")
	if err != nil {
		t.Errorf("Received error %s", err.Error())
	}

	_, _, err = s.Open("abc")
	t.Logf("Open(abc) = %s", err.Error())
	if err == nil {
		t.Errorf("Received nil error")
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

func exists(paths ...string) bool {
	_, err := os.Stat(filepath.Join(paths...))
	return err == nil
}
