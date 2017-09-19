package main

import (
	"encoding/hex"
	"path"
	"sync"
	"testing"
)

func TestChecksum(t *testing.T) {
	// Does checksum routine checksum correctly?
	var wg sync.WaitGroup
	var table = []struct {
		Name string
		MD5  string
	}{
		{Name: "testdata/checksum.txt", MD5: "63c759187d2ec28910ac4f3f72690be3"},
	}

	in := make(chan string)
	out := make(chan File, 200) // buffer so it won't block

	wg.Add(1)
	go func() {
		ChecksumLocalFiles("./", in, out)
		close(out)
		wg.Done()
	}()

	for _, row := range table {
		in <- path.Join(".", row.Name)
	}
	close(in)
	wg.Wait()

	// this assumes files are checksummed in same order as table...
	for _, row := range table {
		f := <-out
		if f.Name != row.Name {
			t.Errorf("Expected %v, Got %v", row.Name, f.Name)
		}
		md5 := hex.EncodeToString(f.MD5)
		if md5 != row.MD5 {
			t.Errorf("Expected MD5 %v, Got %v", row.MD5, md5)
		}
	}
}

func TestParseManifest(t *testing.T) {
	var table = []struct {
		Name     string
		MD5      string
		SHA256   string
		MimeType string
	}{
		{"testdata/a", "1234567890", "abcdef", "text/plain"},
		{"testdata/b", "2345678901", "bcdefa", "image/png"},
		{"testdata/c", "", "", "application/xml"},
	}

	fixture := path.Join(".", "testdata", "manifest.txt")
	out := make(chan File, 200) // buffer so it won't block

	err := ParseManifest("./", fixture, out)
	if err != nil {
		t.Fatal(err)
	}

	for _, row := range table {
		f := <-out
		if f.Name != row.Name {
			t.Errorf("Expected %v, Got %v", row.Name, f.Name)
		}
		h := hex.EncodeToString(f.MD5)
		if h != row.MD5 {
			t.Errorf("Expected MD5 %v, Got %v", row.MD5, h)
		}
		h = hex.EncodeToString(f.SHA256)
		if h != row.SHA256 {
			t.Errorf("Expected SHA256 %v, Got %v", row.SHA256, h)
		}
		if f.MimeType != row.MimeType {
			t.Errorf("Expected mimetype %v, Got %v", row.MimeType, f.MimeType)
		}
	}
}
