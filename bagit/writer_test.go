package bagit

import (
	"bytes"
	"testing"

	"github.com/ndlib/bendo/store"
)

func TestHumansize(t *testing.T) {
	var table = []struct {
		input  int64
		output string
	}{
		{-1, "-1 Bytes"},
		{0, "0 Bytes"},
		{10, "10 Bytes"},
		{999, "999 Bytes"},
		{1000, "1 KB"},
		{999999, "999 KB"}, // truncate
		{1000000, "1 MB"},
		{10000000, "10 MB"},
		{100000000, "100 MB"},
		{1000000000, "1 GB"},
		{10000000000, "10 GB"},
		{100000000000, "100 GB"},
		{1000000000000, "1 TB"},
	}

	for _, test := range table {
		out := humansize(test.input)
		if out != test.output {
			t.Errorf("Received %s, expected %s", out, test.output)
		}
	}
}

func TestRoundtrip(t *testing.T) {
	// first save a bag
	mstore := store.NewMemory()
	f, _ := mstore.Create("test-bag.zip")
	w := NewWriter(f, "zzz-test-bag")
	w.SetTag("Contact-Name", "Nobody")
	out, _ := w.Create("hello")
	out.Write([]byte("hello there"))
	w.Close()
	f.Close()

	// now read it and see if it matches what was written
	f2, size, _ := mstore.Open("test-bag.zip")
	r, _ := NewReader(f2, size)
	contactName := r.Tags()["Contact-Name"]
	if contactName != "Nobody" {
		t.Errorf("Read contact name %s, expected %s\n", contactName, "Nobody")
	}
	version := r.Tags()["BagIt-Version"]
	if version != Version {
		t.Errorf("Read version %s, expected %s\n", version, Version)
	}
	in, _ := r.Open("hello")
	buf := new(bytes.Buffer)
	size, _ = buf.ReadFrom(in)
	in.Close()
	data := buf.String()
	if data != "hello there" {
		t.Errorf("Read %s, expected %s\n", data, "hello there")
	}
	f.Close()
}
