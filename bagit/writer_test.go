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
	f, err := mstore.Create("test-bag.zip")
	if err != nil {
		t.Fatal(err)
	}
	w := NewWriter(f, "zzz-test-bag")
	w.SetTag("Contact-Name", "Nobody")
	out, err := w.Create("hello")
	if err != nil {
		t.Fatal(err)
	}
	out.Write([]byte("hello there"))
	w.Close()
	f.Close()

	// now read it and see if it matches what was written
	f2, size, err := mstore.Open("test-bag.zip")
	if err != nil {
		t.Fatal(err)
	}
	r, err := NewReader(f2, size)
	if err != nil {
		t.Fatal(err)
	}
	contactName := r.Tags()["Contact-Name"]
	if contactName != "Nobody" {
		t.Errorf("Read contact name %s, expected %s\n", contactName, "Nobody")
	}
	version := r.Tags()["BagIt-Version"]
	if version != Version {
		t.Errorf("Read version %s, expected %s\n", version, Version)
	}

	// does the hello payload file match?
	in, err := r.Open("hello")
	if err != nil {
		t.Fatal(err)
	}
	buf := new(bytes.Buffer)
	size, err = buf.ReadFrom(in)
	if err != nil {
		t.Error(err)
	}
	in.Close()
	data := buf.String()
	if data != "hello there" {
		t.Errorf("Read %s, expected %s\n", data, "hello there")
	}

	// does the bag verification work?
	err = r.Verify()
	if err != nil {
		t.Errorf("Valid returned %s\n", err.Error())
	}

	// does the file listing work?
	filelist := r.Files()
	if len(filelist) != 1 || filelist[0] != "hello" {
		t.Errorf("File list is %v, expected [hello]", filelist)
	}

	f2.Close()
}
