package items

import (
	"bytes"
	"io"
	"reflect"
	"testing"
	"time"
)

func TestDecodeID(t *testing.T) {
	var table = []struct {
		input, output string
		n             int
	}{
		{"xyz-0001", "xyz", 1},
		{"b930agg8z-002003", "b930agg8z", 2003},
		{"abcdefg", "", 0},
		{"abc-0001-1", "abc-0001", 1},
		{"abc-0001-2", "abc-0001", 2},
		{"abc-0001-z", "", 0},
	}
	for _, test := range table {
		s, n := desugar(test.input)
		if s != test.output || n != test.n {
			t.Errorf("Got %s, %d, expected %s, %d", s, n, test.output, test.n)
		}
	}
}

type closer struct{ closed bool }

func (c *closer) Close() error {
	c.closed = true
	return nil
}
func (c *closer) Read(p []byte) (int, error) { return 0, io.EOF }

func TestParentCloser(t *testing.T) {
	var parent, child closer
	p := parentReadCloser{
		parent:     &parent,
		ReadCloser: &child,
	}
	p.Close()
	if !parent.closed {
		t.Errorf("Got parent %v, expected %v",
			parent.closed,
			true)
	}
	if !child.closed {
		t.Errorf("Got child %v, expected %v",
			child.closed,
			true)
	}
}

func TestExtractBlobId(t *testing.T) {
	var table = []struct {
		input  string
		output BlobID
	}{
		{"blob/234", 234},
		{"abc/6", 0},
		{"blob/3/5", 0},
		{"blob/cdef", 0},
	}
	for _, test := range table {
		b := extractBlobID(test.input)
		if b != test.output {
			t.Errorf("For %s got %v, expected %v",
				test.input,
				b,
				test.output)
		}
	}
}

func TestSerialization(t *testing.T) {
	item := &Item{
		ID:        "123456",
		MaxBundle: 5,
		Blobs: []*Blob{
			&Blob{
				ID:         1,
				SaveDate:   time.Now(),
				Creator:    "alice",
				Size:       9876543210, // > 2**31
				Bundle:     2,
				MD5:        []byte{123, 234, 34, 45, 56, 67, 78, 89, 90, 12},
				SHA256:     []byte{213, 24, 35, 46, 57, 68, 89, 31},
				DeleteDate: time.Now(),
				Deleter:    "bob",
				DeleteNote: "this is not valid",
			},
		},
		Versions: []*Version{
			&Version{
				ID:       1,
				SaveDate: time.Now(),
				Creator:  "don",
				Note:     "test note",
				Slots: map[string]BlobID{
					"file1":  2,
					"file2":  2,
					"README": 1,
				},
			},
		},
	}
	buf := &bytes.Buffer{}

	err := writeItemInfo(buf, item)
	if err != nil {
		t.Fatalf("Received error %s", err.Error())
	}
	result, err := readItemInfo(buf)
	if err != nil {
		t.Fatalf("Received error %s", err.Error())
	}
	if item.ID != result.ID ||
		item.MaxBundle != result.MaxBundle ||
		len(item.Versions) != len(result.Versions) ||
		len(item.Blobs) != len(result.Blobs) {
		t.Errorf("Received %#v, expected %#v", result, item)
	}
	for i := range item.Blobs {
		a := item.Blobs[i]
		b := result.Blobs[i]
		if a.SaveDate.Sub(b.SaveDate) != 0 ||
			a.DeleteDate.Sub(b.DeleteDate) != 0 {
			t.Errorf("Received %v, expected %v", b, a)
		}
		// there is a location cache in dates, so deepequal
		// keeps thinking they are different.
		a.SaveDate = b.SaveDate
		a.DeleteDate = b.DeleteDate
		if !reflect.DeepEqual(a, b) {
			t.Errorf("Received %#v, expected %#v", b, a)
		}
	}
	for i := range item.Versions {
		a := item.Versions[i]
		b := result.Versions[i]
		if a.SaveDate.Sub(b.SaveDate) != 0 {
			t.Errorf("Received %v, expected %v", b, a)
		}
		// there is a location cache in dates, so deepequal
		// keeps thinking they are different.
		a.SaveDate = b.SaveDate
		if !reflect.DeepEqual(*a, *b) {
			t.Errorf("Received %#v, expected %#v", b, a)
		}
	}
}
