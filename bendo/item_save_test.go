package bendo

import (
	"io"
	"testing"
)

func TestDecodeID(t *testing.T) {
	var table = []struct {
		input, output string
		n             int
	}{
		{"xyz-0001", "xyz", 1},
		{"b930agg8z-002003", "b930agg8z", 2003},
		{"abcdefg", "", 0},
		{"abc-0001-1", "", 0},
	}
	for _, test := range table {
		s, n := desugar(test.input)
		if s != test.output || n != test.n {
			t.Errorf("Got %s, %d, expected %s, %d", s, n, test.output, test.n)
		}
	}
}

func TestWriteSizer(t *testing.T) {
	b := []byte("hello. this is a long string")
	ws := &writeSizer{}
	n, err := ws.Write(b)
	if n != len(b) || err != nil {
		t.Errorf("Got (%d, %v), expected (%d, %v)",
			n, err,
			len(b), nil)
	}
	if int64(n) != ws.Size() {
		t.Errorf("Size() = %d, expected %d", ws.Size(), n)
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
		b := extractBlobId(test.input)
		if b != test.output {
			t.Errorf("For %s got %v, expected %v",
				test.input,
				b,
				test.output)
		}
	}
}
