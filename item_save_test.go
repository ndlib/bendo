package bendo

import (
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
