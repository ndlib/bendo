package store

import (
	"net/http"
	"testing"
	"time"
)

func TestCOW(t *testing.T) {
	cow := &COW{
		host:   "http://localhost:14000",
		local:  NewFileSystem("ppp"),
		client: &http.Client{Timeout: 120 * time.Second},
	}

	c := cow.List()
	for s := range c {
		t.Log(s)
	}
}
