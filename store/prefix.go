package store

import (
	"io"
	"strings"
)

// Wrap the store s by one which will prefix all its keys by prefix.
// This provides a way to namespace the keys, and to share the same underlying
// store among a group of users.
func NewPrefix(s Store, prefix string) Store {
	return prefixstore{s: s, p: prefix}
}

type prefixstore struct {
	s Store  // the store being wrapped
	p string // the prefix to put in front of every key
}

func (ps prefixstore) List() <-chan string {
	out := make(chan string)
	in := ps.s.List()
	go func() {
		for key := range in {
			if strings.HasPrefix(key, ps.p) {
				out <- strings.TrimPrefix(key, ps.p)
			}
		}
		close(out)
	}()
	return out
}

func (ps prefixstore) ListPrefix(prefix string) ([]string, error) {
	return ps.s.ListPrefix(ps.p + prefix)
}

func (ps prefixstore) Open(key string) (ReadAtCloser, int64, error) {
	return ps.s.Open(ps.p + key)
}

func (ps prefixstore) Create(key string) (io.WriteCloser, error) {
	return ps.s.Create(ps.p + key)
}

func (ps prefixstore) Delete(key string) error {
	return ps.s.Delete(ps.p + key)
}
