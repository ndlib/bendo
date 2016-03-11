package blobcache

import (
	"fmt"
	"testing"

	"github.com/ndlib/bendo/store"
)

func TestEviction(t *testing.T) {
	cache := NewLRU(store.NewMemory(), 100)
	// "hello world" is 11 bytes. so 10 should cause a cache eviction
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("hello-%d", i)
		w, err := cache.Put(key)
		if err != nil {
			t.Fatalf("received %s", err.Error())
		}
		w.Write([]byte("hello world"))
		w.Close()
	}

	// see if one was evicted. This does not assume an eviction strategy.
	var nEvicted int
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("hello-%d", i)
		r, size, err := cache.Get(key)
		if err != nil {
			t.Fatalf("received %s", err.Error())
		}
		if r == nil {
			nEvicted++
			continue
		}
		if size != 11 {
			t.Errorf("Received size %d, expected %d", size, 11)
		}
		r.Close()
	}
	t.Logf("nEvicted = %d", nEvicted)
	if nEvicted == 0 {
		t.Errorf("No items evicted")
	}
}

func TestTooLargeItem(t *testing.T) {
	cache := NewLRU(store.NewMemory(), 100)
	key := "qwerty"
	w, err := cache.Put(key)
	if err != nil {
		t.Fatalf("received %s", err.Error())
	}
	// write this in pieces. should error on last one
	for i := 0; i < 10; i++ {
		_, err = w.Write([]byte("hello world"))
		if err != nil {
			t.Logf("Received error %s", err.Error())
			break
		}
	}
	if err != ErrCacheFull {
		t.Errorf("Did not receive ErrCacheFull")
	}
	w.Close()
	size := cache.size
	if size != 0 {
		t.Errorf("Cache size is %d. Expected %d", size, 0)
	}
}

func TestScan(t *testing.T) {
	mem := store.NewMemory()

	// populate the store
	var table = []struct {
		key, contents string
	}{
		{"qwerty", "1234567890"},
		{"asdf", "1234567890-="},
		{"zxcv", "abcdefghijklmnopqrstuvwxyz"},
	}

	for _, elem := range table {
		w, err := mem.Create(elem.key)
		if err != nil {
			t.Fatal(err)
		}
		w.Write([]byte(elem.contents))
		w.Close()
	}

	// now set up the cache and scan it
	cache := NewLRU(mem, 100)
	cache.Scan()

	for _, elem := range table {
		r, _, _ := cache.Get(elem.key)
		if r == nil {
			t.Logf("key %s: nil", elem.key)
			continue
		}
		r.Close()
	}

	// now set up a small cache and scan that
	cache = NewLRU(mem, 15)
	cache.Scan()

	for _, elem := range table {
		r, _, _ := cache.Get(elem.key)
		if r == nil {
			t.Logf("key %s: nil", elem.key)
			continue
		}
		r.Close()
	}
}

func TestDelete(t *testing.T) {
	cache := NewLRU(store.NewMemory(), 100)
	key := "1234"
	w, err := cache.Put(key)
	if err != nil {
		t.Fatalf("received %s", err.Error())
	}
	w.Write([]byte("abcdefghijklmnopqrstuvwxyz"))
	w.Close()

	if !cache.Contains(key) {
		t.Errorf("Cache does not contain item, expected it to.")
	}
	if cache.size != 26 {
		t.Errorf("Cache size is %d, expected 26", cache.size)
	}

	err = cache.Delete(key)
	if err != nil {
		t.Errorf("Error deleting key, %s", err.Error())
	}

	if cache.Contains(key) {
		t.Errorf("Cache contains item, expected it to be deleted.")
	}
	if cache.size != 0 {
		t.Errorf("Cache size is %d, expected 0", cache.size)
	}

	// delete it a second time. should not get an error
	err = cache.Delete(key)
	if err != nil {
		t.Errorf("Error deleting key, %s", err.Error())
	}

}
