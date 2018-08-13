package blobcache

import (
	"fmt"
	"testing"

	"github.com/ndlib/bendo/store"
)

func TestEvictionLRU(t *testing.T) {
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

func TestTooLargeItemLRU(t *testing.T) {
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

func TestScanLRU(t *testing.T) {
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

func TestDeleteLRU(t *testing.T) {
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

func TestStoreSyncLRU(t *testing.T) {
	// the cache keeps a LRU list in memory.
	// That means it can get out of sync with the backing store
	// in two ways: a file could exist that the LRU does not know about
	// and the LRU can think a file exists that doesn't.

	s := store.NewMemory()
	cache := NewLRU(s, 100)

	// A) If file exists and LRU does not know about it, it is not cached
	// make file out-of-band
	w, _ := s.Create("1234")
	w.Write([]byte("abcdefghijklmnopqrstuvwxyz"))
	w.Close()

	// should not contain the key
	if cache.Contains("1234") {
		t.Errorf("Cache contains item, expected it not to")
	}

	// should not get the key
	rac, n, err := cache.Get("1234")
	if err != nil {
		t.Errorf("Error getting item: %v", err)
	}
	if rac != nil {
		t.Errorf("Got item, but didn't expect to get item")
		rac.Close()
	}
	// should be able to add the key
	w, err = cache.Put("1234")
	if err != nil {
		t.Errorf("Error putting item: %v", err)
	}
	w.Write([]byte("1234567890"))
	// should get error while the first add is pending
	_, err = cache.Put("1234")
	if err != ErrPutPending {
		t.Errorf("Expected ErrPutPending, got %v", err)
	}
	w.Close()
	// should now get the key
	rac, n, err = cache.Get("1234")
	if err != nil {
		t.Errorf("Error getting item: %v", err)
	}
	if rac == nil {
		t.Errorf("Didn't get item")
	}
	if n != 10 {
		t.Errorf("Got length %v, expected 10", n)
	}
	rac.Close()

	// B) If LRU thinks a file exists and it doesn't, it is not cached
	// remove the file out-of-band
	s.Delete("1234")

	// should not get the key
	rac, n, err = cache.Get("1234")
	if err != nil {
		t.Errorf("Error getting item: %v", err)
	}
	if rac != nil {
		t.Errorf("Got item, but didn't expect to get item")
		rac.Close()
	}
}
