package transaction

import (
	"testing"

	"github.com/ndlib/bendo/blobcache"
	"github.com/ndlib/bendo/fragment"
	"github.com/ndlib/bendo/items"
	"github.com/ndlib/bendo/store"
)

func TestCommitErrors(t *testing.T) {
	// covers CURATE-250 - does not stop early on error
	tx := &Transaction{
		ItemID:  "abcd1234",
		BlobMap: make(map[string]int),
		Commands: []command{
			command{"add", "file1"}, // Cannot find file1
			command{"add", "file2"}, // Cannot find file2
			command{"slot", "test/file", "file1"},
		},
	}

	tape := items.NewWithCache(store.NewMemory(), items.NewMemoryCache())
	uploads := fragment.New(store.NewMemory())
	cache := blobcache.NewLRU(store.NewMemory(), 400)

	tx.Commit(*tape, uploads, cache)
	t.Logf("%v", tx.Err)
	if len(tx.Err) != 1 {
		t.Errorf("Expected 1 error, got %d", len(tx.Err))
	}
}
