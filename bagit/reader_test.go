package bagit

import (
	"archive/zip"
	"encoding/hex"
	"io"
	"testing"
	"time"

	"github.com/ndlib/bendo/store"
)

type zdata map[string]string

func TestVerify(t *testing.T) {
	var table = []struct {
		name     string
		contents zdata
		ok       bool
	}{
		// payload files split between two manifests
		{"ok-1", zdata{
			"data/hello1":         "hello",
			"data/hello2":         "hello",
			"manifest-md5.txt":    "5d41402abc4b2a76b9719d911017c592 data/hello1\n",
			"manifest-sha256.txt": "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824 data/hello2\n",
			"tagmanifest-md5.txt": "49ce66cef8d32ec33eca290c2c731185 manifest-md5.txt\nbd41f3fc8aa771760265275d3576a30a manifest-sha256.txt\n",
		}, true},
		// extra payload file
		{"extra-1", zdata{
			"data/hello1":         "hello",
			"data/hello2":         "hello",
			"manifest-md5.txt":    "5d41402abc4b2a76b9719d911017c592 data/hello1\n",
			"tagmanifest-md5.txt": "49ce66cef8d32ec33eca290c2c731185 manifest-md5.txt\n",
		}, false},
		// missing payload file
		{"extra-2", zdata{
			"data/hello1":         "hello",
			"manifest-md5.txt":    "5d41402abc4b2a76b9719d911017c592 data/hello1\n",
			"manifest-sha256.txt": "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824 data/hello2\n",
			"tagmanifest-md5.txt": "49ce66cef8d32ec33eca290c2c731185 manifest-md5.txt\nbd41f3fc8aa771760265275d3576a30a manifest-sha256.txt\n",
		}, false},
		// missing tag file
		{"extra-3", zdata{
			"data/hello1":         "hello",
			"data/hello2":         "hello",
			"manifest-md5.txt":    "5d41402abc4b2a76b9719d911017c592 data/hello1\n",
			"manifest-sha256.txt": "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824 data/hello2\n",
			"tagmanifest-md5.txt": "49ce66cef8d32ec33eca290c2c731185 manifest-md5.txt\nbd41f3fc8aa771760265275d3576a30a manifest-sha256.txt\nabcdef missing.txt\n",
		}, true},
		// mismatch payload file
		{"checksum-1", zdata{
			"data/hello1":         "hello",
			"data/hello2":         "hello",
			"manifest-md5.txt":    "00000000000000000000000000000000 data/hello1\n",
			"manifest-sha256.txt": "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824 data/hello2\n",
			"tagmanifest-md5.txt": "d0d355c1ef01ef6a24b68112d62b1700 manifest-md5.txt\nbd41f3fc8aa771760265275d3576a30a manifest-sha256.txt\n",
		}, false},
		// mismatch tag file
		{"checksum-2", zdata{
			"data/hello1":         "hello",
			"data/hello2":         "hello",
			"manifest-md5.txt":    "5d41402abc4b2a76b9719d911017c592 data/hello1\n",
			"manifest-sha256.txt": "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824 data/hello2\n",
			"tagmanifest-md5.txt": "00000000000000000000000000000000 manifest-md5.txt\nbd41f3fc8aa771760265275d3576a30a manifest-sha256.txt\n",
		}, false},
		// extra tag file
		{"checksum-3", zdata{
			"data/hello1":         "hello",
			"data/hello2":         "hello",
			"tagfile.txt":         "extra tag file",
			"manifest-md5.txt":    "5d41402abc4b2a76b9719d911017c592 data/hello1\n",
			"manifest-sha256.txt": "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824 data/hello2\n",
			"tagmanifest-md5.txt": "49ce66cef8d32ec33eca290c2c731185 manifest-md5.txt\nbd41f3fc8aa771760265275d3576a30a manifest-sha256.txt\n",
		}, true},
		// manifest not hex
		{"manifest-1", zdata{
			"data/hello1":         "hello",
			"data/hello2":         "hello",
			"tagfile.txt":         "extra tag file",
			"manifest-md5.txt":    "thisisnothexdata0000000000000000 data/hello1\n",
			"manifest-sha256.txt": "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824 data/hello2\n",
			"tagmanifest-md5.txt": "f6c4e3fa0e551b551b1fc171f01c1bdf manifest-md5.txt\nbd41f3fc8aa771760265275d3576a30a manifest-sha256.txt\n",
		}, false},
		// malformed manifest -- missing final newline
		{"manifest-2", zdata{
			"data/hello1":         "hello",
			"data/hello2":         "hello",
			"manifest-md5.txt":    "5d41402abc4b2a76b9719d911017c592 data/hello1",
			"manifest-sha256.txt": "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824 data/hello2\n",
			"tagmanifest-md5.txt": "2afc9fa64386fe74f0500bc6f83b9d9c manifest-md5.txt\nbd41f3fc8aa771760265275d3576a30a manifest-sha256.txt\n",
		}, true},
		// manifest line only has hash
		{"manifest-3", zdata{
			"data/hello1":         "hello",
			"data/hello2":         "hello",
			"manifest-md5.txt":    "5d41402abc4b2a76b9719d911017c592 data/hello1\n",
			"manifest-sha256.txt": "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824\n",
			"tagmanifest-md5.txt": "2afc9fa64386fe74f0500bc6f83b9d9c manifest-md5.txt\nbd41f3fc8aa771760265275d3576a30a manifest-sha256.txt\n",
		}, false},
	}

	mstore := store.NewMemory()
	for _, tab := range table {
		t.Logf("Doing %s", tab.name)
		f, err := mstore.Create(tab.name)
		if err != nil {
			t.Fatal(err)
		}
		makezipfile(f, tab.contents)
		f.Close()

		f2, size, err := mstore.Open(tab.name)
		if err != nil {
			t.Fatal(err)
		}

		r, err := NewReader(f2, size)
		if err != nil {
			t.Error(err)
		}
		err = r.Verify()
		if tab.ok && err != nil {
			t.Errorf("Error, valid returned %s", err.Error())
		} else if !tab.ok && err == nil {
			t.Errorf("Error, valid returned nil")
		}
		f2.Close()
	}
}

func TestTagParser(t *testing.T) {
	var table = []struct {
		name     string
		contents zdata
		tags     map[string]string
	}{
		// Parse normal tag file
		{"ok-1",
			zdata{
				"bag-info.txt": "a-tag: some text\nanother-tag: more text\n  extended line",
			},
			map[string]string{
				"a-tag":       "some text",
				"another-tag": "more text extended line",
			}},
		{"ok-2",
			zdata{
				"bag-info.txt": "first tag:important\nthis line is skipped\n\n this line continues the first\n",
			},
			map[string]string{
				"first tag": "important this line continues the first",
			}},
	}

	mstore := store.NewMemory()
	for _, tab := range table {
		t.Logf("Doing %s", tab.name)
		f, err := mstore.Create(tab.name)
		if err != nil {
			t.Fatal(err)
		}
		makezipfile(f, tab.contents)
		f.Close()

		f2, size, err := mstore.Open(tab.name)
		if err != nil {
			t.Fatal(err)
		}

		r, err := NewReader(f2, size)
		if err != nil {
			t.Error(err)
		}
		tags := r.Tags()
		if !mapsEqual(tags, tab.tags) {
			t.Errorf("tags unequal received %#v, expected %#v",
				tags,
				tab.tags)
		}
		f2.Close()
	}
}

func makezipfile(w io.Writer, contents zdata) {
	const dirname = "test/"
	z := zip.NewWriter(w)
	for k, v := range contents {
		header := zip.FileHeader{
			Name:   dirname + k,
			Method: zip.Store,
		}
		header.SetModTime(time.Now())
		out, _ := z.CreateHeader(&header)
		// this should check the number of bytes written, and loop
		out.Write([]byte(v))
	}
	z.Close()
}

func mapsEqual(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v1 := range a {
		v2 := b[k]
		if v1 != v2 {
			return false
		}
	}
	return true
}

func TestChecksum(t *testing.T) {
	
	// Test Data for Checksums tests
	cksum  := zdata{
			"data/hello1":         "hello",
			"data/hello2":         "hello",
			"manifest-md5.txt":    "5d41402abc4b2a76b9719d911017c592 data/hello1\n",
			"manifest-sha256.txt": "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824 data/hello2\n",
			"tagmanifest-md5.txt": "49ce66cef8d32ec33eca290c2c731185 manifest-md5.txt\nbd41f3fc8aa771760265275d3576a30a manifest-sha256.txt\n",
	}

	mstore := store.NewMemory()
		f, err := mstore.Create("cksum")
		if err != nil {
			t.Fatal(err)
		}
		makezipfile(f, cksum )
		f.Close()

		f2, size, err := mstore.Open("cksum")
		if err != nil {
			t.Fatal(err)
		}

		r, err := NewReader(f2, size)
		if err != nil {
			t.Error(err)
		}

		// Verify Existing Key can be retrieved
		if r.Checksum("hello1") == nil {
			t.Error("Checksum for file 'data/hello1' returns nil")
		}

		// Verify Existing Key can be retrieved
		if r.Checksum("hello2") == nil {
			t.Error("Checksum for file 'data/hello2' returns nil")
		}

		// Verify Checksum of Existing Key is correct
		if hex.EncodeToString(r.Checksum("hello2").SHA256) != "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824" {
			t.Error("Checksum.SHA256 for file 'data/hello2' returns nil")
		}

		// Verify NonExistent Key Fails
		if r.Checksum("hello3") != nil {
			t.Error("Checksum for nonexistent file 'data/hello3' returns value")
		}

		f2.Close()
}
