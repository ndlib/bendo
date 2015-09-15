package items

import (
	"crypto/md5"
	"crypto/sha256"
	"strings"
	"testing"

	"github.com/ndlib/bendo/store"
)

func TestWriteBlobErrors(t *testing.T) {
	ms := store.NewFileSystem(".")
	s := New(ms)
	w, err := s.Open("abc")
	if err != nil {
		t.Fatalf("Unexpected error %s", err.Error())
	}
	w.SetCreator("nobody")
	bid, err := w.WriteBlob(strings.NewReader("hello world"),
		3,   // wrong length
		nil, // wrong md5
		nil) // wrong sha256
	if err == nil {
		t.Fatalf("Got nil err, expected an error")
	}
	if bid != 0 {
		t.Fatalf("Got blob id %d, expected 0", bid)
	}
	data := "hello"
	md5 := md5.Sum([]byte(data))
	sha256 := sha256.Sum256([]byte(data))
	bid, err = w.WriteBlob(strings.NewReader(data),
		int64(len(data)),
		md5[:],
		sha256[:])
	if err != nil {
		t.Fatalf("Got %s, expected nil", err.Error())
	}
	t.Logf("Got blob id %d", bid)
	err = w.Close()
	if err != nil {
		t.Fatalf("Got %s, expected nil", err.Error())
	}
}
