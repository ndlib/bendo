package items

import (
	"archive/zip"
	"crypto/md5"
	"crypto/sha256"
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/ndlib/bendo/store"
)

func TestWriteBlob(t *testing.T) {
	ms := store.NewMemory()
	s := New(ms)
	w, err := s.Open("abc", "nobody")
	if err != nil {
		t.Fatalf("Unexpected error %s", err.Error())
	}

	// first use the wrong lengths and hashes
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

	// now use correct information
	bid = writedata(t, w, "hello")
	w.SetSlot("slotname", bid)
	err = w.Close()
	if err != nil {
		t.Fatalf("Got %s, expected nil", err.Error())
	}

	// try opening a second time and see if slot information is copied
	w, err = s.Open("abc", "nobody")
	if err != nil {
		t.Fatalf("Unexpected error %s", err.Error())
	}
	w.Close()

	checkslots(t, s, "abc", []slotTriple{{2, "slotname", bid}})

	// now add a new blob and remove the slot entry for the old one
	w, err = s.Open("abc", "nobody")
	if err != nil {
		t.Fatalf("Unexpected error %s", err.Error())
	}
	w.SetSlot("slotname", 0)
	newBid := writedata(t, w, "The cow jumpes over the moon")
	w.SetSlot("poem", newBid)
	w.Close()

	checkslots(t, s, "abc", []slotTriple{
		{1, "slotname", bid},
		{2, "slotname", bid},
		{3, "slotname", 0},
		{3, "poem", newBid},
	})
}

func TestWriteDuplicate(t *testing.T) {
	ms := store.NewMemory()
	s := New(ms)
	w, err := s.Open("item-name", "nobody")
	if err != nil {
		t.Fatalf("Unexpected error %s", err.Error())
	}

	// write a blob and remember its id
	bid := writedata(t, w, "hello")
	w.SetSlot("slotname", bid)

	// try writing the blob again...see if we get the same id
	bid2 := writedata(t, w, "hello")
	if bid != bid2 {
		t.Errorf("Received %d and expected %d for the blob id", bid2, bid)
	}

	// try writing a blob without hash values...it should make a new one
	bid2, err = w.WriteBlob(strings.NewReader("hello"), 0, nil, nil)
	if err != nil {
		t.Fatalf("Got %s, expected nil", err.Error())
	}
	if bid == bid2 {
		t.Errorf("Received %d and expected something different", bid2)
	}

	err = w.Close()
	if err != nil {
		t.Fatalf("Got %s, expected nil", err.Error())
	}

	// try opening a second time and see if the same id is still returned
	w, err = s.Open("item-name", "nobody")
	if err != nil {
		t.Fatalf("Unexpected error %s", err.Error())
	}
	bid2 = writedata(t, w, "hello")
	if bid != bid2 {
		t.Errorf("Received %d and expected %d for the blob id", bid2, bid)
	}
	err = w.Close()
	if err != nil {
		t.Fatalf("Got %s, expected nil", err.Error())
	}
}

func TestOpenCorrupt(t *testing.T) {
	ms := store.NewMemory()
	s := New(ms)

	// a bad bundle should cause an error when opening for writing
	// make a bad bundle file
	out, err := ms.Create(sugar("abc", 1))
	if err != nil {
		t.Fatalf("Received error %s", err.Error())
	}
	out.Write([]byte("not a valid zip file")) // never fails for memory store
	out.Close()                               // never fails for memory store

	// now try to open
	_, err = s.Open("abc", "nobody")
	if err != zip.ErrFormat {
		t.Fatalf("Received error %s, expected %s", err.Error(), zip.ErrFormat)
	}
}

func TestDeleteBlob(t *testing.T) {
	ms := store.NewMemory()
	s := New(ms)
	w, err := s.Open("abc", "nobody")
	if err != nil {
		t.Fatalf("Unexpected error %s", err.Error())
	}
	w.SetCreator("nobody")

	for i := 0; i < 10; i++ {
		istring := strconv.Itoa(i)
		bid := writedata(t, w, "hello "+istring)
		w.SetSlot("slot"+istring, bid)
	}
	err = w.Close()
	if err != nil {
		t.Fatalf("Got %s, expected nil", err.Error())
	}

	// now delete the first five blobs
	w, err = s.Open("abc", "nobody")
	if err != nil {
		t.Fatalf("Unexpected error %s", err.Error())
	}
	w.DeleteBlob(1)
	w.DeleteBlob(2)
	w.DeleteBlob(3)
	w.DeleteBlob(4)
	w.DeleteBlob(5)
	w.DeleteBlob(1) // delete this one twice!!
	err = w.Close()
	if err != nil {
		t.Fatalf("Got %s, expected nil", err.Error())
	}

	// first bundle should be missing
	_, _, err = ms.Open(sugar("abc", 1))
	if err == nil {
		t.Errorf("Received nil, expected error")
	}

	// is the 10th blob readable?
	rc, _, err := s.Blob("abc", 10)
	if err != nil {
		t.Errorf("Received %s, expected nil", err.Error())
	}
	var p = make([]byte, 32)
	n, _ := rc.Read(p)
	rc.Close()
	if string(p[:n]) != "hello 9" {
		t.Errorf("Received %v, expected 'hello 9'", p)
	}

	// is the 1st blob deleted?
	_, _, err = s.Blob("abc", 1)
	if err == nil {
		t.Errorf("Received nil, expected error")
	}
}

//
// Utility code
//

func writedata(t *testing.T, w *Writer, data string) BlobID {
	t.Logf("writedata '%.10s'", data)
	md5 := md5.Sum([]byte(data))
	sha256 := sha256.Sum256([]byte(data))
	bid, err := w.WriteBlob(strings.NewReader(data),
		int64(len(data)),
		md5[:],
		sha256[:])
	if err != nil {
		t.Fatalf("Got %s, expected nil", err.Error())
	}
	t.Logf("Got blob id %d", bid)
	return bid
}

type slotTriple struct {
	version     VersionID
	slot        string
	expectedBid BlobID
}

func checkslots(t *testing.T, s *Store, id string, table []slotTriple) {
	t.Logf("checkslot %s", id)
	itm, err := s.Item(id)
	if err != nil {
		t.Fatalf("Unexpected error %s", err.Error())
	}
	for _, triple := range table {
		target := itm.BlobByVersionSlot(triple.version, triple.slot)
		t.Logf("found (%d, %s) = %d", triple.version, triple.slot, target)
		if target != triple.expectedBid {
			t.Errorf("Received %d, expected %d", target, triple.expectedBid)
		}

		extslot := fmt.Sprintf("@%d/%s", triple.version, triple.slot)
		target = itm.BlobByExtendedSlot(extslot)
		t.Logf("found (%s) = %d", extslot, target)
		if target != triple.expectedBid {
			t.Errorf("Received %d, expected %d", target, triple.expectedBid)
		}
	}
}
