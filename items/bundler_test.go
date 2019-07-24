package items

import (
	"strings"
	"testing"

	"github.com/ndlib/bendo/store"
)

func TestBundleWriter(t *testing.T) {
	ms := store.NewMemory()
	item := &Item{ID: "12345"}
	bw := NewBundler(ms, item)
	if bw.CurrentBundle() != 1 {
		t.Fatalf("CurrentBundle() == %d, expected 1", bw.CurrentBundle())
	}
	blob := &Blob{ID: 1}
	item.Blobs = append(item.Blobs, blob)
	result, err := bw.WriteBlob(blob, strings.NewReader("Hello There"))
	if err != nil {
		t.Fatalf("WriteBlob() == %s, expected nil", err.Error())
	}
	blob.Bundle = result.Bundle
	err = bw.Close()
	if err != nil {
		t.Fatalf("Close() == %s, expected nil", err.Error())
	}
	_, _, err = ms.Open("12345-0001.zip")
	if err != nil {
		t.Fatalf("Expected bundle 12345-0001.zip to exist")
	}
	_, _, err = ms.Open("12345-0002.zip")
	if err == nil {
		t.Fatalf("Expected bundle 12345-0002.zip to not exist.")
	}
	helloR, err := OpenBundleStream(ms, "12345-0001.zip", "blob/1")
	if err != nil {
		t.Fatalf("OpenBundleStream() == %s, expected nil", err.Error())
	}
	var hello = make([]byte, 20)
	n, _ := helloR.Read(hello)
	if n != 11 {
		t.Fatalf("Blob/1 length = %d, expected 11", n)
	}
	if string(hello)[:n] != "Hello There" {
		t.Fatalf("Blob/1 = %#v, expected \"Hello There\"", string(hello))
	}
}

func TestOpenAtCreation(t *testing.T) {
	ms := store.NewMemory()
	item := &Item{ID: "12345"}
	bw := NewBundler(ms, item)
	if bw.CurrentBundle() != 1 {
		t.Fatalf("CurrentBundle() == %d, expected 1", bw.CurrentBundle())
	}
	err := bw.Close()
	if err != nil {
		t.Fatalf("Close() == %s, expected nil", err.Error())
	}
	_, _, err = ms.Open("12345-0001.zip")
	if err != nil {
		t.Fatalf("Expected bundle 12345-0001.zip to exist")
	}
}
