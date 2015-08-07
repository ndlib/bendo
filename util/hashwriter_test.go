package util

import (
	"bytes"
	"encoding/hex"
	"testing"
)

func TestHashWriter(t *testing.T) {
	const input = "hello1 hello2 hello3 hello4 hello5abcdefghijklmnopqrstuvwxyz0123456789"
	goalMD5, _ := hex.DecodeString("0101fc798d94a730b0f0bf1bd2cc1959")
	goalSHA256, _ := hex.DecodeString("fef15edd82b33633582c723562d192fec2d2003df12d4aeac89df17c279a1658")
	var w = new(bytes.Buffer)
	hw := NewHashWriter(w)
	dohashtest(t, hw, input, goalMD5, goalSHA256)
	w.Reset()
	hw2 := NewMD5Writer(w)
	dohashtest(t, hw2, input, goalMD5, nil)
}

func dohashtest(t *testing.T, hw *HashWriter, input string, goalmd5, goalsha256 []byte) {
	hw.Write([]byte(input))
	h, ok := hw.CheckMD5(goalmd5)
	if !ok {
		t.Fatalf("Got %v, expected %v\n", h, goalmd5)
	}
	h, ok = hw.CheckSHA256(goalsha256)
	if !ok {
		t.Fatalf("Got %v, expected %v\n", h, goalsha256)
	}
}
