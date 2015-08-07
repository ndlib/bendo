package util

import (
	"bytes"
	"crypto/md5"
	"crypto/sha256"
	"hash"
	"io"
)

// An io.Writer which will also calculate the md5 and sha256 sums of the input stream.
type HashWriter struct {
	io.Writer // our io.MultiWriter
	md5       hash.Hash
	sha256    hash.Hash
}

func NewHashWriter(w io.Writer) *HashWriter {
	hw := &HashWriter{
		md5:    md5.New(),
		sha256: sha256.New(),
	}
	hw.Writer = io.MultiWriter(w, hw.md5, hw.sha256)
	return hw
}

func NewMD5Writer(w io.Writer) *HashWriter {
	hw := &HashWriter{
		md5: md5.New(),
	}
	hw.Writer = io.MultiWriter(w, hw.md5)
	return hw
}

// Returns the MD5 hash for this writer. Also returns true if either
// goal is empty or goal is provided, and it matches the sum
func (hw *HashWriter) CheckMD5(goal []byte) ([]byte, bool) {
	var computed []byte
	if hw.md5 != nil {
		computed = hw.md5.Sum(nil)
	}
	ok := len(goal) == 0 || bytes.Compare(goal, computed) == 0
	return computed, ok
}

func (hw *HashWriter) CheckSHA256(goal []byte) ([]byte, bool) {
	var computed []byte
	if hw.sha256 != nil {
		computed = hw.sha256.Sum(nil)
	}
	ok := len(goal) == 0 || bytes.Compare(goal, computed) == 0
	return computed, ok
}
