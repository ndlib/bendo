package util

import (
	"bytes"
	"crypto/md5"
	"crypto/sha256"
	"hash"
	"io"
)

// VerifyStreamHash checksums the given io.Reader and compares the checksum
// against the provided md5 and sha256 checksums. It returns true if everything
// matches, and false otherwise. Pass in an empty slice to not verify a given
// checksum type. For example, to only verify the SHA256 hash of the reader,
// pass in []byte{} for the md5 parameter.
// The reader is not closed when finished.
func VerifyStreamHash(r io.Reader, md5, sha256 []byte) (bool, error) {
	if len(md5) == 0 && len(sha256) == 0 {
		return true, nil
	}
	hw := NewHashWriterPlain()
	_, err := io.Copy(hw, r)
	var result = true
	if len(md5) > 0 {
		_, ok := hw.CheckMD5(md5)
		result = result && ok
	}
	if len(sha256) > 0 {
		_, ok := hw.CheckSHA256(sha256)
		result = result && ok
	}
	return result, err
}

// An HashWriter wraps an io.Writer and also calculate the MD5 and SHA256 hashes
// of the bytes written.
type HashWriter struct {
	io.Writer // our io.MultiWriter
	md5       hash.Hash
	sha256    hash.Hash
}

// NewHashWriter returns a HashWriter wrapping w.
func NewHashWriter(w io.Writer) *HashWriter {
	hw := &HashWriter{
		md5:    md5.New(),
		sha256: sha256.New(),
	}
	hw.Writer = io.MultiWriter(w, hw.md5, hw.sha256)
	return hw
}

// NewMD5Writer returns a HashWriter wrapping w and only computing an MD5 hash.
func NewMD5Writer(w io.Writer) *HashWriter {
	hw := &HashWriter{
		md5: md5.New(),
	}
	hw.Writer = io.MultiWriter(w, hw.md5)
	return hw
}

// NewHashWriterPlain return a HashWriter that does not wrap an output stream.
// It will just compute the checksums of the data written to it.
func NewHashWriterPlain() *HashWriter {
	hw := &HashWriter{
		md5:    md5.New(),
		sha256: sha256.New(),
	}
	hw.Writer = io.MultiWriter(hw.md5, hw.sha256)
	return hw
}

// CheckMD5 returns the MD5 hash for this writer, and compares it for equality
// with the goal hash passed in. Returns true if goal matches the MD5 hash,
// false otherwise. If the goal is empty then it is treated as matching, and
// true is returned.
func (hw *HashWriter) CheckMD5(goal []byte) ([]byte, bool) {
	var computed []byte
	if hw.md5 != nil {
		computed = hw.md5.Sum(nil)
	}
	ok := len(goal) == 0 || bytes.Compare(goal, computed) == 0
	return computed, ok
}

// CheckSHA256 returns the SHA256 hash for this writer, and compares it for
// equality with the goal hash passed in. Returns true if goal matches the
// SHA256 hash, false otherwise. If the goal is empty then it is treated as
// matching, and true is returned.
func (hw *HashWriter) CheckSHA256(goal []byte) ([]byte, bool) {
	var computed []byte
	if hw.sha256 != nil {
		computed = hw.sha256.Sum(nil)
	}
	ok := len(goal) == 0 || bytes.Compare(goal, computed) == 0
	return computed, ok
}
