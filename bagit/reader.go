package bagit

import (
	"archive/zip"
	"bufio"
	"errors"
	"fmt"
	"io"
	"strings"
)

type Reader struct {
	z *zip.Reader
	t T
}

// NewReader creates a bag reader which wraps r. It expects a ZIP datastream,
// and uses size to locate the zip manifest block, which is at the end.
//
// The checksums are not checked upon opening. Call Verify() to verify all the
// checksums. Tags are loaded lazily from the tag file. Ask for a tag to force
// the tag file to be read.
//
// Closing a reader does not close the underlying ReaderAt.
func NewReader(r io.ReaderAt, size int64) (*Reader, error) {
	var result *Reader
	in, err := zip.NewReader(r, size)
	if err == nil {
		result = &Reader{
			z: in,
			t: New(),
		}
		if len(result.z.File) > 0 {
			paths := strings.SplitN(result.z.File[0].Name, "/", 2)
			if len(paths) == 2 {
				result.t.dirname = paths[0]
			}
		}
	}
	return result, err
}

func (c *Checksum) setmd5(b []byte)    { c.MD5 = b }
func (c *Checksum) setsha1(b []byte)   { c.SHA1 = b }
func (c *Checksum) setsha256(b []byte) { c.SHA256 = b }
func (c *Checksum) setsha512(b []byte) { c.SHA512 = b }

func (r *Reader) readmanifest(name string) {
}

// Open returns a reader for the file having the given name.
// Note, that inside the bag, the file is searched for from the path
// "<bag name>/data/<name>".
func (r *Reader) Open(name string) (io.ReadCloser, error) {
	return r.open("data/" + name)
}

var (
	// ErrNotFound means a stream inside a zip file with the given name
	// could not be found.
	ErrNotFound = errors.New("stream not found")
)

// open will open any file, not necessarily one inside the data directory.
func (r *Reader) open(name string) (io.ReadCloser, error) {
	xname := r.t.dirname + "/" + name
	for _, f := range r.z.File {
		if f.Name != xname {
			continue
		}
		return f.Open()
	}
	return nil, ErrNotFound
}

func (r *Reader) Tags() map[string]string {
	if len(r.t.tags) == 0 {
		r.loadtagfile("bagit.txt")
		r.loadtagfile("bag-info.txt")
	}
	return r.t.tags
}

func (r *Reader) loadtagfile(name string) {
	rc, _ := r.open(name)
	defer rc.Close()
	scanner := bufio.NewScanner(rc)
	for scanner.Scan() {
		// line beginning with white space is a continuation.
		// otherwise split on first colon
		fmt.Println(scanner.Text())
	}
	err := scanner.Err()
	if err == io.EOF {
	}
	if err := scanner.Err(); err != nil {
	}

}
