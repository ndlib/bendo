package bagit

import (
	"archive/zip"
	"bufio"
	"encoding/hex"
	"errors"
	"io"
	"strings"
)

// Reader allows for reading an existing Bag file (in ZIP format).
//
// A Reader does not validate checksums or load tags until asked to do so.
// Use Verify() to hash and verify all the manifests.
// Call Tags() to load the tag files "bagit.txt" and "bag-info.txt". Other
// tag files are ignored. Since tags are stored in a map, the order of the tags
// is not preserved.
type Reader struct {
	z *zip.Reader
	t Bag
}

// NewReader creates a bag reader which wraps r. It expects a ZIP datastream,
// and uses size to locate the zip manifest block, which is at the end.
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
				result.t.dirname = paths[0] + "/"
			}
		}
	}
	return result, err
}

func (c *Checksum) setmd5(b []byte)    { c.MD5 = b }
func (c *Checksum) setsha1(b []byte)   { c.SHA1 = b }
func (c *Checksum) setsha256(b []byte) { c.SHA256 = b }
func (c *Checksum) setsha512(b []byte) { c.SHA512 = b }

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
	xname := r.t.dirname + name
	for _, f := range r.z.File {
		if f.Name != xname {
			continue
		}
		return f.Open()
	}
	return nil, ErrNotFound
}

// Tags returns a map giving all of the tags stored in this bag. The keys
// are the tag names, and the values are the value of the tag. Since tags
// are returned as a map, the order of the tags in the tag files is not
// preserved. Also, if a tag should occur more than one time, only the last
// occurrence is returned.
//
// The tag files "bagit.txt" and "bag-info.txt" are read. Other tag files
// are ignored.
func (r *Reader) Tags() map[string]string {
	if len(r.t.tags) == 0 {
		r.loadtagfile("bagit.txt")
		r.loadtagfile("bag-info.txt")
	}
	return r.t.tags
}

func (r *Reader) loadtagfile(name string) error {
	var previousKey string
	// TODO(dbrower): handle errors on open?
	rc, _ := r.open(name)
	defer rc.Close()
	scanner := bufio.NewScanner(rc)
	for scanner.Scan() {
		line := scanner.Text()
		if len(line) == 0 {
			continue
		}
		// lines beginning with white space are continuations
		if line[0] == ' ' || line[0] == '\t' {
			if previousKey != "" {
				r.t.tags[previousKey] = r.t.tags[previousKey] +
					strings.TrimSpace(line)
			}
			continue
		}
		// otherwise split on first colon
		pieces := strings.SplitN(line, ":", 2)
		if len(pieces) != 2 {
			// no colon?
			continue
		}
		previousKey = pieces[0]
		r.t.tags[previousKey] = strings.TrimSpace(pieces[1])
	}
	return scanner.Err()
}

// Files returns a list of the payload files inside this bag (as opposed to
// the tag and manifest files). The initial "data/" prefix is removed from
// the file names.
//
// The result is recalculated each time the function is called. For large bags
// generating the result could take a while.
func (r *Reader) Files() []string {
	var result []string
	var prefix = r.t.dirname + "data/"
	for _, f := range r.z.File {
		name := f.Name
		xname := strings.TrimPrefix(name, prefix)
		if len(name) != len(xname) {
			result = append(result, xname)
		}
	}
	return result
}

// Verify computes the checksum of each file in this bag, and checks it against
// the manifest files. Both payload ("data/") and tag files are checked.
// The file list is read from manifest files for MD5, SHA1, SHA256, and SHA512
// hashes, although only MD5 and SHA256 hashes are actually computed and verified.
// Files missing an entry in a manifest file, or manifest entires missing a
// corresponding file will cause a verification error.
func (r *Reader) Verify() {
	r.loadManifests()
}

func (r *Reader) loadManifests() {
	// TODO(dbrower): check for errors
	r.loadManifestFile("manifest-md5", (*Checksum).setmd5)
	r.loadManifestFile("manifest-sha1", (*Checksum).setsha1)
	r.loadManifestFile("manifest-sha256", (*Checksum).setsha256)
	r.loadManifestFile("manifest-sha512", (*Checksum).setsha512)

	r.loadManifestFile("tagmanifest-md5", (*Checksum).setmd5)
}

func (r *Reader) loadManifestFile(name string, fset func(*Checksum, []byte)) error {
	// TODO(dbrower): handle errors on open?
	rc, _ := r.open(name)
	defer rc.Close()
	scanner := bufio.NewScanner(rc)
	for scanner.Scan() {
		line := scanner.Text()
		if len(line) == 0 {
			continue
		}
		pieces := strings.Fields(line)
		if len(pieces) != 2 {
			continue
		}
		fname := pieces[1]
		chksm := r.t.manifest[fname]
		if chksm == nil {
			chksm = new(Checksum)
			r.t.manifest[fname] = chksm
		}
		h, err := hex.DecodeString(pieces[0])
		if err != nil {
			continue
		}
		fset(chksm, h)
	}
	return scanner.Err()
}
