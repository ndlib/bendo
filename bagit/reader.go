package bagit

import (
	"archive/zip"
	"bufio"
	"encoding/hex"
	"errors"
	"io"
	"strings"

	"github.com/ndlib/bendo/util"
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
// Closing a reader does not close the wrapped ReaderAt.
func NewReader(r io.ReaderAt, size int64) (*Reader, error) {
	in, err := zip.NewReader(r, size)
	if err != nil {
		return nil, err
	}
	result := &Reader{
		z: in,
		t: New(),
	}
	// are there any files inside the zip?
	if len(in.File) > 0 {
		// according to bagit spec, EVERYTHING in the zip
		// should be inside the same directory, so take the first
		// file inside and figure out its top-most directory name.
		paths := strings.SplitN(in.File[0].Name, "/", 2)
		if len(paths) == 2 {
			result.t.dirname = paths[0] + "/"
		}
	}
	return result, nil
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
	rc, err := r.open(name)
	if err != nil {
		return err
	}
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
					" " + strings.TrimSpace(line)
			}
			continue
		}
		// otherwise split on first colon
		pieces := strings.SplitN(line, ":", 2)
		if len(pieces) != 2 {
			// no colon?
			continue
		}
		previousKey = strings.TrimSpace(pieces[0])
		r.t.tags[previousKey] = strings.TrimSpace(pieces[1])
	}
	return scanner.Err()
}

// Checksum returns a pointer to the Checksum struct of the given filername
// culled from the manifests map. It assumes that the request file
// resides in the data directory of the bag, and so prepends "data/" to the filename
// provided. If no entry exists for the given file, it returns nil
func (r *Reader) Checksum(name string) *Checksum {

	// load the manifest files in from wherever

	if len(r.t.manifest) == 0 {
		err := r.loadManifests()

		if err != nil {
			return nil // a null *Checksum
		}
	}

	return r.t.manifest["data/"+name]
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

// Possible verification errors.
var (
	ErrExtraFile   = errors.New("bagit: extra file")
	ErrMissingFile = errors.New("bagit: missing file")
	ErrChecksum    = errors.New("bagit: checksum mismatch")
)

// BagError is used to hold a validation error and the file in the bag which
// has the error.
type BagError struct {
	Err  error
	File string
}

func (err BagError) Error() string {
	return err.Err.Error() + " " + err.File
}

// Verify computes the checksum of each file in this bag, and checks it against
// the manifest files. Both payload ("data/") and tag files are checked.
// The file list is read from manifest files for MD5, SHA1, SHA256, and SHA512
// hashes, although only MD5 and SHA256 hashes are actually computed and verified.
// Files missing an entry in a manifest file, or manifest entires missing a
// corresponding file will cause a verification error. Tag files which are
// missing a manifest entry are the only exception to the verification error.
func (r *Reader) Verify() error {
	err := r.loadManifests()
	if err != nil {
		return err
	}

	// Check the quick stuff first, and do the time consuming checksum
	// verification last.
	var dataprefix = r.t.dirname + "data/"
	var npayload int

	// Does every payload ("data/") file in this zip appear in our manifest
	// map? tag files may also appear in the map, we don't care yet.
	//
	// We need to do some pathname manipulation since the zip directory
	// names have the form "bagname/data/blah/blah" but the manifest
	// has names of the form "data/blah/blah".
	for _, f := range r.z.File {
		if !strings.HasPrefix(f.Name, dataprefix) {
			continue
		}
		npayload++
		xname := strings.TrimPrefix(f.Name, r.t.dirname)
		if r.t.manifest[xname] == nil {
			return BagError{Err: ErrExtraFile, File: xname}
		}
	}

	// Does every file listed in the manifest exist in the zip file?
	//
	// The loop above ensures npayload <= nmanifest.
	// so if nmanifest != npayload, it must be because it contains files
	// not present in the zip file. The downside of this method is we do
	// not know the name of the missing file.
	var nmanifest int
	for k := range r.t.manifest {
		if strings.HasPrefix(k, "data/") {
			nmanifest++
		}
	}
	if npayload != nmanifest {
		return BagError{Err: ErrMissingFile}
	}

	// Do all the checksums match?
	// Since t.manifest includes both payload and data files, we will
	// verify more than nmanifest files here.
	for _, f := range r.z.File {
		xname := strings.TrimPrefix(f.Name, r.t.dirname)
		checksum := r.t.manifest[xname]
		if checksum == nil {
			// this file is not in the manifest. We don't care
			// since we have already verified all the payload
			// files are accounted for.
			continue
		}
		in, err := f.Open()
		if err != nil {
			return err
		}
		ok, err := util.VerifyStreamHash(in, checksum.MD5, checksum.SHA256)
		_ = in.Close()
		if err != nil {
			return err
		} else if !ok {
			// checksum error!
			return BagError{Err: ErrChecksum, File: xname}
		}
	}

	return nil
}

type chksumSetter func(*Checksum, []byte)

func (r *Reader) loadManifests() error {
	var filelist = []struct {
		filename string
		setfunc  chksumSetter
	}{
		{"manifest-md5.txt", (*Checksum).setmd5},
		{"manifest-sha1.txt", (*Checksum).setsha1},
		{"manifest-sha256.txt", (*Checksum).setsha256},
		{"manifest-sha512.txt", (*Checksum).setsha512},
		{"tagmanifest-md5.txt", (*Checksum).setmd5},
	}
	for _, entry := range filelist {
		err := r.loadManifestFile(entry.filename, entry.setfunc)
		if err != nil && err != ErrNotFound {
			return err
		}
	}
	return nil
}

func (r *Reader) loadManifestFile(name string, fset chksumSetter) error {
	rc, err := r.open(name)
	if err != nil {
		return err
	}
	defer rc.Close()
	scanner := bufio.NewScanner(rc)
	for scanner.Scan() {
		// each checksum line should look like
		// HEXDIGEST  a/file/name
		line := scanner.Text()
		if len(line) == 0 {
			continue
		}
		pieces := strings.Fields(line)
		if len(pieces) != 2 {
			return err
		}
		fname := pieces[1]
		chksm := r.t.manifest[fname]
		if chksm == nil {
			chksm = new(Checksum)
			r.t.manifest[fname] = chksm
		}
		h, err := hex.DecodeString(pieces[0])
		if err != nil {
			return err
		}
		fset(chksm, h)
	}
	return scanner.Err()
}
