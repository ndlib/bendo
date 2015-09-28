// Package bagit implements the enough of the BagIt specification to read and
// save the BagIt files used by Bendo. It is tailored to use the Store interface
// instead of directly using the file system. It creates zip files which do
// not use compression. It also only supports MD5 and SHA256 checksums for
// the manifest file.
//
// Specific items not implemented are fetch files and holely bags. It doesn't
// preserve the order of the tags in the bag-info.txt file. It also doesn't
// preserve multiple occurrences of tags in the bag-info.txt file.
//
// This package allows for reading a bag, verifying a bag, and creating new
// bags. It does not provide any services for updating a bag.
// Checksums are generated for each file when a bag is created.
// After that, checksums are only calculated when a bag is (explicitly) verified.
// In particular, checksums are not calculated when reading content from a bag.
//
// The interface is designed to mirror the archive/zip interface as much as
// possible.
//
// The BagIt spec can be found at https://tools.ietf.org/html/draft-kunze-bagit-11.
package bagit

import (
	"archive/zip"
	"encoding/hex"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/ndlib/bendo/util"
)

// T represents a single BagIt file.
type T struct {
	// the bag's name, which is the directory this bag unserializes into.
	dirname string

	// for each file in this bag, the checksums we expect for it.
	// payload files begin with "data/". Tag and control files don't.
	manifest map[string]*Checksum

	// list of tags to be saved in the bag-info.txt file. The key is the
	// tag name, and the value is the content to save for that tag.
	// content strings are not wrapped at column 75 in this implementation.
	tags map[string]string
}

// Checksum contains all the checksums we know about for a given file.
// Some entries may be empty. At least one entry should be present.
type Checksum struct {
	MD5    []byte
	SHA1   []byte
	SHA256 []byte
	SHA512 []byte
}

// access methods used by manifest() below
func (c Checksum) md5() []byte    { return c.MD5 }
func (c Checksum) sha1() []byte   { return c.SHA1 }
func (c Checksum) sha256() []byte { return c.SHA256 }
func (c Checksum) sha512() []byte { return c.SHA512 }

const (
	// Version is the version of the BagIt specification this package implements.
	Version = "0.97"
)

// New creates a new BagIt structure. Is this needed at all?
func New() T {
	return T{
		manifest: make(map[string]*Checksum),
		tags:     make(map[string]string),
	}
}

// Writer allows for writing a new bag file. When it is closed, all the
// relevant tag files and manifests will be written out.
type Writer struct {
	z        *zip.Writer      // the underlying zip writer
	t        T                // out bag structure to track the files
	checksum *Checksum        // pointer to current checksum
	hw       *util.HashWriter // current hash writer
	ns       int              // number of "streams" (i.e. payload files)
	sz       int64            // size of the payload files, in bytes
}

// NewWriter creates a new bag writer which will serialize itself to the
// provided io.Writer. Use name to set the directory name the bag will
// unserialize into, as required by the spec.
func NewWriter(w io.Writer, name string) *Writer {
	t := New()
	t.dirname = name
	return &Writer{
		z: zip.NewWriter(w),
		t: t,
	}
}

// Close this Writer and serialize all necessary bookkeeping files. It does
// not close the original io.Writer provided to NewWriter().
func (w *Writer) Close() error {
	w.t.tags["Payload-Oxum"] = fmt.Sprintf("%d.%d", w.sz, w.ns)
	w.t.tags["Bagging-Date"] = time.Now().Format("2006-01-02")
	w.t.tags["Bag-Size"] = humansize(w.sz)

	w.writeTags()
	w.writeManifests()
	return w.z.Close()
}

// SetTag adds the given tag to this bag, and sets it to be equal to content.
// The bag writer will add the tags "Payload-Oxum", "Bagging-Date", and
// "Bag-Size" itself. Other useful tags are listed in the BagIt specification.
func (w *Writer) SetTag(tag, content string) {
	w.t.tags[tag] = content
}

// Create a new file inside this bag. The file will be put inside the "data/"
// directory.
func (w *Writer) Create(name string) (io.Writer, error) {
	w.ns++
	out, err := w.create("data/" + name)
	return &countWriter{
		w:     out,
		count: &w.sz,
	}, err
}

// create is for internal use. It allows non-payload files to be written.
func (w *Writer) create(name string) (io.Writer, error) {
	// save checksums if there is an active writer
	_ = w.Checksum()

	ck := new(Checksum)
	w.t.manifest[name] = ck
	w.checksum = ck

	header := zip.FileHeader{
		Name:   w.t.dirname + "/" + name,
		Method: zip.Store,
	}
	header.SetModTime(time.Now())
	out, err := w.z.CreateHeader(&header)

	w.hw = util.NewHashWriter(out)

	return w.hw, err
}

// Return the current checksums for the current file returned from Create().
func (w *Writer) Checksum() *Checksum {
	if w.hw != nil && w.checksum != nil {
		w.checksum.MD5, _ = w.hw.CheckMD5(nil)
		w.checksum.SHA256, _ = w.hw.CheckSHA256(nil)
	}
	return w.checksum
}

func (w *Writer) writeTags() {
	// first write bag-it marker file
	out, _ := w.create("bagit.txt")
	fmt.Fprintf(out, "BagIt-version: %s\n", Version)
	fmt.Fprintf(out, "Tag-File-Character-Encoding: UTF-8\n")

	// now write tags file
	out, _ = w.create("bag-info.txt")
	for k, v := range w.t.tags {
		fmt.Fprintf(out, "%s: %s\n", k, v)
	}
	// ensure the checksum is entered into the manifest
	w.Checksum()
}

func (w *Writer) writeManifests() {
	w.manifest(false, "md5", Checksum.md5)
	w.manifest(false, "sha1", Checksum.sha1)
	w.manifest(false, "sha256", Checksum.sha256)
	w.manifest(false, "sha512", Checksum.sha512)

	// do the tagmanifest
	w.manifest(true, "md5", Checksum.md5)
}

func (w *Writer) manifest(istag bool, name string, hash func(Checksum) []byte) {
	var out io.Writer
	for fname, checksum := range w.t.manifest {
		if istag && strings.HasPrefix(fname, "data/") {
			continue
		}
		if !istag && !strings.HasPrefix(fname, "data/") {
			continue
		}
		h := hash(*checksum)
		if len(h) == 0 {
			continue
		}
		if out == nil {
			var mname string
			if istag {
				mname = "tagmanifest-" + name + ".txt"
			} else {
				mname = "manifest-" + name + ".txt"
			}
			// TODO(dbrower): check for error?
			out, _ = w.create(mname)
		}
		fmt.Fprintf(out, "%s %s\n", hex.EncodeToString(h), fname)
	}
}

type countWriter struct {
	w     io.Writer
	count *int64
}

func (w *countWriter) Write(p []byte) (int, error) {
	n, err := w.w.Write(p)
	*w.count += int64(n)
	return n, err
}

// Metric constants for humansize. Lowercased so as to be unexported.
const (
	kb int64 = 1000
	mb       = 1000 * kb
	gb       = 1000 * mb
	tb       = 1000 * gb
)

func humansize(size int64) string {
	var units string
	switch {
	case size < kb:
		units = "Bytes"
	case size < mb:
		size /= kb
		units = "KB"
	case size < gb:
		size /= mb
		units = "MB"
	case size < tb:
		size /= gb
		units = "GB"
	default:
		size /= tb
		units = "TB"
	}
	return fmt.Sprintf("%d %s", size, units)
}
