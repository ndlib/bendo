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

// Writer allows for writing a new bag file. When it is closed, all the
// relevant tag files and manifests will be written out.
type Writer struct {
	z        *zip.Writer      // the underlying zip writer
	t        Bag              // our bag structure to track the files
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
	t.dirname = name + "/"
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

	// If Close() is called after a write error, then this first
	// call will also fail with an error.
	err := w.writeTags()
	if err != nil {
		return err
	}
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
	// save checksums in case there is an active writer
	_ = w.Checksum()

	ck := new(Checksum)
	w.t.manifest[name] = ck
	w.checksum = ck

	header := zip.FileHeader{
		Name:   w.t.dirname + name,
		Method: zip.Store,
	}
	header.SetModTime(time.Now())
	out, err := w.z.CreateHeader(&header)

	w.hw = util.NewHashWriter(out)

	return w.hw, err
}

// Checksum returns the checksums for what has been written so far to the
// last io.Writer returned by Create().
func (w *Writer) Checksum() *Checksum {
	if w.hw != nil && w.checksum != nil {
		w.checksum.MD5, _ = w.hw.CheckMD5(nil)
		w.checksum.SHA256, _ = w.hw.CheckSHA256(nil)
	}
	return w.checksum
}

func (w *Writer) writeTags() error {
	// first write bag-it marker file
	out, err := w.create("bagit.txt")
	if err != nil {
		return err
	}
	fmt.Fprintf(out, "BagIt-Version: %s\n", Version)
	fmt.Fprintf(out, "Tag-File-Character-Encoding: UTF-8\n")

	// now write tags file
	out, err = w.create("bag-info.txt")
	if err != nil {
		return err
	}
	for k, v := range w.t.tags {
		fmt.Fprintf(out, "%s: %s\n", k, v)
	}
	return nil
}

func (w *Writer) writeManifests() {
	// ensure any pending checksum is saved
	_ = w.Checksum()

	w.manifest(false, "md5", Checksum.md5)
	w.manifest(false, "sha1", Checksum.sha1)
	w.manifest(false, "sha256", Checksum.sha256)
	w.manifest(false, "sha512", Checksum.sha512)

	// do the tagmanifest
	w.manifest(true, "md5", Checksum.md5)
}

// access methods used by manifest() below
func (c Checksum) md5() []byte    { return c.MD5 }
func (c Checksum) sha1() []byte   { return c.SHA1 }
func (c Checksum) sha256() []byte { return c.SHA256 }
func (c Checksum) sha512() []byte { return c.SHA512 }

func (w *Writer) manifest(istag bool, name string, hash func(Checksum) []byte) {
	var out io.Writer
	for fname, checksum := range w.t.manifest {
		// tag manifests only include files NOT having the prefix "data/"
		if istag && strings.HasPrefix(fname, "data/") {
			continue
		}
		// non-tag manifests only include "data/" files
		if !istag && !strings.HasPrefix(fname, "data/") {
			continue
		}
		h := hash(*checksum)
		// does this file have a checksum of this type?
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
			out, _ = w.create(mname)
		}
		// The 2 spaces is to be identical to the GNU md5sum output.
		// Although md5sum outputs " *" to mark binary mode, that
		// results in each file name being prefixed with an asterisk.
		fmt.Fprintf(out, "%s  %s\n", hex.EncodeToString(h), fname)
	}
}

// countWriter is an io.Writer that counts the number of bytes written to it.
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
