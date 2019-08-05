package main

import (
	"bytes"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/ndlib/bendo/items"
	"github.com/ndlib/bendo/store"
	"github.com/ndlib/bendo/util"
)

var (
	storeDir = flag.String("storage", ".", "location of the storage directory")
	creator  = flag.String("creator", "butil", "Creator name to use")
	verbose  = flag.Bool("v", false, "Display more information")
	usage    = `
butil <command> <command arguments>

Possible commands:
    blob <item id> <blob number>

    item <item id list>

    list

    add <item id> <file/directory list>

    set <item id> <file/directory list>

    identify-missing-blobs

    fix-missing-blobs <item id list>
`
)

func main() {
	flag.Parse()

	fmt.Printf("Using storage dir %s\n", *storeDir)
	fs := store.NewFileSystem(*storeDir)
	r := items.New(fs)

	args := flag.Args()

	if len(args) == 0 {
		return
	}

	switch args[0] {
	case "blob":
		doblob(r, args[1], args[2])
	case "item":
		doitem(r, args[1:])
	case "list":
		dolist(r)
	case "add":
		doadd(r, args[1], args[2:], false)
	case "set":
		doadd(r, args[1], args[2:], true)
	case "delete":
		dodelete(r, args[1], args[2:])
	case "identify-missing-blobs":
		doidentifymissingblobs(r)
	case "fix-missing-blobs":
		dofixmissingblobs(fs, r, args[1:])
	}
}

func doblob(r *items.Store, id, blob string) {
	bid, _ := strconv.Atoi(blob)

	rc, _, err := r.Blob(id, items.BlobID(bid))
	if err != nil {
		fmt.Printf("%s / %d: Error %s\n", id, bid, err.Error())
	} else {
		io.Copy(os.Stdout, rc)
		rc.Close()
	}
}

func doitem(r *items.Store, ids []string) {
	for _, id := range ids {
		item, err := r.Item(id)
		if err != nil {
			fmt.Printf("%s: Error %s\n", id, err.Error())
			return
		}
		printitem(item)
	}
}

func printitem(item *items.Item) {
	fmt.Println("Item:", item.ID)
	fmt.Println("Num Versions:", len(item.Versions))
	fmt.Println("Num Blobs:", len(item.Blobs))
	fmt.Println("Num Bundles:", item.MaxBundle)
	for _, vers := range item.Versions {
		fmt.Println("---")
		w := tabwriter.NewWriter(os.Stdout, 5, 1, 3, ' ', 0)
		fmt.Fprintf(w, "Version:\t%d\n", vers.ID)
		fmt.Fprintf(w, "SaveDate:\t%v\n", vers.SaveDate)
		fmt.Fprintf(w, "Creator:\t%s\n", vers.Creator)
		fmt.Fprintf(w, "Note:\t%s\n", vers.Note)
		w.Flush()
		if !*verbose {
			continue
		}
		fmt.Printf(" Blob  Slot\n")
		for _, r := range sortBySlot(vers.Slots) {
			fmt.Printf("%5d  %s\n", r.blob, r.slot)
		}
	}
	if !*verbose {
		return
	}
	for _, blob := range item.Blobs {
		fmt.Println("---")
		w := tabwriter.NewWriter(os.Stdout, 5, 1, 3, ' ', 0)
		fmt.Fprintf(w, "Blob:\t%d\n", blob.ID)
		fmt.Fprintf(w, "SaveDate:\t%v\n", blob.SaveDate)
		fmt.Fprintf(w, "Creator:\t%s\n", blob.Creator)
		fmt.Fprintf(w, "Size:\t%d\n", blob.Size)
		fmt.Fprintf(w, "Bundle:\t%d\n", blob.Bundle)
		fmt.Fprintf(w, "MD5:\t%s\n", hex.EncodeToString(blob.MD5))
		fmt.Fprintf(w, "SHA256:\t%s\n", hex.EncodeToString(blob.SHA256))
		fmt.Fprintf(w, "DeleteDate:\t%v\n", blob.DeleteDate)
		fmt.Fprintf(w, "Deleter:\t%v\n", blob.Deleter)
		fmt.Fprintf(w, "DeleteNote:\t%v\n", blob.DeleteNote)
		w.Flush()
	}
}

type brecord struct {
	slot string
	blob items.BlobID
}
type BySlot []brecord

func (s BySlot) Len() int           { return len(s) }
func (s BySlot) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s BySlot) Less(i, j int) bool { return s[i].slot < s[j].slot }

func sortBySlot(m map[string]items.BlobID) []brecord {
	var result []brecord
	for slot, blob := range m {
		result = append(result, brecord{slot, blob})
	}
	sort.Sort(BySlot(result))
	return result
}

func dolist(r *items.Store) {
	c := r.List()
	for name := range c {
		fmt.Println(name)
	}
}

// add all the files to this item. Directories are automatically recursed into.
// Files and directories which begin with a dot are skipped.
func doadd(r *items.Store, id string, files []string, isset bool) {
	item, err := r.Item(id)
	if err != nil && err != items.ErrNoItem {
		fmt.Println(err.Error())
		return
	}
	tx, err := r.Open(id, *creator)
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	defer tx.Close()
	if isset {
		tx.ClearSlots()
	}
	for _, name := range files {
		err := filepath.Walk(name, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if strings.HasPrefix(info.Name(), ".") {
				if info.IsDir() {
					return filepath.SkipDir
				}
				return nil
			}
			if !info.IsDir() {
				slot := deriveSlotName(name, path)
				fmt.Printf("Adding %s\n", path)
				return addfile(item, tx, path, slot, info.Size())
			}
			return nil
		})
		if err != nil {
			fmt.Println(err)
			return
		}
	}
}

func deriveSlotName(parentpath, fname string) string {
	// we prefer slot names to not include the
	// entire absolute path, so keep just the
	// relative part of the path
	slot := strings.TrimPrefix(fname, parentpath)
	if slot == "" {
		// we have fname == parentpath, so just take
		// the base name for the file
		slot = filepath.Base(fname)
	}
	// slots names shouldn't begin with a slash
	return strings.TrimPrefix(slot, "/")
}

func addfile(item *items.Item, tx *items.Writer, fname string, slotname string, size int64) error {
	// see if the file is already in the blob list
	var bid items.BlobID
	if item != nil {
		bid = findBlobByFile(item, fname, size)
	}
	if bid == 0 {
		// read it in
		in, err := os.Open(fname)
		if err != nil {
			return err
		}
		defer in.Close()
		bid, err = tx.WriteBlob(in, size, nil, nil)
		if err != nil {
			return err
		}
	}
	tx.SetSlot(slotname, bid)
	return nil
}

func findBlobByFile(item *items.Item, fname string, size int64) items.BlobID {
	// see if any blobs have the same size. if so, lets checksum it
	// and compare further.
	for _, blob := range item.Blobs {
		if blob.Size == size {
			goto checksum
		}
	}
	return 0

checksum:
	in, err := os.Open(fname)
	if err != nil {
		return 0
	}
	defer in.Close()
	w := util.NewHashWriterPlain()
	_, err = io.Copy(w, in)
	if err != nil {
		return 0
	}
	h, _ := w.CheckSHA256(nil)

	for _, blob := range item.Blobs {
		if bytes.Equal(h, blob.SHA256) {
			return blob.ID
		}
	}
	return 0
}

// add all the files to this item. Directories are automatically recursed into.
// Files and directories which begin with a dot are skipped.
func dodelete(r *items.Store, id string, delblobs []string) {
	var delbid []items.BlobID
	for _, blobid := range delblobs {
		bid, err := strconv.Atoi(blobid)
		if err != nil {
			fmt.Println(err.Error())
			return
		}
		delbid = append(delbid, items.BlobID(bid))
	}
	tx, err := r.Open(id, *creator)
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	for _, bid := range delbid {
		tx.DeleteBlob(bid)
		// TODO(dbrower): also remove any slots pointing to this blob
	}
	err = tx.Close()
	if err != nil {
		fmt.Println(err.Error())
	}
}

// doidentifymissingblob will scan an entire store and list the item ids that
// have "missing blobs". Passing those ids to fix-missing-blob will repair them.
//
// See DLTP-1676.
func doidentifymissingblobs(r *items.Store) {
	ids := r.List()

	for id := range ids {
		item, err := r.Item(id)
		if err != nil {
			fmt.Println(err)
			return
		}
		n := len(item.Blobs)
		if n == 0 {
			// no blobs?
			continue
		}
		if items.BlobID(n) == item.Blobs[n-1].ID {
			// nothing missing
			continue
		}
		// missing blobs are present
		fmt.Println(id)
	}
}

type missingPair struct {
	ID     items.BlobID
	Bundle int
}

var (
	zeroMD5 = []byte{0xd4, 0x1d, 0x8c, 0xd9, 0x8f, 0x00, 0xb2, 0x04,
		0xe9, 0x80, 0x09, 0x98, 0xec, 0xf8, 0x42, 0x7e}
	zeroSHA256 = []byte{0xe3, 0xb0, 0xc4, 0x42, 0x98, 0xfc, 0x1c, 0x14,
		0x9a, 0xfb, 0xf4, 0xc8, 0x99, 0x6f, 0xb9, 0x24,
		0x27, 0xae, 0x41, 0xe4, 0x64, 0x9b, 0x93, 0x4c,
		0xa4, 0x95, 0x99, 0x1b, 0x78, 0x52, 0xb8, 0x55}
)

// dofixmissingblobs does an extremely low level fix to repair items that have
// missing blobs. A missing blob is one that has an ID between 1 and the
// maximum blob id, and is not in the blob list. The fix is to add these blobs
// to the blob list. They are added as having a length of zero (which might not
// be correct!).
//
// See DLTP-1676.
func dofixmissingblobs(fs store.Store, r *items.Store, ids []string) {
	for _, id := range ids {
		fixmissingblob(fs, r, id)
	}
}

func fixmissingblob(fs store.Store, r *items.Store, id string) {
	item, err := r.Item(id)
	if err != nil {
		fmt.Println(err)
		return
	}
	n := len(item.Blobs)
	if n == 0 {
		fmt.Println(id, "- No blobs in item")
		return
	}
	if items.BlobID(n) == item.Blobs[n-1].ID {
		fmt.Println(id, "- No missing blobs")
		return
	}
	// blob list should be sorted by increasing BlobIDs
	var missing []missingPair
	max := item.Blobs[n-1].ID
	i := 0
	for expected := items.BlobID(1); expected <= max; expected++ {
		if item.Blobs[i].ID == expected {
			i++
		} else {
			missing = append(missing, missingPair{
				ID: expected,
				// assumes the missing blob is in the same bundle as the blob
				// with the next larger ID
				Bundle: item.Blobs[i].Bundle,
			})
		}
	}
	fmt.Println(id, "-", len(missing), "missing blobs")

	// Add a blob entry for each one missing. This assumes each missing blob
	// has size 0. Unknown whether this assumption is always true.
	for _, mp := range missing {
		blob := &items.Blob{
			ID:       mp.ID,
			SaveDate: time.Now(),
			Creator:  *creator,
			Size:     0,
			MD5:      zeroMD5,
			SHA256:   zeroSHA256,
			Bundle:   mp.Bundle,
		}
		item.Blobs = append(item.Blobs, blob)
	}
	sort.Stable(byID(item.Blobs))

	// Save the item. We don't make a new version, since we are not changing
	// any file mappings. We are only adding blob entries for missing blobs.
	bw := items.NewBundler(fs, item)
	item.MaxBundle++ // do this after opening the bundle
	err = bw.Close()
	if err != nil {
		fmt.Println(id, err)
	}
}

type byID []*items.Blob

func (p byID) Len() int           { return len(p) }
func (p byID) Less(i, j int) bool { return p[i].ID < p[j].ID }
func (p byID) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }
