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
`
)

func main() {
	flag.Parse()

	fmt.Printf("Using storage dir %s\n", *storeDir)
	r := items.New(store.NewFileSystem(*storeDir))

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
	}
}

func doblob(r *items.Store, id, blob string) {
	bid, _ := strconv.Atoi(blob)

	rc, err := r.Blob(id, items.BlobID(bid))
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
		fmt.Fprintf(w, "ChecksumDate:\t%v\n", blob.ChecksumDate)
		fmt.Fprintf(w, "ChecksumStatus:\t%v\n", blob.ChecksumStatus)
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
		if bytes.Compare(h, blob.SHA256) == 0 {
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
