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
	storeDir = flag.String("s", ".", "location of the storage directory")
	creator  = flag.String("creator", "butil", "Creator name to use")
	usage    = `
butil <command> <command arguments>

Possible commands:
    blob <item id> <blob number>

    item <item id list>

    list

    add <item id> <file/directory list>
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
		doadd(r, args[1], args[2:])
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
	fmt.Println("MaxBundle:", item.MaxBundle)
	for _, vers := range item.Versions {
		fmt.Println("---")
		w := tabwriter.NewWriter(os.Stdout, 5, 1, 3, ' ', 0)
		fmt.Fprintf(w, "Version:\t%d\n", vers.ID)
		fmt.Fprintf(w, "SaveDate:\t%v\n", vers.SaveDate)
		fmt.Fprintf(w, "Creator:\t%s\n", vers.Creator)
		fmt.Fprintf(w, "Note:\t%s\n", vers.Note)
		w.Flush()
		fmt.Printf(" Blob  Slot\n")
		for _, r := range sortBySlot(vers.Slots) {
			fmt.Printf("%5d  %s\n", r.blob, r.slot)
		}
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

func doadd(r *items.Store, id string, files []string) {
	item, err := r.Item(id)
	if err != nil && err != items.ErrNoItem {
		fmt.Println(err.Error())
		return
	}
	tx, err := r.Open(id)
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	tx.SetCreator(*creator)
	defer tx.Close()
	for _, name := range files {
		err := filepath.Walk(name, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				fmt.Println(err.Error())
				return nil
			}
			fmt.Println(info.Name())
			if strings.HasPrefix(info.Name(), ".") {
				return filepath.SkipDir
			}
			if !info.IsDir() {
				fmt.Printf("Adding %s\n", path)
				return addfile(item, tx, path, info.Size())
			}
			return nil
		})
		if err != nil {
			fmt.Println(err)
			return
		}
	}
}

func addfile(item *items.Item, tx *items.Writer, fname string, size int64) error {
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
	tx.SetSlot(fname, bid)
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
