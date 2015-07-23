package main

import (
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"os"
	"strconv"
	"text/tabwriter"

	"github.com/ndlib/bendo/items"
)

var (
	storeDir = flag.String("s", ".", "location of the storage directory")
)

func main() {
	flag.Parse()

	fmt.Printf("Using storage dir %s\n", *storeDir)
	r := items.New(items.NewFileStore(*storeDir))

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
		for slot, blob := range vers.Slots {
			fmt.Printf("%5d  %s\n", blob, slot)
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

func dolist(r *items.Store) {
	c := r.List()
	for name := range c {
		fmt.Println(name)
	}
}

func doadd(r *items.Store, id string, files []string) {
	tx := r.Open(id)
	tx.SetCreator("wendy")
	defer tx.Close()
	for _, name := range files {
		in, err := os.Open(name)
		if err != nil {
			fmt.Println(err)
			return
		}
		bid, err := tx.WriteBlob(in, 0, nil, nil)
		if err != nil {
			fmt.Println(err)
			return
		}
		tx.SetSlot(name, bid)
	}
}
