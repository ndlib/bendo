package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"strconv"

	"github.com/ndlib/bendo/items"
)

var (
	storeDir = flag.String("-s", ".", "location of the storage directory")
)

func main() {
	flag.Parse()

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
		} else {
			fmt.Printf("%#v\n", item)
		}
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
