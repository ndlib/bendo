package main

import (
	"bytes"
	"flag"
	"fmt"
	"io"
	"os"
	"strconv"

	"github.com/ndlib/bendo/bendo"
)

var (
	storeDir = flag.String("-s", ".", "location of the storage directory")
)

func main() {
	flag.Parse()

	r := bendo.New(bendo.NewFileStore(*storeDir))

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
	case "dummy":
		dodummy(r)
	}
}

func doblob(r *bendo.Store, id, blob string) {
	bid, _ := strconv.Atoi(blob)

	rc, err := r.Blob(id, bendo.BlobID(bid))
	if err != nil {
		fmt.Printf("%s / %d: Error %s\n", id, bid, err.Error())
	} else {
		io.Copy(os.Stdout, rc)
		rc.Close()
	}
}

func doitem(r *bendo.Store, ids []string) {
	for _, id := range ids {
		item, err := r.Item(id)
		if err != nil {
			fmt.Printf("%s: Error %s\n", id, err.Error())
		} else {
			fmt.Printf("%#v\n", item)
		}
	}
}

func dolist(r *bendo.Store) {
	c := r.List()
	for name := range c {
		fmt.Println(name)
	}
}

func dodummy(r *bendo.Store) {
	data := bytes.NewBufferString("Hello There World!!")
	tx := r.Open("qwer")
	tx.SetCreator("wendy")
	bid, err := tx.WriteBlob(data, 0, nil, nil)
	tx.SetSlot("hello", bid)
	err = tx.Close()
	fmt.Println(err)
}
