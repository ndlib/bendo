package main

import (
	"bytes"
	"flag"
	"fmt"
	"time"

	"github.com/dbrower/bendo/bendo"
)

var (
	storeDir = flag.String("-s", ".", "location of the storage directory")
)

func main() {
	flag.Parse()

	data := bytes.NewBufferString("Hello There World!!")

	r := bendo.NewRomp(bendo.NewFSStore(*storeDir))
	tx := r.Update("qwer")
	tx.AddBlob(&bendo.Blob{
		SaveDate: time.Now(),
		Creator:  "don",
	}, data)
	err := tx.Commit()

	fmt.Println(err)
}
