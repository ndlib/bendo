package main

import (
	"flag"
	"fmt"

	"github.com/dbrower/bendo/bendo"
)

var (
	storeDir = flag.String("-s", ".", "location of the storage directory")
)

func main() {
	flag.Parse()

	r := bendo.NewRomp(bendo.NewFSStore(*storeDir))
	m, err := r.Item("abc")
	if err != nil {
		fmt.Printf("Error %s", err)
	} else {
		fmt.Printf("Item %#v", m)
	}
}
