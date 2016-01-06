// This package manages a list of files, and their checksums
 
package fileutil

import (
	"crypto/md5"
	"fmt"
	"io"
        "os"
	"github.com/antonholmquist/jason"
)

// Our underlying data structure

type FileList struct {
	Root string
	Files map[string]map[int][]byte
}

// Create an empty FileList

func New( root string) *FileList {
fl := new(FileList)
	fl.Root = root
	fl.Files = make(map[string]map[int][]byte)

	return fl
}

// Construct a FileList from a channel of filenames - open local file, and compute checksums

func (f *FileList) BuildListFromChan( filePipe <- chan string ) {
	
	for fileName := range filePipe {
		// Open the local file
		fReader, ferr := os.Open(f.Root + "/" + fileName)

		if ferr != nil {
			fmt.Println(ferr)
			continue
		}
			 
		md5w := md5.New()

		// Copy from the Reader into the Writer (this will compute the CheckSums)

		io.Copy( md5w, fReader)

		// Get the Checksums
 
		md5Sum := md5w.Sum(nil)

     		innerMap, ok := f.Files[fileName]

    		if !ok {
        		innerMap = make(map[int][]byte)
        		f.Files[fileName] = innerMap
    		}

		f.Files[fileName][1] = md5Sum

	}
} 

// Construct a FileList from a JSON return by the Bendo API

func (f *FileList) BuildListFromJSON( json *jason.Object  ) {

        versionArray, _ := json.GetObjectArray("Versions")
	for _, version := range versionArray {
		slotMap, _ := version.GetObject("Slots")
		fmt.Println(slotMap.String())
	}
}
