// This package manages a list of files, and their checksums
 
package filelist

import (
	"crypto/md5"
	"fmt"
	"io"
        "os"
)

// Our underlying data structure

type FileList struct {
	Item string
	Root string
	Files map[string][]byte
}

// Create an empty FileList

func New( item, root string) *FileList {
fl := new(FileList)
	fl.Item = item
	fl.Root = root
	fl.Files = make(map[string][]byte)

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


		f.Files[fileName] = md5Sum
	}
} 

// Construct a FileList from a JSON return by the Bendo API


