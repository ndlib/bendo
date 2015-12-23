package fileutil

// A catchall package for bclient file operations and data structures.
// May need decomposition if too unwieldy

import (
        "fmt"
	"os"
	"path"
	"path/filepath"
	"sync"
	
	"github.com/ndlib/bendo/cmd/bclient/filelist"
)

var (
	localFileList *filelist.FileList
	remoteFileList *filelist.FileList
	UpLoadDone  sync.WaitGroup
	FilesWalked = make(chan string)
	rootPrefix string
	verbose bool
        
)

// Public Synchronization Gate

func WaitFileListStep() {
	IfVerbose("At UpLoadDone.Wait()")
	UpLoadDone.Wait()
	IfVerbose("UpLoadDone.Wait() satisfied")
}

func InitFileListStep( root string) {
	// Wait for 
	rootPrefix = root
	IfVerbose("InitFileListStep called")
	UpLoadDone.Add(1)
	IfVerbose("InitFileListStep finished")
}
func SetVerbose( isVerbose bool ) {
	verbose = isVerbose
}

func IfVerbose( output string) {
    if verbose {
        fmt.Println(output)
    }
}

func CreateUploadList( files string) {

	IfVerbose("CreateUploadList called")
	defer UpLoadDone.Done()

	filepath.Walk(path.Join(rootPrefix, files), addToUploadList)

	close(FilesWalked)
	IfVerbose("CreateUploadList exit")
}

// addToUploadList is called by fileUtil.CreatUploadList  once for each file under filepath.walk()

func addToUploadList(path string, info os.FileInfo, err error) error {

	if err != nil {
		return err
	}

	// We only want files in the list- leave directories out

	if info.IsDir() {
		return nil
	}

	FilesWalked <- path

	return nil
}

func ComputeLocalChecksums( item string) {
     		defer UpLoadDone.Done()

		
		localFileList = filelist.New( item, rootPrefix)
		localFileList.BuildListFromChan(FilesWalked)
//			file := <-FilesWalked
}
