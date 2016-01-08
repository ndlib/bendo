package fileutil

// A catchall package for bclient file operations and data structures.
// May need decomposition if too unwieldy

import (
	"bytes"
        "fmt"
	"os"
	"path"
	"path/filepath"
	"sync"

       "github.com/antonholmquist/jason"
)

var (
	UpLoadDone  sync.WaitGroup
	UseRemoteList bool = true
	localFileList *FileList
	remoteFileList *FileList
	FilesWalked = make(chan string)
	rootPrefix string
	verbose bool
        
)

// Print out remote filer list
func PrintRemoteList() {
for key, value := range  remoteFileList.Files {
    fmt.Println("Key:", key, "Value:", value)
}

}

// Print out local file list
func PrintLocalList() {

for key, value := range  localFileList.Files {
    fmt.Println("Key:", key, "Value:", value[1])
}

}

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
	UpLoadDone.Add(3)
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

func ComputeLocalChecksums() {
     		defer UpLoadDone.Done()

		
		localFileList = New( rootPrefix)
		localFileList.BuildListFromChan(FilesWalked)
}

func BuildRemoteList( json *jason.Object)  {
		remoteFileList = New( rootPrefix)
		remoteFileList.BuildListFromJSON( json)
}

func CullLocalList() {

	// item does not exist on remote bendo server
	if remoteFileList == nil {
		return
	}
	
	for localFile, localMD5 := range localFileList.Files {
		
		remoteMD5Map := remoteFileList.Files[localFile]

		if remoteMD5Map == nil {
			continue
		}	

		for _, remoteMD5  := range remoteMD5Map {
			if bytes.Compare(localMD5[1], remoteMD5) == 0 {
				delete(localFileList.Files, localFile)
				continue
			}
		} 
		
	} 
}

func QueueFiles( fileQueue chan string) {

	localFileList.AddToSendQueue( fileQueue)
} 
