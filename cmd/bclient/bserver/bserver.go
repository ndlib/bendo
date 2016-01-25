package bserver

import (
	"encoding/json"
	"fmt"
	"github.com/antonholmquist/jason"
	"github.com/ndlib/bendo/cmd/bclient/fileutil"
	"sync"
)

var (
	itemFetchStatus error = nil

	fileIDMutex sync.Mutex

	fileIDList []fileIDStruct

	bendoServer *string
	RemoteJason *jason.Object
)


type fileIDStruct struct {
	fileid string
	slot   string
	item   string
}

func ItemFetchStatus() error {
	return itemFetchStatus
}

func addFileToTransactionList(filename string, fileID string, item string) {

	fileIDMutex.Lock()

	thisFileID := new(fileIDStruct)
	thisFileID.fileid = fileID
	thisFileID.slot = filename
	thisFileID.item = item

	fileIDList = append(fileIDList, *thisFileID)

	fileIDMutex.Unlock()
}

func Init(server *string) {
	fileutil.IfVerbose("github.com/ndlib/bendo/bclient/bserver.Init() called")
	bendoServer = server
}

func FetchItemInfo(item string) {
	defer fileutil.UpLoadDone.Done()
	fileutil.IfVerbose("github.com/ndlib/bendo/bclient/bserver.FetchItemInfo() called")

	json, err := GetItemInfo(item)

	// Some error occurred retrieving from server, or item not found.
	if err != nil {
		itemFetchStatus = err
		fmt.Println(err.Error())
		return
	}

	RemoteJason = json
}

// serve the file queue. This is called from main as 1 or more goroutines
// If the file Upload fails, close the channel and exit

func SendFiles(fileQueue chan string, item string, fileroot string, mut *sync.WaitGroup) {

	for filename := range fileQueue {
		err := uploadFile(filename, fileutil.ShowUploadFileMd5(filename), item, fileroot)

		if err != nil {
			close(fileQueue)
		}
	}

	mut.Done()
}

func uploadFile(filename string, uploadMd5 []byte, item string, fileroot string) error {

	// upload chunks initial buffer size is 1MB

	fileID, uploadErr := chunkAndUpload(fileroot, filename, uploadMd5, item, 1048576)

	// If an error occurred, report it, and return

	if uploadErr != nil {
		// add api call to delete fileid uploads
		fmt.Printf("Error: unable to upload file %s for item %s, %s\n", filename, item, uploadErr.Error())
		return uploadErr
	}

	addFileToTransactionList(filename, fileID, item)

	return nil

}

func SendTransactionRequest(item string) error {

	cmdlist := [][]string{}

	for _, fid := range fileIDList {
		cmdlist = append(cmdlist, []string{"add", fid.fileid})
		cmdlist = append(cmdlist, []string{"slot", fid.slot, fid.fileid})
	}

	buf, _ := json.Marshal(cmdlist)

	transErr := createFileTransAction(buf, item)

	//if transErr != nil {
	//       fmt.Println( transErr.Error())
	//      fmt.Printf( "Error: unable to upload file %s for item %s, %s\n", filename, item, transErr.Error())
	//}

	return transErr
}
