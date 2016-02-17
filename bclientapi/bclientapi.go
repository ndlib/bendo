package bclientapi

import (
	"encoding/json"
	"fmt"
	"github.com/ndlib/bendo/fileutil"
	"path"
	"strconv"
	"sync"
	"time"
)

var (
	fileIDMutex sync.Mutex
	fileIDList  []fileIDStruct
)

type fileIDStruct struct {
	fileid string
	slot   string
	item   string
	isNew  bool
}

// common attributes

type itemAttributes struct {
	fileroot    string
	item        string
	bendoServer string
	chunkSize   int
	wait        bool
}

func addFileToTransactionList(filename string, fileID string, item string, isNew bool) {

	fileIDMutex.Lock()

	thisFileID := new(fileIDStruct)
	thisFileID.fileid = fileID
	thisFileID.slot = filename
	thisFileID.item = item
	thisFileID.isNew = isNew

	fileIDList = append(fileIDList, *thisFileID)

	fileIDMutex.Unlock()
}

func New(server string, item string, fileroot string, chunkSize int, wait bool) *itemAttributes {

	thisItem := new(itemAttributes)
	thisItem.bendoServer = server
	thisItem.item = item
	thisItem.chunkSize = chunkSize
	thisItem.fileroot = fileroot
	thisItem.wait = wait

	return thisItem
}

// serve the file queue. This is called from main as 1 or more goroutines
// If the file Upload fails, close the channel and exit

func (ia *itemAttributes) SendFiles(fileQueue chan string, ld *fileutil.ListData) {

	for filename := range fileQueue {

		// if the file name made it on the queue, it either is not on the bendo server yet,
		// or was uploaded before, but is not the current version.
		// For the latter, we need only tell bendo to use the older version (via its blobID)
		// For the former, we'll need to upload it.

		var err error
		var blobID int64
		var newFile bool

		blobID, newFile = ld.IsUploadNeeded(filename)

		if newFile {
			err = ia.uploadFile(filename, ld.ShowUploadFileMd5(filename))
		} else {
			err = ia.updateBlobID(filename, blobID)
		}

		if err != nil {
			fmt.Println(err)
			close(fileQueue)
		}
	}
}

// serve file requests from the server for  a get
// If the file Get fails, close the channel and exit

func (ia *itemAttributes) GetFiles(fileQueue chan string, pathPrefix string) {

	for filename := range fileQueue {
		err := ia.downLoad(filename, pathPrefix)

		if err != nil {
			fmt.Printf("Error: GetFile return %s\n", err.Error())
			break
		}
	}
}

func (ia *itemAttributes) uploadFile(filename string, uploadMd5 []byte) error {

	// upload chunks buffer size is 1MB * cmd line flag chunksize

	fileID, uploadErr := ia.chunkAndUpload(filename, uploadMd5, ia.chunkSize*1048576)

	// If an error occurred, report it, and return

	if uploadErr != nil {
		// add api call to delete fileid uploads
		fmt.Printf("Error: unable to upload file %s for item %s, %s\n", filename, ia.item, uploadErr.Error())
		return uploadErr
	}

	addFileToTransactionList(filename, fileID, ia.item, true)

	return nil

}

// update the blob id of this file for the given item via a POST item/:item/transaction

func (ia *itemAttributes) updateBlobID(filename string, blobid int64) error {

	addFileToTransactionList(filename, strconv.FormatInt(blobid, 10), ia.item, false)

	return nil
}

// read  fileIDList for new files, build commit list, send to server

func (ia *itemAttributes) SendNewTransactionRequest() (string, error) {

	cmdlist := [][]string{}

	// if fileIDList is empty, no need to send this

	if len(fileIDList) == 0 {
		return "", nil
	}

	for _, fid := range fileIDList {
		if fid.isNew {
			cmdlist = append(cmdlist, []string{"add", fid.fileid})
		}
		cmdlist = append(cmdlist, []string{"slot", fid.slot, fid.fileid})
	}

	buf, _ := json.Marshal(cmdlist)

	transaction, transErr := ia.createFileTransAction(buf)

	return transaction, transErr
}

func (ia *itemAttributes) WaitForCommitFinish(tx string) {

	seconds := 5

	for {
		fmt.Printf(".")
		time.Sleep(time.Second * time.Duration(seconds))

		v, err := ia.getTransactionStatus(path.Base(tx))

		if err != nil {
			fmt.Println(err.Error())
			break
		}

		name, _ := v.GetString("Status")

		if name == "StatusFinished" {
			fmt.Printf("\nFinished\n")
			break
		}

		if name == "StatusError" {
			fmt.Printf("\nError\n")
			break
		}

		seconds += 5
	}
}
