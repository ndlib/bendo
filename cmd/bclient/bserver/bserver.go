package bserver

import (
	"fmt"
	"github.com/antonholmquist/jason"
	"github.com/ndlib/bendo/cmd/bclient/fileutil"
	"path"
)

var (
	ItemFetchStatus error

	bendoServer *string
	RemoteJason *jason.Object
)

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
		ItemFetchStatus = err
		fmt.Println(err.Error())
		return
	}

	RemoteJason = json
}

func SendFiles(fileQueue <-chan string, item string, fileroot string) {

	for filename := range fileQueue {
		uploadFile(filename, fileutil.ShowUploadFileMd5(filename), item, fileroot)
	}
}

func uploadFile(filename string, uploadMd5 []byte, item string, fileroot string) {

	// compute absolute filename  path

	fullFilePath := path.Join(fileroot, filename)

	// chunk that baby initiial size is 1MB

	uploadErr := chunkAndUpload(fullFilePath, uploadMd5, item, 1048576)

	if uploadErr != nil {
		fmt.Println(uploadErr.Error())
	}
	// cleanup

}
