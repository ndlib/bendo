package bserver

import (
	"fmt"
	"github.com/ndlib/bendo/cmd/bclient/fileutil"
	"github.com/antonholmquist/jason"
)

var (
        ItemFetchStatus error

	bserver *string
	RemoteJason *jason.Object

)

func Init(  server *string) {
	fileutil.IfVerbose("github.com/ndlib/bendo/bclient/bserver.Init() called")
	bserver = server
}


func FetchItemInfo( item string ) {
	 defer fileutil.UpLoadDone.Done()
         fileutil.IfVerbose("github.com/ndlib/bendo/bclient/bserver.FetchItemInfo() called")

	 remoteBendo := New( *bserver)

	 json, err := remoteBendo.GetItemInfo( item)

	// Some error occurred retrieving from server, or item not found.
	 if err != nil {
		ItemFetchStatus = err
		fmt.Println(err.Error())
		return
	 }

	RemoteJason = json
}

func SendFiles ( fileQueue <- chan string, item string, fileroot string ){

	fmt.Println(item)
	fmt.Println(fileroot)

	for filename := range fileQueue {
		fmt.Println(filename)
	}
}

func upLoadFile ( filename string, item string, fileroot string ) {
	// compute absolute filename  path
	// create /tmp/bendo_upload/item/filename/
	// chunk that baby
	// compute md5sums of chunks
	// POST/UPLOAD, get upload_id
	// POST/UPLOAD/:upload_id for each chunk
	// POST/ID/TRANSACTION (JSON w/ item ,reldir info) 
	// cleanup
	
	
}
