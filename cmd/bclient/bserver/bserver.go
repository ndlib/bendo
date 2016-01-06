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
