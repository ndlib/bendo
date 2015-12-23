package bserver

import (
	"errors"
	"github.com/ndlib/bendo/cmd/bclient/fileutil"
)

var (
        ItemFetchStatus error

	ItemNotFound = errors.New("Key no found on Server ")

)

func Init() {
	fileutil.IfVerbose("github.com/ndlib/bendo/bclient/bserver.Init() called")
}

func FetchItemInfo( item string ) {
         fileutil.IfVerbose("github.com/ndlib/bendo/bclient/bserver.FetchItemInfo() called")
	 ItemFetchStatus = ItemNotFound
}
