package bclientapi

import (
	"errors"
	"fmt"
	"path"
	"time"

	"github.com/ndlib/bendo/transaction"
)

// common attributes

type ItemAttributes struct {
	fileroot    string
	item        string
	bendoServer string
	chunkSize   int
	wait        bool
	token       string
}

func New(server string, item string, fileroot string, chunkSize int, wait bool, token string) *ItemAttributes {

	thisItem := new(ItemAttributes)
	thisItem.bendoServer = server
	thisItem.item = item
	thisItem.chunkSize = chunkSize
	thisItem.fileroot = fileroot
	thisItem.wait = wait
	thisItem.token = token

	return thisItem
}

// serve file requests from the server for  a get
// If the file Get fails, close the channel and exit

func (ia *ItemAttributes) GetFiles(fileQueue chan string, pathPrefix string) error {

	for filename := range fileQueue {
		err := ia.downLoad(filename, pathPrefix)

		if err != nil {
			fmt.Printf("Error: GetFile return %s\n", err.Error())
			return err
		}
	}

	return nil
}

// upload the give file to the bendo server

func (ia *ItemAttributes) UploadFile(filename string, uploadMd5 []byte, mimetype string) error {
	_, err := ia.chunkAndUpload(filename, uploadMd5, mimetype)

	// If an error occurred, report it, and return

	if err != nil {
		// add api call to delete fileid uploads
		fmt.Printf("Error: unable to upload file %s for item %s, %s\n", filename, ia.item, err)
		return err
	}

	return nil

}

func (ia *ItemAttributes) WaitForCommitFinish(tx string) error {
	delay := 5 * time.Second
	txid := path.Base(tx)

	fmt.Printf("Waiting on %s:", txid)

	// This gives bendo about 12 hours to finish this transaction
	// length of time in seconds = 5 * (131) * (131+1) / 2
	for i := 0; i < 131; i++ {
		var status int64

		fmt.Printf(".")
		time.Sleep(delay)

		v, err := ia.getTransactionStatus(txid)

		if err == nil {
			status, err = v.GetInt64("Status")
		}

		if err != nil {
			fmt.Println(err.Error())
			return err
		}

		switch transaction.Status(status) {
		case transaction.StatusFinished:
			fmt.Printf("\nFinished\n")
			return nil
		case transaction.StatusError:
			fmt.Printf("\nError\n")
			return errors.New("StatusError returned")
		}

		delay += 5 * time.Second
	}
	fmt.Printf("\nTimeout\n")
	return errors.New("timeout")
}
