package bclientapi

import (
	"errors"
	"fmt"
	"path"
	"time"

	"github.com/ndlib/bendo/transaction"
)

// A Connection represents a connection with a Bendo Service.
// It can be shared between multiple goroutines.
type Connection struct {
	// The bendo server this connection is to
	HostURL string

	ChunkSize int
	Token     string
}

// upload the give file to the bendo server

func (c *Connection) UploadFile(item string, filename string, uploadMd5 []byte, mimetype string) error {
	_, err := c.chunkAndUpload(item, filename, uploadMd5, mimetype)

	// If an error occurred, report it, and return

	if err != nil {
		// add api call to delete fileid uploads
		fmt.Printf("Error: unable to upload file %s for item %s, %s\n", filename, item, err)
		return err
	}

	return nil

}

var (
	ErrTransaction = errors.New("error processing transaction")
	ErrTimeout     = errors.New("timeout processing transaction")
)

// WaitForCommitFinish waits for the given transaction to finish.
// It will return an error if the transaction had an error.
// It will poll the server for up to 12 hours, and then return
// a timeout error.
func (c *Connection) WaitForCommitFinish(txpath string) error {
	txid := path.Base(txpath)

	fmt.Printf("Waiting on transaction %s:", txid)

	// loop for at most 12 hours
	const delay = 5 * time.Second
	for i := 0; i < int(12*time.Hour/delay); i++ {
		var status int64

		fmt.Printf(".")
		time.Sleep(delay)

		v, err := c.getTransactionStatus(txid)
		if err == nil {
			status, err = v.GetInt64("Status")
		}
		if err != nil {
			return err
		}

		switch transaction.Status(status) {
		case transaction.StatusFinished:
			return nil
		case transaction.StatusError:
			fmt.Println("Error")
			errlist, _ := v.GetStringArray("Err")
			for _, e := range errlist {
				fmt.Println(e)
			}
			return ErrTransaction
		}
	}
	return ErrTimeout
}
