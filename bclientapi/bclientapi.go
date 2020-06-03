package bclientapi

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"path"
	"sync"
	"time"

	"github.com/ndlib/bendo/transaction"
)

// A Connection represents a connection with a Bendo Service.
// It can be shared between multiple goroutines.
type Connection struct {
	// The bendo server this connection is to
	HostURL string

	// The chunk size to use for uploading files.
	// If 0, defaults to 10485760 bytes = 10 MB.
	ChunkSize int

	// An API key to use when interacting with the server.
	Token string

	// use this to make http requests. It is configured with a timeout.
	client *http.Client

	// keep a list of unused buffers so we can amortize allocation cost.
	chunkpool *sync.Pool
}

type FileInfo struct {
	Size     int64  // the size of this file
	MD5      []byte // the md5 hash for the entire file being uploaded
	Mimetype string // the mimetype of this file
}

// Upload copies r to the bendo server, storing it under the name uploadname. r
// must support seeking since that is used to determine the length of the
// content and if the md5 sum is not set in info, this function will first read
// r to compute it and then seek back to the beginning to then copy r to the
// server. Upload() will resume a partial upload if only part of r is on the
// server.
func (c *Connection) Upload(uploadname string, r io.ReadSeeker, info FileInfo) error {
	return c.upload(uploadname, r, info)
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
