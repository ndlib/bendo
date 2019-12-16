package bclientapi

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/antonholmquist/jason"

	"github.com/ndlib/bendo/transaction"
)

// Exported errors
var (
	ErrNotFound         = errors.New("Item Not Found in Bendo")
	ErrNotAuthorized    = errors.New("Access Denied")
	ErrUnexpectedResp   = errors.New("Unexpected Response Code")
	ErrReadFailed       = errors.New("Read Failed")
	ErrChecksumMismatch = errors.New("Checksum mismatch")
	ErrServerError      = errors.New("Server Error")
)

func (c *Connection) ItemInfo(item string) (*jason.Object, error) {
	return c.doJasonGet("/item/" + item)
}

// getUploadInfo returns information for an uploaded file, if it exists.
// returns information if successful, error otherwise.
func (c *Connection) getUploadInfo(uploadname string) (FileInfo, error) {
	var result FileInfo
	v, err := c.doJasonGet("/upload/" + uploadname + "/metadata")
	if err != nil {
		return result, err
	}
	result.Size, _ = v.GetInt64("Size")
	if vv, _ := v.GetString("MD5"); vv != "" {
		result.MD5, _ = base64.StdEncoding.DecodeString(vv)
	}
	result.Mimetype, _ = v.GetString("MimeType")
	return result, nil
}

// Download copies the given (item, filename) pair from bendo to the given io.Writer.
func (c *Connection) Download(w io.Writer, item string, filename string) error {
	var path = c.HostURL + "/item/" + item + "/" + filename

	req, _ := http.NewRequest("GET", path, nil)
	resp, err := c.do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	switch resp.StatusCode {
	case 200:
		break
	case 404:
		log.Println("returned 404", path)
		return ErrNotFound
	case 401:
		return ErrNotAuthorized
	default:
		return fmt.Errorf("Received status %d from Bendo", resp.StatusCode)
	}

	_, err = io.Copy(w, resp.Body)

	return err
}

// do performs an http request using our client with a timeout. The
// timeout is arbitrary, and is just there so we don't hang indefinitely
// should the server never close the connection.
func (c *Connection) do(req *http.Request) (*http.Response, error) {
	if c.Token != "" {
		req.Header.Add("X-Api-Key", c.Token)
	}
	if c.client == nil {
		c.client = &http.Client{
			Timeout: 10 * time.Minute, // arbitrary
		}
	}
	return c.client.Do(req)
}

// An Action represents an action we want bendo to do in the processing of a
// transaction. The Encode method returns an encoding of this action as a list
// of strings in the simple command language Bendo uses. Each Action represent a
// single entry, and a usual transaction will contain many actions.
type Action struct {
	What ActionKind
	// the exact fields used depends on What.
	UploadID string // for new content, the upload id of the content
	MimeType string // new mime type
	BlobID   int64  // for blobs already on server
	Name     string // the name for the file on server
}

type ActionKind int

const (
	AUnknown ActionKind = iota
	ANewBlob
	AUpdateMimeType
	AUpdateFile
)

func (a Action) String() string {
	switch a.What {
	case ANewBlob:
		return fmt.Sprintf("<Action ANewBlob, UploadID=%s>",
			a.UploadID)
	case AUpdateMimeType:
		return fmt.Sprintf("<Action AUpdateMimeType, Blob=%d, MimeType=%s>",
			a.BlobID,
			a.MimeType)
	case AUpdateFile:
		return fmt.Sprintf("<Action AUpdateFile, Name=%s, Blob=%d, UploadID=%s>",
			a.Name,
			a.BlobID,
			a.UploadID)
	}
	return fmt.Sprintf("<Action AUnknown, What=%d>", a.What)
}

// makeTransactionCommands turns an Action list into a list of transaction
// commands to send to the bendo server.
func makeTransactionCommands(todo []Action) [][]string {
	var cmdlist [][]string

	for _, t := range todo {
		switch t.What {
		case ANewBlob:
			cmdlist = append(cmdlist, []string{"add", t.UploadID})
		case AUpdateMimeType:
			id := strconv.FormatInt(t.BlobID, 10)
			cmdlist = append(cmdlist, []string{"mimetype", id, t.MimeType})
		case AUpdateFile:
			var fileID string
			// are we using a remote blob or a newly uploaded one?
			if t.BlobID > 0 {
				fileID = strconv.FormatInt(t.BlobID, 10)
			} else {
				fileID = t.UploadID
			}
			cmdlist = append(cmdlist, []string{"slot", t.Name, fileID})
		}
	}
	return cmdlist
}
func (c *Connection) StartTransaction(item string, todo []Action) (string, error) {
	path := c.HostURL + "/item/" + item + "/transaction"
	cmdlist := makeTransactionCommands(todo)
	buf, _ := json.Marshal(cmdlist)

	req, _ := http.NewRequest("POST", path, bytes.NewReader(buf))
	resp, err := c.do(req)
	if err != nil {
		return "", err
	}
	// need to defer this?
	defer resp.Body.Close()

	switch resp.StatusCode {
	case 202:
		txid := resp.Header.Get("Location")
		return txid, nil
	default:
		log.Printf("Received HTTP status %d for POST %s", resp.StatusCode, path)
		return "", ErrUnexpectedResp
	}
}

type TransactionInfo struct {
	Status transaction.Status
	Errors []string
}

// TransactionStatus returns info on the given transaction ID. If the transaction
// is being processed, it does not wait for the transaction to finish. The status
// is an integer as given by transaction.TransactionStatus.
func (c *Connection) TransactionStatus(txid string) (TransactionInfo, error) {
	var result TransactionInfo
	v, err := c.doJasonGet("/transaction/" + txid)
	if err != nil {
		return result, err
	}
	x, err := v.GetInt64("Status")
	if err == nil {
		result.Status = transaction.Status(x)
	}
	result.Errors, _ = v.GetStringArray("Err")
	return result, err
}

func (c *Connection) doJasonGet(path string) (*jason.Object, error) {
	path = c.HostURL + path

	req, err := http.NewRequest("GET", path, nil)
	if err != nil {
		return nil, err
	}

	req.Header.Set("Accept-Encoding", "application/json")
	resp, err := c.do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case 200:
		return jason.NewObjectFromReader(resp.Body)
	case 404:
		return nil, ErrNotFound
	case 401:
		return nil, ErrNotAuthorized
	default:
		return nil, fmt.Errorf("Received status %d from Bendo", resp.StatusCode)
	}
}
