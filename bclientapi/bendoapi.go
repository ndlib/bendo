package bclientapi

import (
	"bytes"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"

	"github.com/antonholmquist/jason"
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

func (c *Connection) GetItemInfo(item string) (*jason.Object, error) {
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

// Not well named - sets a POST /item/:id/transaction

func (c *Connection) CreateTransaction(item string, cmdlist []byte) (string, error) {

	var path = c.HostURL + "/item/" + item + "/transaction"

	req, _ := http.NewRequest("POST", path, bytes.NewReader(cmdlist))
	resp, err := c.do(req)

	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 202 {
		log.Printf("Received HTTP status %d for POST %s", resp.StatusCode, path)
		return "", ErrUnexpectedResp
	}

	transaction := resp.Header.Get("Location")

	return transaction, nil
}

func (c *Connection) getTransactionStatus(transaction string) (*jason.Object, error) {
	return c.doJasonGet("/transaction/" + transaction)
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

	if resp.StatusCode != 200 {
		switch resp.StatusCode {
		case 404:
			return nil, ErrNotFound
		case 401:
			return nil, ErrNotAuthorized
		default:
			return nil, fmt.Errorf("Received status %d from Bendo", resp.StatusCode)
		}
	}
	return jason.NewObjectFromReader(resp.Body)
}
