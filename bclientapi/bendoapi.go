package bclientapi

import (
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path"

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

// get upload metadata (if it exists) . Assumes that the upload fileid is item#-filemd5sum
// returns json of metadata if successful, error otherwise

func (c *Connection) getUploadMeta(fileId string) (*jason.Object, error) {
	return c.doJasonGet("/upload/" + fileId + "/metadata")
}

func (c *Connection) downLoad(item string, fileName string, pathPrefix string) error {
	var httpPath = c.HostURL + "/item/" + item + "/" + fileName

	req, _ := http.NewRequest("GET", httpPath, nil)
	if c.Token != "" {
		req.Header.Add("X-Api-Key", c.Token)
	}
	r, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer r.Body.Close()
	if r.StatusCode != 200 {
		r.Body.Close()
		switch r.StatusCode {
		case 404:
			log.Printf("%s returned 404\n", httpPath)
			return ErrNotFound
		case 401:
			return ErrNotAuthorized
		default:
			return fmt.Errorf("Received status %d from Bendo", r.StatusCode)
		}
	}

	// How do we handle large downloads?

	targetFile := path.Join(pathPrefix, fileName)
	targetDir, _ := path.Split(targetFile)

	err = os.MkdirAll(targetDir, 0755)

	if err != nil {
		log.Println("Error: could not create directory", targetDir, err)
		return err
	}

	filePtr, err := os.Create(targetFile)

	if err != nil {
		log.Println("Error: could not create file", targetFile, err)
		return err
	}
	defer filePtr.Close()

	_, err = io.Copy(filePtr, r.Body)

	return err
}

func (c *Connection) PostUpload(chunk []byte, chunkmd5sum []byte, filemd5sum []byte, mimetype string, fileId string) error {

	var path = c.HostURL + "/upload/" + fileId

	req, _ := http.NewRequest("POST", path, bytes.NewReader(chunk))
	req.Header.Set("X-Upload-Md5", hex.EncodeToString(chunkmd5sum))
	if c.Token != "" {
		req.Header.Add("X-Api-Key", c.Token)
	}
	if mimetype != "" {
		req.Header.Add("Content-Type", mimetype)
	}
	if len(filemd5sum) > 0 {
		req.Header.Add("X-Content-MD5", hex.EncodeToString(filemd5sum))
	}
	resp, err := http.DefaultClient.Do(req)

	if err != nil {
		return err
	}

	defer resp.Body.Close()

	switch resp.StatusCode {
	case 200:
		break
	case 412:
		return ErrChecksumMismatch
	case 500:
		err = ErrServerError
		fallthrough
	default:
		message := make([]byte, 512)
		resp.Body.Read(message)
		log.Printf("Received HTTP status %d for %s\n", resp.StatusCode, path)
		log.Println(string(message))
		if err == nil {
			err = errors.New(string(message))
		}
		return err
	}
	return nil
}

// Not well named - sets a POST /item/:id/transaction

func (c *Connection) CreateTransaction(item string, cmdlist []byte) (string, error) {

	var path = c.HostURL + "/item/" + item + "/transaction"

	req, _ := http.NewRequest("POST", path, bytes.NewReader(cmdlist))
	if c.Token != "" {
		req.Header.Add("X-Api-Key", c.Token)
	}
	resp, err := http.DefaultClient.Do(req)

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
	if c.Token != "" {
		req.Header.Add("X-Api-Key", c.Token)
	}

	resp, err := http.DefaultClient.Do(req)

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
