package bclientapi

import (
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path"

	"github.com/antonholmquist/jason"
)

// Exported errors
var (
	ErrNotFound       = errors.New("Item Not Found in Bendo")
	ErrNotAuthorized  = errors.New("Access Denied")
	ErrUnexpectedResp = errors.New("Unexpected Response Code")
	ErrReadFailed     = errors.New("Read Failed")
)

func (ia *ItemAttributes) GetItemInfo() (*jason.Object, error) {
	return ia.doJasonGet("/item/" + ia.item)
}

// get upload metadata (if it exists) . Assumes that the upload fileid is item#-filemd5sum
// returns json of metadata if successful, error otherwise

func (ia *ItemAttributes) getUploadMeta(fileId string) (*jason.Object, error) {
	return ia.doJasonGet("/upload/" + fileId + "/metadata")
}

func (ia *ItemAttributes) downLoad(fileName string, pathPrefix string) error {
	var httpPath = ia.bendoServer + "/item/" + ia.item + "/" + fileName

	req, _ := http.NewRequest("GET", httpPath, nil)
	if ia.token != "" {
		req.Header.Add("X-Api-Key", ia.token)
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
			fmt.Printf("%s returned 404\n", httpPath)
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
		fmt.Printf("Error: could not create directory %s\n%s\n", targetDir, err.Error())
		return err
	}

	filePtr, err := os.Create(targetFile)

	if err != nil {
		fmt.Printf("Error: could not create file %s\n%s\n", targetFile, err.Error())
		return err
	}
	defer filePtr.Close()

	_, err = io.Copy(filePtr, r.Body)

	return err
}

func (ia *ItemAttributes) PostUpload(chunk []byte, chunkmd5sum []byte, filemd5sum []byte, mimetype string, fileId string) (fileid string, err error) {

	var path = ia.bendoServer + "/upload/" + fileId

	req, _ := http.NewRequest("POST", path, bytes.NewReader(chunk))
	req.Header.Set("X-Upload-Md5", hex.EncodeToString(chunkmd5sum))
	if ia.token != "" {
		req.Header.Add("X-Api-Key", ia.token)
	}
	if mimetype != "" {
		req.Header.Add("Content-Type", mimetype)
	}
	if len(filemd5sum) > 0 {
		req.Header.Add("X-Content-MD5", hex.EncodeToString(filemd5sum))
	}
	resp, err := http.DefaultClient.Do(req)

	if err != nil {
		return fileId, err
	}
	resp.Body.Close()
	if resp.StatusCode != 200 {
		fmt.Printf("Received HTTP status %d for %s\n", resp.StatusCode, path)
	}
	return fileId, nil
}

// Not well named - sets a POST /item/:id/transaction

func (ia *ItemAttributes) CreateTransaction(cmdlist []byte) (string, error) {

	var path = ia.bendoServer + "/item/" + ia.item + "/transaction"

	req, _ := http.NewRequest("POST", path, bytes.NewReader(cmdlist))
	if ia.token != "" {
		req.Header.Add("X-Api-Key", ia.token)
	}
	resp, err := http.DefaultClient.Do(req)

	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 202 {
		fmt.Printf("Received HTTP status %d for POST %s", resp.StatusCode, path)
		return "", ErrUnexpectedResp
	}

	transaction := resp.Header.Get("Location")

	return transaction, nil
}

func (ia *ItemAttributes) getTransactionStatus(transaction string) (*jason.Object, error) {
	return ia.doJasonGet("/transaction/" + transaction)
}

func (ia *ItemAttributes) doJasonGet(path string) (*jason.Object, error) {
	path = ia.bendoServer + path

	req, err := http.NewRequest("GET", path, nil)

	if err != nil {
		return nil, err
	}

	req.Header.Set("Accept-Encoding", "application/json")
	if ia.token != "" {
		req.Header.Add("X-Api-Key", ia.token)
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
