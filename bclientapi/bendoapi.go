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

func (ia *itemAttributes) GetItemInfo() (*jason.Object, error) {

	var path = "http://" + ia.bendoServer + "/item/" + ia.item

	req, _ := http.NewRequest("GET", path, nil)
	if ia.token != "" {
		req.Header.Add("X-Api-Key", ia.token)
	}
	r, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer r.Body.Close()
	if r.StatusCode != 200 {
		switch r.StatusCode {
		case 404:
			return nil, ErrNotFound
		case 401:
			return nil, ErrNotAuthorized
		default:
			return nil, fmt.Errorf("Received status %d from Bendo", r.StatusCode)
		}
	}

	v, err := jason.NewObjectFromReader(r.Body)

	return v, err
}

// get upload metadata (if it exists) . Assumes that the upload fileid is item#-filemd5sum
// returns json of metadata if successful, error otherwise

func (ia *itemAttributes) getUploadMeta(fileId string) (*jason.Object, error) {

	var path = "http://" + ia.bendoServer + "/upload/" + fileId + "/metadata"

	req, _ := http.NewRequest("GET", path, nil)
	if ia.token != "" {
		req.Header.Add("X-Api-Key", ia.token)
	}
	req.Header.Add("Accept-Encoding", "application/json")
	r, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer r.Body.Close()
	if r.StatusCode != 200 {
		switch r.StatusCode {
		case 404:
			return nil, ErrNotFound
		case 401:
			return nil, ErrNotAuthorized
		default:
			return nil, fmt.Errorf("Received status %d from Bendo", r.StatusCode)
		}
	}

	v, err := jason.NewObjectFromReader(r.Body)

	return v, err
}

func (ia *itemAttributes) downLoad(fileName string, pathPrefix string) error {
	var httpPath = "http://" + ia.bendoServer + "/item/" + ia.item + "/" + fileName

	req, _ := http.NewRequest("GET", httpPath, nil)
	if ia.token != "" {
		req.Header.Add("X-Api-Key", ia.token)
	}
	r, err := http.DefaultClient.Do(req)

	defer r.Body.Close()
	if err != nil {
		return err
	}
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

	defer r.Body.Close()

	if err != nil {
		fmt.Printf("Error: could not create directory %s\n%s\n", err.Error())
		return err
	}

	filePtr, err := os.Create(targetFile)
	defer filePtr.Close()

	if err != nil {
		fmt.Printf("Error: could not create file %s\n%s\n", err.Error())
		return err
	}

	_, err = io.Copy(filePtr, r.Body)

	return err
}

func (ia *itemAttributes) PostUpload(chunk []byte, chunkmd5sum []byte, filemd5sum []byte, fileId string) (fileid string, err error) {

	var path = "http://" + ia.bendoServer + "/upload/" + fileId

	req, _ := http.NewRequest("POST", path, bytes.NewReader(chunk))
	req.Header.Set("X-Upload-Md5", hex.EncodeToString(chunkmd5sum))
	if ia.token != "" {
		req.Header.Add("X-Api-Key", ia.token)
	}
	resp, err := http.DefaultClient.Do(req)

	defer resp.Body.Close()
	if err != nil {
		return fileId, err
	}
	if resp.StatusCode != 200 {

		fmt.Printf("Received HTTP status %d for %s\n", resp.StatusCode, path)
		return fileId, nil
	}

	return fileId, nil
}

// Not well named - sets a POST /item/:id/transaction

func (ia *itemAttributes) createFileTransAction(cmdlist []byte) (string, error) {

	var (
		path     = "http://" + ia.bendoServer + "/item/" + ia.item
		location = "/transaction"
	)

	req, _ := http.NewRequest("POST", path+location, bytes.NewReader(cmdlist))
	if ia.token != "" {
		req.Header.Add("X-Api-Key", ia.token)
	}
	resp, err := http.DefaultClient.Do(req)

	defer resp.Body.Close()
	if err != nil {
		return "", err
	}
	if resp.StatusCode != 202 {

		fmt.Printf("Received HTTP status %d for POST %s", resp.StatusCode, path+location)
		return "", ErrUnexpectedResp
	}

	transaction := resp.Header.Get("Location")

	return transaction, nil
}

func (ia *itemAttributes) getTransactionStatus(transaction string) (*jason.Object, error) {
	var path = "http://" + ia.bendoServer + "/transaction/" + transaction

	req, err := http.NewRequest("GET", path, nil)

	if err != nil {
		return nil, err
	}

	req.Header.Set("Accept-Encoding", "application/json")
	if ia.token != "" {
		req.Header.Add("X-Api-Key", ia.token)
	}

	resp, err := http.DefaultClient.Do(req)
	defer resp.Body.Close()

	if err != nil {
		return nil, err
	}
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

	v, err := jason.NewObjectFromReader(resp.Body)

	return v, err
}
