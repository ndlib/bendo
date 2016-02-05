package bclientapi

import (
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"
	"github.com/antonholmquist/jason"
	"io"
	"net/http"
	"os"
	"path"
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

	r, err := http.Get(path)
	if err != nil {
		return nil, err
	}
	if r.StatusCode != 200 {
		r.Body.Close()
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

	r, err := http.Get(httpPath)
	if err != nil {
		return err
	}
	if r.StatusCode != 200 {
		r.Body.Close()
		switch r.StatusCode {
		case 404:
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
		fmt.Printf("Error: could not create directory %s\n%s\n", err.Error())
		return err
	}

	filePtr, err := os.Create(targetFile)

	if err != nil {
		fmt.Printf("Error: could not create file %s\n%s\n", err.Error())
		return err
	}

	defer r.Body.Close()
	defer filePtr.Close()

	_, err = io.Copy(filePtr, r.Body)

	return err
}

func (ia *itemAttributes) PostUpload(chunk []byte, chunkmd5sum []byte, filemd5sum []byte, fileId string) (fileid string, err error) {

	var path = "http://" + ia.bendoServer

	if fileId != BogusFileId {
		path += fileId
	} else {
		path += "/upload"
	}

	// yeah, OK , this is dumb. Now that I know that http needs a reader,
	// I should have the chunking code pass a reader.

	req, _ := http.NewRequest("POST", path, bytes.NewReader(chunk))
	req.Header.Set("X-Upload-Md5", hex.EncodeToString(chunkmd5sum))
	resp, err := http.DefaultClient.Do(req)

	defer resp.Body.Close()
	if err != nil {
		return fileId, err
	}
	if resp.StatusCode != 200 {

		fmt.Printf("Received HTTP status %d for %s\n", resp.StatusCode, path)
		return fileId, nil
	}

	route := resp.Header.Get("Location")

	if route == "" {
		fmt.Printf("No Location returned on POST: %s", route)
		return fileId, err
	}

	return route, nil
}

func (ia *itemAttributes) createFileTransAction(cmdlist []byte) error {

	var (
		path     = "http://" + ia.bendoServer + "/item/" + ia.item
		location = "/transaction"
	)

	req, _ := http.NewRequest("POST", path+location, bytes.NewReader(cmdlist))
	resp, err := http.DefaultClient.Do(req)

	defer resp.Body.Close()
	if err != nil {
		return err
	}
	if resp.StatusCode != 202 {

		fmt.Printf("Received HTTP status %d for POST %s", resp.StatusCode, path+location)
		return ErrUnexpectedResp
	}

	//transaction  := resp.Header.Get("Location")

	return nil
}
