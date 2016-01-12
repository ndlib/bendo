package bserver

import (
	"errors"
	"fmt"
	"github.com/antonholmquist/jason"
	"net/http"
)

// Exported errors
var (
	ErrNotFound      = errors.New("Item Not Found in Bendo")
	ErrNotAuthorized = errors.New("Access Denied")
	ErrReadFailed    = errors.New("Read Failed")
)

func GetItemInfo(id string) (*jason.Object, error) {
	var path = "http://" + *bendoServer + "/item/" + id

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

func PostUpload(chunk []byte, chunkmd5sum []byte, filemd5sum []byte, fileId string) (fileid string, err error) {

	var path = "http://" + *bendoServer + "/upload/"

	if fileId != BogusFileId {
		path += "/" + fileId
	}

	return fileid, nil
}
