package bserver

import (
	"errors"
	"fmt"
	"net/http"
	"github.com/antonholmquist/jason"
)

// Exported errors
var (
	ErrNotFound      = errors.New("Item Not Found in Bendo")
	ErrNotAuthorized = errors.New("Access Denied")
	ErrReadFailed = errors.New("Read Failed")
)

type RemoteBendo struct {
	hostpath  string
}

func New( remotePath string )( *RemoteBendo) {
	rb := new (RemoteBendo)

	rb.hostpath = remotePath

	return rb
}

func (rb *RemoteBendo) GetItemInfo(id string) ( *jason.Object , error) {
	var path = "http://" + rb.hostpath + "/item/" +  id 


	r, err := http.Get(path)
	if err != nil {
		return nil,  err
	}
	if r.StatusCode != 200 {
		r.Body.Close()
		switch r.StatusCode {
		case 404:
			return nil,  ErrNotFound
		case 401:
			return nil,  ErrNotAuthorized
		default:
			return nil,  fmt.Errorf("Received status %d from Bendo", r.StatusCode)
		}
	}


	v, err := jason.NewObjectFromReader(r.Body) 

	return v,  err
}
