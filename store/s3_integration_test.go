// +build s3

package store

// tests S3Store with an external service. Can use amazon s3, or can run a local
// service with the same API (e.g. Minio).
//
// To run from the command line
//
//    env "AWS_ACCESS_KEY_ID=XXXXX" "AWS_SECRET_ACCESS_KEY=YYYY" go test -tags=s3 -run S3

import (
	"fmt"
	"io"
	"os"
	"strconv"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
)

func getSession() *session.Session {
	s3Config := &aws.Config{
		Endpoint:         aws.String("http://localhost:9000"),
		Region:           aws.String("us-east-1"),
		DisableSSL:       aws.Bool(true),
		S3ForcePathStyle: aws.Bool(true),
	}
	return session.New(s3Config)
}

func TestS3Open(t *testing.T) {
	s := NewS3("zoo", "", getSession())
	items, err := s.ListPrefix("")
	t.Log(err)
	t.Log(items)
	if len(items) == 0 {
		return
	}
	r, size, err := s.Open(items[0])
	t.Log(size, err)
	n, err := io.Copy(os.Stdout, NewReader(r))
	t.Log(n)
	t.Logf("%#v", err)
	r.Close()
}

func TestS3Create(t *testing.T) {
	s := NewS3("zoo", "abc/", getSession())
	w, err := s.Create("first")
	if err != nil {
		t.Log(err)
	}
	n, err := w.Write([]byte("abcdefghijklmnopqrstuvwxyz"))
	t.Log(n, err)
	err = w.Close()
	t.Log(err)

	w, err = s.Create("second")
	data := make([]byte, 3000000)
	for i := 0; i < 10; i++ {
		n, err = w.Write(data)
		t.Log(n, err)
	}
	err = w.Close()
	t.Log(err)
}

func TestS3List(t *testing.T) {
	const N = 3 * 1024

	s := NewS3("zoo", "list/", getSession())

	// add items
	for i := 0; i < N; i++ {
		w, err := s.Create(fmt.Sprintf("%d", i))
		if err != nil {
			t.Error(err)
			continue
		}
		w.Write([]byte("01234567890123456789"))
		w.Close()
	}

	// see if everything was found
	nfound := 0
	c := s.List()
	for name := range c {
		_, err := strconv.Atoi(name)
		if err != nil {
			t.Error(err)
			continue
		}
		nfound++
		s.Delete(name)
	}
	if nfound != N {
		t.Error("expected", N, "found", nfound)
	}
}

func TestS3Delete(t *testing.T) {
	s := NewS3("zoo", "delete/", getSession())
	w, err := s.Create("first")
	if err != nil {
		t.Log(err)
	}
	n, err := w.Write([]byte("abcdefghijklmnopqrstuvwxyz"))
	t.Log(n, err)
	err = w.Close()
	t.Log(err)

	err = s.Delete("first")
	t.Log(err)

	err = s.Delete("first")
	t.Log(err)
}
