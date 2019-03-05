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

const testBucket = "testlibnd-bendo-cache"

func getSession() *session.Session {
	s3Config := &aws.Config{
		// uncomment for local Minio instance
		//Endpoint:         aws.String("http://localhost:9000"),
		//DisableSSL:       aws.Bool(true),
		Region:           aws.String("us-east-1"),
		S3ForcePathStyle: aws.Bool(true),
	}
	return session.New(s3Config)
}

func TestS3Open(t *testing.T) {
	s := NewS3(testBucket, "", getSession())
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
	s := NewS3(testBucket, "abc/", getSession())
	w, err := s.Create("first")
	if err != nil {
		t.Error(err)
	}
	n, err := w.Write([]byte("abcdefghijklmnopqrstuvwxyz"))
	t.Log(n, err)
	err = w.Close()
	t.Log(err)

	w, err = s.Create("second")
	t.Log(err)
	totallength := int64(0)
	for i := 0; i < 400000; i++ {
		n, err = w.Write([]byte("abcdefghijklmnopqrstuvwxyz"))
		if err != nil {
			t.Error(err)
		}
		totallength += int64(n)
	}
	err = w.Close()
	t.Log(err)

	// is the uploaded file the right length?
	r, size, err := s.Open("second")
	t.Log(size, err)
	if size != totallength {
		t.Error("Uploaded item length is", size, "expected", totallength)
	}
	r.Close()
}

func TestS3List(t *testing.T) {
	const N = 3 * 1024

	s := NewS3(testBucket, "list/", getSession())

	// add items
	for i := 0; i < N; i++ {
		w, err := s.Create(fmt.Sprintf("%d", i))
		if err != nil {
			t.Error(err)
			continue
		}
		_, err = w.Write([]byte("01234567890123456789"))
		t.Log(err)

		err = w.Close()
		t.Log(err)
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
		err = s.Delete(name)
		t.Log(err)
	}
	if nfound != N {
		t.Error("expected", N, "found", nfound)
	}
}

func TestS3Delete(t *testing.T) {
	s := NewS3(testBucket, "delete/", getSession())
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
