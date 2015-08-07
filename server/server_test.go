package server

import (
	"crypto/md5"
	"encoding/hex"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/ndlib/bendo/items"
	"github.com/ndlib/bendo/store"
	"github.com/ndlib/bendo/transaction"
)

func TestTransaction1(t *testing.T) {
	checkStatus(t, "GET", "/item/zxcv", 404)
	txpath := getlocation(t, "POST", "/item/zxcv/transaction", 200)
	t.Log("got tx path", txpath)
	// cannot open two transactions on one item
	checkStatus(t, "POST", "/item/zxcv/transaction", 409)

	// bad transaction ids are rejected
	checkStatus(t, "GET", "/transaction/abc", 404)

	blobpath := uploadstring(t, "POST", txpath, "hello world")
	t.Log("got blob path", blobpath)
	blobpath = uploadstring(t, "PUT", blobpath, " and hello sun")
	checkStatus(t, "GET", blobpath, 200)
	checkStatus(t, "POST", blobpath, 405)
	checkStatus(t, "POST", txpath+"/commit", 200)
	checkStatus(t, "GET", "/item/zxcv", 200)
	checkStatus(t, "GET", "/blob/zxcv/1", 200)
	checkStatus(t, "GET", "/blob/zxcv/2", 404)
	text := getbody(t, "GET", "/blob/zxcv/1", 200)
	if text != "hello world and hello sun" {
		t.Fatalf("Received %#v, expected %#v", text, "hello world and hello sun")
	}
}

func TestTransactionCommands(t *testing.T) {
	// add two blobs, and then delete one
	checkStatus(t, "GET", "/item/zxcvbnm", 404)
	txpath := getlocation(t, "POST", "/item/zxcvbnm/transaction", 200)
	t.Log("got tx path", txpath)
	blob1 := uploadstring(t, "POST", txpath, "hello world")
	t.Log("got blob path", blob1)
	blob2 := uploadstring(t, "POST", txpath, "delete me")
	t.Log("got blob path", blob2)
	checkStatus(t, "POST", txpath+"/commit", 200)
	text := getbody(t, "GET", "/blob/zxcvbnm/2", 200)
	if text != "delete me" {
		t.Fatalf("Received %#v, expected %#v", text, "delete me")
	}
	// now delete blob 2
	txpath = getlocation(t, "POST", "/item/zxcvbnm/transaction", 200)
	t.Log("got tx path", txpath)
	uploadstring(t, "PUT", txpath+"/commands", `[["delete", "2"]]`)
	checkStatus(t, "POST", txpath+"/commit", 200)
	text = getbody(t, "GET", "/blob/zxcvbnm/1", 200)
	if text != "hello world" {
		t.Fatalf("Received %#v, expected %#v", text, "hello world")
	}
	text = getbody(t, "GET", "/blob/zxcvbnm/2", 404)
	if text == "delete me" {
		t.Fatalf("Received %#v, expected %#v", text, "")
	}
}

func TestUploadHash(t *testing.T) {
	// upload blob with no hashes and with a wrong hash
	checkStatus(t, "GET", "/item/uploadhash", 404)
	txpath := getlocation(t, "POST", "/item/uploadhash/transaction", 200)
	t.Log("got tx path", txpath)
	uploadstringhash(t, "POST", txpath, "hello world", "", 400)
	uploadstringhash(t, "POST", txpath, "hello world", "nothexnumber", 400)
	uploadstringhash(t, "POST", txpath, "hello world", "abcdef0123456789", 412)
	blobpath := uploadstringhash(t, "POST", txpath, "hello world", "5eb63bbbe01eeed093cb22bb8f5acdc3", 200)
	t.Log("got blob path", blobpath)
	// do same thing again, only now extending previous upload
	uploadstringhash(t, "PUT", blobpath, "hello world", "", 400)
	uploadstringhash(t, "PUT", blobpath, "hello world", "nothexnumber", 400)
	uploadstringhash(t, "PUT", blobpath, "hello world", "abcdef0123456789", 412)
	uploadstringhash(t, "PUT", blobpath, "hello world", "5eb63bbbe01eeed093cb22bb8f5acdc3", 200)
}

func uploadstring(t *testing.T, verb, route string, s string) string {
	md5hash := md5.Sum([]byte(s))
	return uploadstringhash(t, verb, route, s, hex.EncodeToString(md5hash[:]), 200)
}

func uploadstringhash(t *testing.T, verb, route, s, hash string, statuscode int) string {

	req, err := http.NewRequest(verb, testServer.URL+route, strings.NewReader(s))
	if err != nil {
		t.Fatal("Problem creating request", err)
	}
	req.Header.Set("X-Upload-Md5", hash)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(route, err)
		return ""
	}
	defer resp.Body.Close()
	if resp.StatusCode != statuscode {
		t.Errorf("%s: Received status %d, expected %d",
			route,
			resp.StatusCode,
			statuscode)
		return ""
	}
	return resp.Header.Get("Location")
}

func getlocation(t *testing.T, verb, route string, expstatus int) string {
	resp := checkRoute(t, verb, route, expstatus)
	if resp != nil {
		defer resp.Body.Close()
		return resp.Header.Get("Location")
	}
	return ""
}

func getbody(t *testing.T, verb, route string, expstatus int) string {
	resp := checkRoute(t, verb, route, expstatus)
	if resp != nil {
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			t.Fatal(route, err)
		}
		resp.Body.Close()
		return string(body)
	}
	return ""
}

func checkStatus(t *testing.T, verb, route string, expstatus int) {
	resp := checkRoute(t, verb, route, expstatus)
	if resp != nil {
		resp.Body.Close()
	}
}

func checkRoute(t *testing.T, verb, route string, expstatus int) *http.Response {
	req, err := http.NewRequest(verb, testServer.URL+route, nil)
	if err != nil {
		t.Fatal("Problem creating request", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(route, err)
		return nil
	}
	if resp.StatusCode != expstatus {
		t.Errorf("%s: Expected status %d and received %d",
			route,
			expstatus,
			resp.StatusCode)
		resp.Body.Close()
		return nil
	}
	return resp
}

var testServer *httptest.Server

func init() {
	Items = items.New(store.NewMemory())
	TxStore = transaction.New(store.NewMemory())
	TxStore.Load()
	testServer = httptest.NewServer(AddRoutes())
}
