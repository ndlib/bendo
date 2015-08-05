package server

import (
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
	txpath := getlocation(t, "POST", "/item/zxcv", 200)
	t.Log("got tx path", txpath)
	// cannot open two transactions on one item
	checkStatus(t, "POST", "/item/zxcv", 409)

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

func TestTransactionCommands2(t *testing.T) {
	// add two blobs, and then delete one
	checkStatus(t, "GET", "/item/zxcvbnm", 404)
	txpath := getlocation(t, "POST", "/item/zxcvbnm", 200)
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
	txpath = getlocation(t, "POST", "/item/zxcvbnm", 200)
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

func TestTransactionCommands(t *testing.T) {
	// add one blob, and then delete it
	checkStatus(t, "GET", "/item/zxcvasdf", 404)
	txpath := getlocation(t, "POST", "/item/zxcvasdf", 200)
	t.Log("got tx path", txpath)
	blob := uploadstring(t, "POST", txpath, "delete me")
	t.Log("got blob path", blob)
	checkStatus(t, "POST", txpath+"/commit", 200)
	text := getbody(t, "GET", "/blob/zxcvasdf/1", 200)
	if text != "delete me" {
		t.Fatalf("Received %#v, expected %#v", text, "delete me")
	}
	// now delete blob it
	txpath = getlocation(t, "POST", "/item/zxcvasdf", 200)
	t.Log("got tx path", txpath)
	uploadstring(t, "PUT", txpath+"/commands", `[["delete", "1"]]`)
	checkStatus(t, "POST", txpath+"/commit", 200)
	text = getbody(t, "GET", "/blob/zxcvasdf/1", 404)
	if text == "delete me" {
		t.Fatalf("Received %#v, expected %#v", text, "")
	}
}

func uploadstring(t *testing.T, verb, route string, s string) string {
	req, err := http.NewRequest(verb, testServer.URL+route, strings.NewReader(s))
	if err != nil {
		t.Fatal("Problem creating request", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(route, err)
		return ""
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		t.Errorf("%s: Received status %d",
			route,
			resp.StatusCode)
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
