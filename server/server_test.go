package server

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/ndlib/bendo/blobcache"
	"github.com/ndlib/bendo/fragment"
	"github.com/ndlib/bendo/items"
	"github.com/ndlib/bendo/store"
	"github.com/ndlib/bendo/transaction"
	"github.com/ndlib/bendo/util"
)

func TestTransaction1(t *testing.T) {
	// bad transaction ids are rejected
	checkStatus(t, "GET", "/transaction/abc", 404)

	// non-well-formed json is rejected
	// must use uploadstringhash to send the bad json
	uploadstringhash(t, "POST", "/item/abc1/transaction", `["not correct format"]`, "", 400)
	// bad or non-existant commands are rejected
	sendtransaction(t, "/item/abc2/transaction", [][]string{{"not a command"}}, 400)
	sendtransaction(t, "/item/abc3/transaction", [][]string{{"not a command"}}, 400)

	// transactions on items already having pending transactions are rejected
	abc := "abc4" + randomid()
	sendtransaction(t, "/item/"+abc+"/transaction", [][]string{{"sleep"}}, 202)
	sendtransaction(t, "/item/"+abc+"/transaction", [][]string{{"add", "file"}}, 409)

	// do a simple transaction
	file1 := uploadstring(t, "POST", "/upload", "hello world")
	t.Log("got file1 =", file1)
	uploadstring(t, "POST", file1, " and hello sun")

	itemid := "zxcv" + randomid()
	checkStatus(t, "GET", "/item/"+itemid, 404)

	// should use file1 path here...
	txpath := sendtransaction(t, "/item/"+itemid+"/transaction",
		[][]string{{"add", path.Base(file1)}}, 202)
	t.Log("got tx path", txpath)

	// tx is processed async to the web frontend.
	// maybe we should see if the transaction completes first?
	time.Sleep(10 * time.Millisecond) // sleep a squinch

	checkStatus(t, "GET", "/item/"+itemid, 200)
	checkStatus(t, "GET", "/blob/"+itemid+"/2", 404)
	text := getbody(t, "GET", "/blob/"+itemid+"/1", 200)
	if text != "hello world and hello sun" {
		t.Fatalf("Received %#v, expected %#v", text, "hello world and hello sun")
	}
}

func TestTransactionCommands(t *testing.T) {
	// add two blobs, and then delete one
	blob1 := uploadstring(t, "POST", "/upload", "hello world")
	t.Log("blob1 = ", blob1)
	blob2 := uploadstring(t, "POST", "/upload", "delete me")
	t.Log("blob2 =", blob2)

	itemid := "zxcvbnm" + randomid()
	checkStatus(t, "GET", "/item/"+itemid, 404)
	txpath := sendtransaction(t, "/item/"+itemid+"/transaction",
		[][]string{{"add", path.Base(blob1)},
			{"add", path.Base(blob2)}}, 202)
	t.Log("got tx path", txpath)
	// tx is processed async from the commit above.
	// maybe wait for it to be committed?
	time.Sleep(10 * time.Millisecond) // sleep a squinch
	checkStatus(t, "GET", "/item/"+itemid, 200)
	text := getbody(t, "GET", "/blob/"+itemid+"/2", 200)
	if text != "delete me" {
		t.Errorf("Received %#v, expected %#v", text, "delete me")
	}
	// now delete blob 2
	txpath = sendtransaction(t, "/item/"+itemid+"/transaction",
		[][]string{{"delete", "2"}}, 202)
	t.Log("got tx path", txpath)
	time.Sleep(10 * time.Millisecond) // sleep a squinch
	text = getbody(t, "GET", "/blob/"+itemid+"/1", 200)
	if text != "hello world" {
		t.Errorf("Received %#v, expected %#v", text, "hello world")
	}
	text = getbody(t, "GET", "/blob/"+itemid+"/2", 404)
	if text != "Blob has been deleted\n" {
		t.Errorf("Received %#v, expected %#v", text, "Blob has been deleted\n")
	}
}

func TestUploadNameAssign(t *testing.T) {
	// if we ask for a name, is it created?
	ourpath := "/upload/uploadnameassign" + randomid()
	checkStatus(t, "GET", ourpath, 404)
	assignedpath := uploadstring(t, "POST", ourpath, "zxcv")
	if assignedpath != ourpath {
		t.Errorf("Got path %s, expected %s", assignedpath, ourpath)
	}
}

func TestUploadHash(t *testing.T) {
	// upload blob with no hashes and with a wrong hash
	uppath := "/upload"
	uploadstringhash(t, "POST", uppath, "hello world", "", 400)
	uploadstringhash(t, "POST", uppath, "hello world", "nothexnumber", 400)
	firstpath := uploadstringhash(t, "POST", uppath, "hello world", "abcdef0123456789", 412)
	secondpath := uploadstringhash(t, "POST", uppath, "hello world", "5eb63bbbe01eeed093cb22bb8f5acdc3", 200)
	t.Log("firstpath = ", firstpath)
	t.Log("secondpath = ", secondpath)
	// do same thing again, only now extending previous upload
	uploadstringhash(t, "POST", secondpath, "hello world", "", 400)
	uploadstringhash(t, "POST", secondpath, "hello world", "nothexnumber", 400)
	uploadstringhash(t, "POST", secondpath, "hello world", "abcdef0123456789", 412)
	uploadstringhash(t, "POST", secondpath, "hello world", "5eb63bbbe01eeed093cb22bb8f5acdc3", 200)
	// now check that the uploads with a bad hash were rolled back
	// the first blob is the bad POST. it should be empty
	text := getbody(t, "GET", firstpath, 200)
	if text != "" {
		t.Fatalf("Received %#v, expected %#v", text, "")
	}
	// the second blob should have one good POST and one good PUT
	text = getbody(t, "GET", secondpath, 200)
	const expected = "hello worldhello world"
	if text != expected {
		t.Fatalf("Received %#v, expected %#v", text, expected)
	}
}

func TestDeleteFile(t *testing.T) {
	// add a file, then delete it.
	filepath := uploadstring(t, "POST", "/upload", "hello world")
	t.Log("got file path", filepath)
	checkStatus(t, "DELETE", filepath, 200)
	checkStatus(t, "GET", filepath, 404) // There should be no file
}

// Tests for HEAD request, SHA checksum return, and Caching header
// see bendo issues #32 and #117
func TestHeadCacheSHA(t *testing.T) {

	const fileContent = "We choose do do these things"
	const fileShaHexSum = "2b04a7382010d4c172156664d2c5caaa4d550a32b0655427192dd92ce168c833"

	t.Log("Testing HEAD item request")

	// Upload a file, create an item
	filePath := uploadstring(t, "POST", "/upload", fileContent)
	t.Log("got file path", filePath)

	itemid := "zxcv" + randomid()
	txpath := sendtransaction(t, "/item/"+itemid+"/transaction",
		[][]string{{"add", path.Base(filePath)}, {"slot", "testFile1", path.Base(filePath)}}, 202)
	t.Log("got tx path", txpath)
	// tx is processed async from the commit above.
	// maybe wait for it to be committed?
	time.Sleep(10 * time.Millisecond) // sleep a squinch

	// verify the HEAD body is empty
	respBody := getbody(t, "HEAD", "/item/"+itemid+"/testFile1", 200)
	if respBody != "" {
		t.Errorf("Expected Empty HEAD body, got %s\n", respBody)
	}

	// see if item was cached
	t.Log("Testing Cache Header")
	resp := checkRoute(t, "HEAD", "/item/"+itemid+"/testFile1", 200)
	if resp == nil {
		t.Fatalf("Unexpected nil response")
	}
	resp.Body.Close()
	cacheStatus := resp.Header.Get("X-Cached")
	if cacheStatus != "0" {
		t.Errorf("X-Cached expected 0, received %s", cacheStatus)
	}
	shaHexSum := resp.Header.Get("X-Content-Sha256")
	if shaHexSum != fileShaHexSum {
		t.Errorf("X-Content-Sha256 expected %s, received %s", fileShaHexSum, shaHexSum)
	}

	// get file twice and see if second time was cached
	resp = checkRoute(t, "GET", "/item/"+itemid+"/testFile1", 200)
	resp.Body.Close()
	time.Sleep(10 * time.Millisecond) // sleep a squinch so the caching can happen
	resp = checkRoute(t, "GET", "/item/"+itemid+"/testFile1", 200)
	if resp == nil {
		t.Fatalf("Unexpected nil response")
	}
	resp.Body.Close()
	cacheStatus = resp.Header.Get("X-Cached")
	if cacheStatus != "1" {
		t.Errorf("X-Cached expected 1, received %s", cacheStatus)
	}
	shaHexSum = resp.Header.Get("X-Content-Sha256")
	if shaHexSum != fileShaHexSum {
		t.Errorf("X-Content-Sha256 expected %s, received %s", fileShaHexSum, shaHexSum)
	}
}

//
// Test Helpers
//

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

func sendtransaction(t *testing.T, route string, commands [][]string, statuscode int) string {
	content, _ := json.Marshal(commands)
	return uploadstringhash(t, "POST", route, string(content), "", statuscode)
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
		resp.Body.Close()
		if err != nil {
			t.Fatal(route, err)
		}
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
	server := &RESTServer{
		Validator: NobodyValidator{},
		Items:     items.New(store.NewMemory()),
		TxStore:   transaction.New(store.NewMemory()),
		FileStore: fragment.New(store.NewMemory()),
		Cache:     blobcache.NewLRU(store.NewMemory(), 400),
	}
	server.txgate = util.NewGate(MaxConcurrentCommits)
	server.TxStore.Load()
	testServer = httptest.NewServer(server.addRoutes())
}
