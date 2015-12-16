package server

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"io/ioutil"
	"log"
	"net/http"
	"net/http/httptest"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/ndlib/bendo/fragment"
	"github.com/ndlib/bendo/items"
	"github.com/ndlib/bendo/store"
	"github.com/ndlib/bendo/transaction"
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
	err := openDatabase("memory")
	if err != nil {
		log.Println(err.Error())
	}
	server := &RESTServer{
		Validator: NewNobodyValidator(),
		Items:     items.New(store.NewMemory()),
		TxStore:   transaction.New(store.NewMemory()),
		FileStore: fragment.New(store.NewMemory()),
	}
	server.TxStore.Load()
	testServer = httptest.NewServer(server.addRoutes())
}
