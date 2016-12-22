package server

import (
	"path"
	"testing"
)

// test /admin/tape_use commmands
func TestTapeUseAdmin(t *testing.T) {
	// make sure tape is turned on at the end
	defer checkStatus(t, "PUT", "/admin/use_tape/on", 201)

	checkStatus(t, "PUT", "/admin/use_tape/on", 201)

	text := getbody(t, "GET", "/admin/use_tape", 200)
	if text != "On" {
		t.Fatalf("Received %#v, expected %#v", text, "On")
	}

	checkStatus(t, "PUT", "/admin/use_tape/off", 201)

	text = getbody(t, "GET", "/admin/use_tape", 200)
	if text != "Off" {
		t.Fatalf("Received %#v, expected %#v", text, "Off")
	}
}

// test Blobs under no_tape_use
func TestTapeItem(t *testing.T) {
	// make sure tape is turned on at the end
	defer checkStatus(t, "PUT", "/admin/use_tape/on", 201)

	// upload a blob, and commit an item
	checkStatus(t, "PUT", "/admin/use_tape/on", 201)
	blob1 := uploadstring(t, "POST", "/upload", "hello world")
	blob2 := uploadstring(t, "POST", "/upload", "goodbye cruel world")
	t.Log("blob1 =", blob1)
	t.Log("blob2 =", blob2)

	itemid := "zxcvbnm" + randomid()
	txpath := sendtransaction(t,
		"/item/"+itemid+"/transaction",
		[][]string{
			{"add", path.Base(blob1)},
			{"slot", "testFile1", path.Base(blob1)},
			{"add", path.Base(blob2)},
			{"slot", "testFile2", path.Base(blob2)}},
		202)
	t.Log("got tx path", txpath)
	// tx is processed async from the commit above.
	waitTransaction(t, txpath)

	t.Log("first")
	checkStatus(t, "GET", "/item/"+itemid+"/testFile1", 200)
	t.Log("second")
	checkStatus(t, "GET", "/item/"+itemid+"/testFile1", 200)

	checkStatus(t, "PUT", "/admin/use_tape/off", 201)
	// get item cached
	t.Log("third")
	checkStatus(t, "GET", "/item/"+itemid+"/testFile1", 200)
	// get item non-cached
	checkStatus(t, "GET", "/item/"+itemid+"/testFile2", 503)
}

func TestTapeBundle(t *testing.T) {
	// make sure tape is turned on at the end
	defer checkStatus(t, "PUT", "/admin/use_tape/on", 201)

	checkStatus(t, "PUT", "/admin/use_tape/on", 201)
	checkStatus(t, "GET", "/bundle/list/", 200)
	checkStatus(t, "GET", "/bundle/list/noone", 200)
	checkStatus(t, "GET", "/bundle/open/noone", 404)
	checkStatus(t, "PUT", "/admin/use_tape/off", 201)
	checkStatus(t, "GET", "/bundle/list/", 503)
	checkStatus(t, "GET", "/bundle/list/noone", 503)
	checkStatus(t, "GET", "/bundle/open/noone", 503)
}
