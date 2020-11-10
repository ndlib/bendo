// +build blackpearl

package store

// Tests the BlackPearl interface code with an external service. Can use actual
// device, or the simulator.
//
// To run from the command line
//
//    env "DS3_ACCESS_KEY=XXXXX" "DS3_SECRET_KEY=YYYY" "DS3_ENDPOINT=" go test -tags=blackpearl -run BP
//
// Using the Spectra Logic BlackPearl Simulator
//  - Machine image is on this page:
//      https://developer.spectralogic.com/downloads/
//  - Documentation: https://developer.spectralogic.com/sim-install/
//
// Set up the machine using the documentation, create a bucket "bendo-test" and
// then run
//
//    env "DS3_ACCESS_KEY=YmVuZG8=" \
//        "DS3_SECRET_KEY=kG8RYsbf" \
//        "DS3_ENDPOINT=http://192.168.1.70:8080" \
//        go test -tags=blackpearl -run BP
//
// Replace the keys with those for the user you created and the IP address with
// the one the simulator is using.

import (
	"fmt"
	"hash/fnv"
	"io"
	"net/url"
	"os"
	"strconv"
	"testing"

	"github.com/SpectraLogic/ds3_go_sdk/ds3"
	//"github.com/SpectraLogic/ds3_go_sdk/ds3/buildclient"
	"github.com/SpectraLogic/ds3_go_sdk/ds3/networking"
	// ds3models "github.com/SpectraLogic/ds3_go_sdk/ds3/models"
)

const testBucketBP = "bendo-test"

func getClient(t *testing.T) *ds3.Client {
	// Create a client from environment variables. Since the simulator doesn't
	// have a valid SSL certificate, we need to build it ourselves.

	endpoint := os.Getenv("DS3_ENDPOINT")
	accessKey := os.Getenv("DS3_ACCESS_KEY")
	secretKey := os.Getenv("DS3_SECRET_KEY")

	switch {
	case endpoint == "":
		t.Fatal("DS3_ENDPOINT missing")
	case accessKey == "":
		t.Fatal("DS3_ACCESS_KEY missing")
	case secretKey == "":
		t.Fatal("DS3_SECRET_KEY missing")
	}

	endpointUrl, err := url.Parse(endpoint)
	if err != nil {
		t.Fatal(err)
	}

	return ds3.NewClientBuilder(
		endpointUrl,
		&networking.Credentials{AccessId: accessKey, Key: secretKey}).
		WithIgnoreServerCertificate(true).
		BuildClient()
}

func TestBPListPrefix(t *testing.T) {
	bp := NewBlackPearl(testBucketBP, "", getClient(t))
	items, err := bp.ListPrefix("")
	t.Log(err)
	t.Log(items)
	if len(items) == 0 {
		return
	}
	r, size, err := bp.Open(items[0])
	t.Log(size, err)
	n, err := io.Copy(os.Stdout, NewReader(r))
	t.Log(n)
	t.Logf("%#v", err)
	r.Close()
}

func TestBPCreate(t *testing.T) {
	bp := NewBlackPearl(testBucketBP, "create/", getClient(t))

	// not sure whether to delete at the beginning or at the end
	bp.Delete("first")
	bp.Delete("second")

	// Try to write first (short) file.
	w, err := bp.Create("first")
	if err != nil {
		t.Fatal(err)
	}
	n, err := w.Write([]byte("abcdefghijklmnopqrstuvwxyz"))
	if err != nil {
		t.Error(err)
	}
	err = w.Close()
	if err != nil {
		t.Error(err)
	}

	// now write second (larger) file
	w, err = bp.Create("second")
	if err != nil {
		t.Fatal(err)
	}
	hash := fnv.New64a() // to verify correctness
	totallength := int64(0)
	for totallength < 100_000_000 {
		const data = "abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz"
		hash.Write([]byte(data))
		n, err = w.Write([]byte(data))
		if err != nil {
			t.Error(err)
		}
		totallength += int64(n)
	}
	err = w.Close()
	if err != nil {
		t.Error(err)
	}
	uploadHash := hash.Sum64()

	// does the uploaded file have a matching hash?
	r, size, err := bp.Open("second")
	if err != nil {
		t.Fatal(err)
	}
	if size != totallength {
		t.Error("Uploaded item length is", size, "expected", totallength)
	}
	hash.Reset()
	nn, err := io.Copy(hash, NewReader(r))
	if err != nil {
		t.Error(err)
	}
	err = r.Close()
	if err != nil {
		t.Error(err)
	}
	if nn != size {
		t.Error("Read", nn, "expected", size)
	}
	if uploadHash != hash.Sum64() {
		t.Error("Read hash", hash.Sum64(), "expected", uploadHash)
	}
}

func TestBPList(t *testing.T) {
	// The simulator is slow, so don't upload too many items
	const N = 100

	bp := NewBlackPearl(testBucketBP, "list/", getClient(t))

	// add items
	for i := 0; i < N; i++ {
		w, err := bp.Create(fmt.Sprintf("%d", i))
		if err != nil {
			t.Error(err)
			continue
		}
		_, err = w.Write([]byte("01234567890123456789"))
		if err != nil {
			t.Error(err)
		}

		err = w.Close()
		if err != nil {
			t.Error(err)
		}
	}

	// see if everything was found
	nfound := 0
	c := bp.List()
	for name := range c {
		_, err := strconv.Atoi(name)
		if err != nil {
			t.Error(err)
			continue
		}
		nfound++
		err = bp.Delete(name)
		if err != nil {
			t.Error(err)
		}
	}
	if nfound != N {
		t.Error("expected", N, "found", nfound)
	}
}

func TestS3Delete(t *testing.T) {
	bp := NewBlackPearl(testBucketBP, "delete/", getClient(t))
	w, err := bp.Create("first")
	if err != nil {
		t.Log(err)
	}
	_, err = w.Write([]byte("abcdefghijklmnopqrstuvwxyz"))
	if err != nil {
		t.Error(err)
	}
	err = w.Close()
	if err != nil {
		t.Error(err)
	}

	err = bp.Delete("first")
	if err != nil {
		t.Error(err)
	}

	// make sure not an error to delete non-existent item
	err = bp.Delete("first")
	if err != nil {
		t.Error(err)
	}
}
