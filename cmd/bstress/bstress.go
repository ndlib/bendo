package main

// Stress test a bendo instance
//
// Parameters:
//  N   - The number of goroutines to use. Default is 100
//  Z   - The maximum size of upload. Default is 100 MB
//
//  url - the url of the bendo instance. Default is http://localhost:14000

import (
	"bytes"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/ndlib/bendo/util"
)

func main() {
	flag.Parse()
	wg := sync.WaitGroup{}
	gate := util.NewGate(*NumGoroutines)
	for i := 0; i < 10000; i++ {
		id := fmt.Sprintf("id%5dx", i)
		wg.Add(1)
		go func() {
			gate.Enter()
			CreateItem(id)
			gate.Leave()
			wg.Done()
		}()
	}
	wg.Wait()
}

var (
	NumGoroutines = flag.Int("n", 100, "number of goroutines")
	MaxUpload     = flag.Int("z", 100, "max file size in MB")
	urlpath       = flag.String("url", "http://localhost:14000", "base url of service to test")

	slotnames = []string{
		"descMetadata",
		"image",
		"12345/content.xml",
		"12345/meta",
		"thumbnail",
		"12345/thumbnail",
		"rightsMetadata",
		"12345/rightsMetadata",
		"RELS-EXT",
		"foxml",
	}
)

// Upload a few files to make a new item.

func CreateItem(id string) {
	resp, _ := http.Post(*urlpath+"/item/"+id+"/transaction", "", nil)
	resp.Body.Close()
	txpath := resp.Header.Get("Location")

	nfiles := rand.Intn(10)
	slots := [][]string{}
	for i := 0; i < nfiles; i++ {
		tempname := uploadfile(txpath)
		slots = append(slots, []string{"slot", slotnames[i], tempname})
	}
	// TODO: upload slot names
	buf, _ := json.Marshal(slots)
	Put(*urlpath+txpath+"/commands", bytes.NewReader(buf))
	resp, _ = http.Post(*urlpath+txpath+"/commit", "", nil)
	resp.Body.Close()
	if resp.StatusCode != 200 {
		log.Printf("Received status %d: %s", resp.Status, id)
	}
}

func uploadfile(txpath string) string {
	// upload content in chunks. first time is special
	var firsttime = true
	var route = txpath
	var verb = "POST"
	size := rand.Intn(*MaxUpload)
	chunk := chunks.Get().(*Chunk)
	for size > 0 {
		req, _ := http.NewRequest(verb,
			*urlpath+route,
			bytes.NewReader(chunk.Data))
		req.Header.Set("X-Upload-Md5", hex.EncodeToString(chunk.MD5))
		resp, _ := http.DefaultClient.Do(req)
		resp.Body.Close()
		if firsttime {
			firsttime = false
			verb = "PUT"
			route = resp.Header.Get("Location")
		}
		if resp.StatusCode != 200 {
			log.Printf("Received HTTP status %d for %s", resp.StatusCode, route)
		}
		size -= len(chunk.Data)
	}
	chunks.Put(chunk)

	// figure out this file's id
	routePieces := strings.Split(route, "/")
	return routePieces[len(routePieces)-1]
}

func Put(path string, r io.Reader) {
	req, _ := http.NewRequest("PUT", path, r)
	resp, _ := http.DefaultClient.Do(req)
	resp.Body.Close()
}

/************************/

type Chunk struct {
	Data []byte
	MD5  []byte // the md5 hash of the data
}

var (
	chunks *sync.Pool = &sync.Pool{New: NewChunk}
)

const (
	chunksize = 1 << 16
)

func NewChunk() interface{} {
	start := byte(rand.Intn(256))
	c := make([]byte, chunksize)
	for i := 0; i < chunksize; i++ {
		c[i] = start
		start = (start + 1) & 0xff
	}
	h := md5.Sum(c)
	return &Chunk{
		Data: c,
		MD5:  h[:],
	}
}
