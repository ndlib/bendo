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
	"path"
	"sync"
	"time"

	"github.com/ndlib/bendo/util"
)

func main() {
	flag.Parse()
	wg := sync.WaitGroup{}
	gate := util.NewGate(*NumGoroutines)
	for i := 0; i < 10000; i++ {
		id := fmt.Sprintf("id%05dx", i)
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

func dumpbody(resp *http.Response) {
	b := new(bytes.Buffer)
	io.Copy(b, resp.Body)
	log.Printf("   > %s", b.String())
}

// Upload a few files to make a new item.

func CreateItem(id string) {
	var totalsize int64
	starttime := time.Now()

	log.Printf("Starting uploads for %s", id)
	nfiles := rand.Intn(10) + 1
	commands := [][]string{}
	for i := 0; i < nfiles; i++ {
		tempname, size := uploadfile()
		totalsize += size
		commands = append(commands,
			[]string{"add", tempname},
			[]string{"slot", slotnames[i], tempname})
	}
	buf, _ := json.Marshal(commands)
	resp, err := http.Post(*urlpath+"/item/"+id+"/transaction",
		"",
		bytes.NewReader(buf))
	if err != nil {
		log.Println(err)
		return
	}
	if resp.StatusCode != 202 {
		log.Printf("Received status %d: %s", resp.StatusCode, id)
		dumpbody(resp)
		return
	}
	resp.Body.Close()

	runDuration := time.Since(starttime)
	log.Printf("Created %s: %v bytes, %v time, %f MB/s", id, totalsize,
		runDuration,
		float64(totalsize/1000000)/runDuration.Seconds())

}

func uploadfile() (string, int64) {
	// upload content in chunks. first time is special
	var (
		route = "/upload"
		size  = rand.Intn(*MaxUpload * 1000000)
		sz    = size // use sz for loop, and size to return
		chunk = chunks.Get().(*Chunk)
	)
	for sz > 0 {
		req, _ := http.NewRequest("POST",
			*urlpath+route,
			bytes.NewReader(chunk.Data))
		req.Header.Set("X-Upload-Md5", hex.EncodeToString(chunk.MD5))
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			time.Sleep(1 * time.Millisecond)
			continue
		}
		if resp.StatusCode != 200 {
			log.Printf("Received HTTP status %d for %s", resp.StatusCode, route)
			dumpbody(resp)
			break
		}
		route = resp.Header.Get("Location")
		if route == "" {
			log.Printf("No Location returned on POST: %s", route)
			break
		}
		resp.Body.Close()
		sz -= len(chunk.Data)
	}
	chunks.Put(chunk)

	return path.Base(route), int64(size - sz)
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
	chunksize = 1 << 20
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
