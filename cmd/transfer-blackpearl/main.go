// transfer-blackpearl is a utility to copy files from a filesystem to a
// BlackPearl storage device. It is needed to handle the rename from the
// pair-tree directory structure Bendo uses for filesystems to a flat naming
// convention Bendo uses for object stores.
//
// For example, file trees such as
//
//	ab/
//		cd/
//			abcd123-0001.zip
//			abcd456-0001.zip
//	ef/
//		gh/
//			efgh-0001.zip
//
// get flattened to
//
// abcd123-0001.zip
// abcd456-0001.zip
// efgh-0001.zip
//
// RUNNING
//
// To run this you need to provide a path to the root of the file tree you want
// to upload; the address and storage prefixes for the BlackPearl device; and
// the authorization credentials for the BlackPearl device.
//
// Example command line:
//
// $ env "DS3_ACCESS_KEY=YmVuZG8=" "DS3_SECRET_KEY=kG8RYsbf" ./bin/transfer-blackpearl bendo_storage/00 blackpearl://192.168.1.71:8080/tester/prefix
//
// The goal of this is to move all the files in the source tree to the BlackPearl.
// If there is an error, it is safe to restart.
// Upload verification is expected to be handled by another utility or process.

package main

import (
	"io"
	"io/ioutil"
	"log"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/SpectraLogic/ds3_go_sdk/ds3"
	ds3models "github.com/SpectraLogic/ds3_go_sdk/ds3/models"
	"github.com/SpectraLogic/ds3_go_sdk/ds3/networking"
)

var (
	Bucket string
	Prefix string
	Client *ds3.Client
)

// A file stores a single file we found in the source tree. It has a full path
// to the file, its size, and the name it should have on the target system.
type file struct {
	path   string
	size   int64
	target string
}

func listfiles(root string) ([]file, error) {
	infos, err := ioutil.ReadDir(root)
	if err != nil {
		return nil, err
	}

	var result []file
	for _, info := range infos {
		name, _ := filepath.Abs(filepath.Join(root, info.Name()))
		if info.IsDir() {
			files, err := listfiles(name)
			if err != nil {
				return nil, err
			}
			result = append(result, files...)
		} else {
			result = append(result, file{
				path:   name,
				size:   info.Size(),
				target: filepath.Join(Prefix, filepath.Base(name)),
			})
		}
	}
	return result, nil
}

// chunk is a unit of upload as determined by the BlackPearl
type chunk struct {
	name   string
	offset int64
	length int64
}

func uploadbuffer(jobID string, r io.Reader, c chunk) error {
	rr := &limitedSizeReader{io.LimitedReader{R: r, N: c.length}}
	input := ds3models.NewPutObjectRequest(Bucket, c.name, rr).
		WithJob(jobID).
		WithOffset(c.offset)

	_, err := Client.PutObject(input)
	return err
}

// sizeReader adds a Size() function. This is needed for the BlackPearl SDK.
type limitedSizeReader struct {
	io.LimitedReader
}

func (s *limitedSizeReader) Size() (int64, error) {
	return s.N, nil
}

// waitForBlackPearl will block until the BlackPearl is ready for the next
// upload of the given jobid. It will return early if there an an error.
func waitForBlackPearl(jobID string) ([]chunk, error) {
	// wait until BP is ready for an upload
	for {
		input := ds3models.NewGetJobChunksReadyForClientProcessingSpectraS3Request(jobID)
		resp, err := Client.GetJobChunksReadyForClientProcessingSpectraS3(input)
		if err != nil {
			return nil, err
		}

		// Can any chunks be processed?
		numberOfChunks := len(resp.MasterObjectList.Objects)
		if numberOfChunks > 0 {
			var result []chunk
			for _, c := range resp.MasterObjectList.Objects {
				for _, d := range c.Objects {
					result = append(result, chunk{
						name:   *d.Name,
						offset: d.Offset,
						length: d.Length,
					})
				}
			}
			return result, nil
		}

		// If the Get Job Chunks Ready for Processing request returns an empty list,
		// then the server's cache is currently saturated and the client must wait
		// before sending more data. The client should wait the number of seconds
		// specified in the Retry-After HTTP response header.
		timeout := 10 * time.Second // default to 10 seconds
		if s := resp.Headers.Get("Retry-After"); s != "" {
			v, err := strconv.Atoi(s)
			if err == nil && v > 0 {
				timeout = time.Duration(v) * time.Second
			}
		}
		log.Println("waiting for blackpearl", timeout.Seconds())
		time.Sleep(timeout)
	}
}

func uploadfiles(files []file) error {
	// map from the target file name back to the source file.
	filemap := make(map[string]string)

	// convert our file list into the format the BlackPearl expects.
	bpFiles := make([]ds3models.Ds3PutObject, len(files))
	for i := range files {
		filemap[files[i].target] = files[i].path

		bpFiles[i] = ds3models.Ds3PutObject{
			Name: files[i].target,
			Size: files[i].size,
		}
	}

	// Start the bulk request with only this one file to upload.
	request := ds3models.NewPutBulkJobSpectraS3Request(Bucket, bpFiles).
		WithVerifyAfterWrite(true) // unsure if we need this option
	resp, err := Client.PutBulkJobSpectraS3(request)
	if err != nil {
		return err
	}

	jobID := resp.MasterObjectList.JobId
	chunkCount := len(resp.MasterObjectList.Objects)

	for ; chunkCount > 0; chunkCount-- {
		// wait until BP is ready for an upload
		chunks, err := waitForBlackPearl(jobID)
		if err != nil {
			return err
		}

		for _, chunk := range chunks {
			log.Println(chunk)

			path := filemap[chunk.name]
			if path == "" {
				log.Println("Error: No source file for target", chunk.name)
				continue
			}
			f, err := os.Open(path)
			if err != nil {
				log.Println(path, err)
				continue
			}
			// go to the right place
			_, err = f.Seek(chunk.offset, os.SEEK_SET)
			if err == nil {
				err = uploadbuffer(jobID, f, chunk)
			}
			f.Close()
			if err != nil {
				log.Println(path, err)
			}
		}
	}

	return err
}
func splitBucketPrefix(location string) (bucket, prefix string) {
	if location == "" {
		return
	}
	location = strings.TrimPrefix(location, "/")
	v := strings.SplitN(location, "/", 2)
	bucket = v[0]
	if len(v) > 1 {
		prefix = v[1]
	}
	if prefix != "" && !strings.HasSuffix(prefix, "/") {
		prefix = prefix + "/"
	}
	return
}

func setupclient(location string) {
	u, _ := url.Parse(location)
	// build the URL for the blackpearl
	endpoint := &url.URL{
		Scheme: "http",
		Host:   u.Host,
	}
	if u.Host == "blackpearls" {
		endpoint.Scheme = "https"
	}
	Bucket, Prefix = splitBucketPrefix(u.Path)
	if Bucket == "" {
		log.Fatalln("cannot find bucket name in", location)
	}
	accessKey := os.Getenv("DS3_ACCESS_KEY")
	if accessKey == "" {
		log.Fatalln("Set $DS3_ACCESS_KEY")
	}
	secretKey := os.Getenv("DS3_SECRET_KEY")
	if secretKey == "" {
		log.Fatalln("Set $DS3_SECRET_KEY")
	}
	Client = ds3.NewClientBuilder(
		endpoint,
		&networking.Credentials{AccessId: accessKey, Key: secretKey},
	).BuildClient()
}

func main() {
	if len(os.Args) != 3 {
		log.Fatalln("USAGE: transfer-blackpearl ${SOURCE_DIR} ${TARGET_BP}")
	}
	source := os.Args[1]
	target := os.Args[2]
	setupclient(target)
	log.Println("Scanning source tree")
	files, err := listfiles(source)
	if err != nil {
		log.Fatalln(err)
	}

	log.Println("Found", len(files), "files")

	err = uploadfiles(files)
	if err != nil {
		log.Fatalln(err)
	}
}
