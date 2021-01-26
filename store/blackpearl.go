package store

// The BlackPearl code was forked from the S3 client code. I am unsure how
// similar/different they really are. For now it seemed easier to treat them as
// different.

import (
	"io"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/SpectraLogic/ds3_go_sdk/ds3"
	ds3models "github.com/SpectraLogic/ds3_go_sdk/ds3/models"
	raven "github.com/getsentry/raven-go"
)

// A BlackPearl store represents a store that is kept on a SpectraLogic's
// BlackPearl appliance.
// Do not change Bucket or Prefix concurrently with calls using the structure.
type BlackPearl struct {
	client  *ds3.Client
	Bucket  string
	Prefix  string
	TempDir string     // where to make temp files. "" uses default place
	sizes   *sizecache // track HEAD info
}

// NewBlackPearl creates a new BlackPearl store. It will use the given bucket
// and will prepend prefix to all keys. This is to allow for a bucket to be
// used for more than one store. For example if prefix were "cache/" then an
// Open("hello") would look for the key "cache/hello" in the bucket. The
// authorization method and credentials in the session are used for all
// accesses.
func NewBlackPearl(bucket, prefix string, client *ds3.Client) *BlackPearl {
	return &BlackPearl{
		Bucket: bucket,
		Prefix: prefix,
		client: client,
		sizes:  newSizeCache(),
	}
}

// listeach will call f once for each item in the bucket with the given prefix.
// The prefix is added to the prefix in the BlackPearl structure, if any.
// If any errors occur, the listeach will return early.
func (bp *BlackPearl) listeach(prefix string, f func(string)) error {
	marker := ""
	for {
		request := ds3models.NewGetBucketRequest(bp.Bucket)
		request.WithPrefix(bp.Prefix + prefix)
		if marker != "" {
			request.WithMarker(marker)
		}
		response, err := bp.client.GetBucket(request)
		if err != nil {
			log.Println("BlackPearl listeach:", bp.Prefix, prefix, err)
			raven.CaptureError(err, map[string]string{
				"Bucket":  bp.Bucket,
				"Prefix":  bp.Prefix,
				"Pattern": prefix})
			return err
		}

		for _, obj := range response.ListBucketResult.Objects {
			f(strings.TrimPrefix(*obj.Key, bp.Prefix))
		}

		// done paging?
		if response.ListBucketResult.NextMarker == nil {
			return nil
		}
		marker = *response.ListBucketResult.NextMarker
	}
}

// List returns a list of all the keys in this store. It will only return ones
// that satisfy the store's Prefix, so it is safe to use this on a bucket
// containing other items.
func (bp *BlackPearl) List() <-chan string {
	out := make(chan string)
	go func() {
		defer close(out)
		bp.listeach("", func(s string) {
			out <- s
		})
	}()
	return out
}

// ListPrefix returns the keys in this store that have the given prefix.
// The argument prefix is added to the store's Prefix.
func (bp *BlackPearl) ListPrefix(prefix string) ([]string, error) {
	var result []string
	err := bp.listeach(prefix, func(s string) {
		result = append(result, s)
	})
	return result, err
}

// Open will return a ReadAtCloser to get the content for the given key.
//
// The current implementation will download the entire contents for every call
// to Open(). There is a lot of room for optimization and caching.
func (bp *BlackPearl) Open(key string) (ReadAtCloser, int64, error) {
	// check that the key exists, and if so get its size
	size, err := bp.stat(key)
	if err != nil {
		return nil, 0, err
	}
	fullkey := bp.Prefix + key
	f, err := ioutil.TempFile(bp.TempDir, "bp-download-")
	if err != nil {
		return nil, size, err
	}
	err = bp.downloadObject(f, fullkey)
	result := &bpTempFileReadAtCloser{f}
	if err != nil {
		result.Close() // this will remove the temp file
	}
	return result, size, err
}

// Create will return a WriteCloser to upload content to the given key. Data is
// batched into a temporary file and then uploaded when Close() is called.
// Might need to make directoy of the temporary file be configurable.
func (bp *BlackPearl) Create(key string) (io.WriteCloser, error) {
	// does the key already exist?
	_, err := bp.stat(key)
	if err == nil {
		return nil, ErrKeyExists
	}
	bp.sizes.Set(key, 0) // make 0 in case this key was previously deleted
	fullkey := bp.Prefix + key

	f, err := ioutil.TempFile(bp.TempDir, "blackpearl-")
	if err != nil {
		return nil, err
	}

	return &bpWriteCloser{
		client:   bp.client,
		bucket:   bp.Bucket,
		key:      fullkey,
		tempfile: f,
	}, nil
}

// Delete will remove the given key from the store. The store's Prefix is
// prepended first. It is not an error to delete something that doesn't exist.
func (bp *BlackPearl) Delete(key string) error {
	_, err := bp.client.DeleteObject(
		ds3models.NewDeleteObjectRequest(
			bp.Bucket,
			bp.Prefix+key,
		))
	// It is not an error if an item doesn't exist.
	if e, ok := err.(ds3models.BadStatusCodeError); ok {
		if e.ActualStatusCode == 404 {
			err = nil
		}
	}
	if err != nil {
		log.Println("BlackPearl Delete:", bp.Prefix, key, err)
		raven.CaptureError(err, map[string]string{
			"Bucket": bp.Bucket,
			"Prefix": bp.Prefix,
			"Key":    key})
	} else {
		bp.sizes.Set(key, sizeDeleted)
	}
	return err
}

// Stage will take a list of keys and ask the BlackPearl to stage all of them
// in its cache. It is for performance only and doesn't need to be called.
func (bp *BlackPearl) Stage(keys []string) {
	// need to prefix all the keys
	var prefixkeys []string
	for _, key := range keys {
		prefixkeys = append(prefixkeys, bp.Prefix+key)
	}
	_, _ = bp.client.StageObjectsJobSpectraS3(
		ds3models.NewStageObjectsJobSpectraS3Request(bp.Bucket, prefixkeys),
	)
}

// stat will check if a key exists, and if so it returns the size. If the item
// does not exist an error is returned. The prefix is added to the key before
// checking.
func (bp *BlackPearl) stat(key string) (int64, error) {
	// Cache the key sizes as we see them. This drastically cuts down on the
	// number of HEAD requests.
	return bp.sizes.Get(key, bp.stat0)
}

// stat0 implements the actual HEAD request to the blackpearl. Returns either
// an error or the size. You probably want to call stat().
func (bp *BlackPearl) stat0(key string) (int64, error) {
	// seems like there should be a better way of doing this??
	info, err := bp.client.HeadObject(
		ds3models.NewHeadObjectRequest(
			bp.Bucket,
			bp.Prefix+key,
		))
	if err != nil {
		return 0, err
	}
	x := info.Headers.Get("Content-Length")
	if x == "" {
		return 0, nil
	}
	xx, err := strconv.Atoi(x)
	return int64(xx), err
}

// bpTempFileReadAtCloser wraps a file ReadAt interface that will delete the
// file when it is closed.
type bpTempFileReadAtCloser struct {
	*os.File
}

func (tf *bpTempFileReadAtCloser) Close() error {
	name := tf.File.Name()
	err := tf.File.Close()
	err2 := os.Remove(name)
	if err == nil {
		err = err2
	}
	return err
}

// downloadObject will copy the entire contents of the given object into the
// provided writer.
func (bp *BlackPearl) downloadObject(w io.Writer, key string) error {
	request := ds3models.NewGetBulkJobSpectraS3Request(bp.Bucket, []string{key})
	resp, err := bp.client.GetBulkJobSpectraS3(request)
	if err != nil {
		return err
	}

	jobID := resp.MasterObjectList.JobId
	chunkCount := len(resp.MasterObjectList.Objects)

	for ; chunkCount > 0; chunkCount-- {
		// wait until BP is ready for download
		chunks, err := waitForBlackPearl(bp.client, jobID)
		if err != nil {
			return err
		}
		for _, chunk := range chunks {
			log.Println("download", chunk)
			input := ds3models.NewGetObjectRequest(bp.Bucket, key).
				WithJob(jobID).
				WithOffset(chunk.offset)
			response, err := bp.client.GetObject(input)
			if err != nil {
				return err
			}
			_, err = io.Copy(w, response.Content)
			if err != nil {
				return err
			}
			response.Content.Close()
		}
	}
	return nil
}

// bpWriteCloser does an upload to the BlackPearl appliance. We do this using
// the Bullk PUT interface. Since this interface requires us to know the file
// size, we first save the file to a local temporary file. When this is
// finished, we then upload the file to the BlackPearl.
//
// It is possible the place temporary files are saved is not big enough to
// store our file. We should probably make the directory used configurable.
type bpWriteCloser struct {
	client   *ds3.Client
	bucket   string
	key      string // with prefix, if any
	tempfile *os.File
}

func (wc *bpWriteCloser) Write(p []byte) (int, error) {
	return wc.tempfile.Write(p)
}

// Close will take the temporary file created and upload it to the BlackPearl.
// This call will not return until either the temporary file has been
// completely transferred to the BlackPearl or an error occurs. Either way, the
// temporary file will be deleted.
func (wc *bpWriteCloser) Close() error {
	defer func() {
		// close and delete the temp file
		if wc.tempfile != nil {
			name := wc.tempfile.Name()
			wc.tempfile.Close()
			err := os.Remove(name)
			if err != nil {
				log.Println("bpWriteCloser", err)
			}
		}
	}()

	info, err := wc.tempfile.Stat()
	if err != nil {
		return err
	}
	size := info.Size()

	// Start the bulk request with only this one file to upload.
	request := ds3models.NewPutBulkJobSpectraS3Request(
		wc.bucket,
		[]ds3models.Ds3PutObject{{Name: wc.key, Size: size}}).
		WithVerifyAfterWrite(true) // unsure if we need this option
	resp, err := wc.client.PutBulkJobSpectraS3(request)
	if err != nil {
		return err
	}

	jobID := resp.MasterObjectList.JobId
	chunkCount := len(resp.MasterObjectList.Objects)

	for ; chunkCount > 0; chunkCount-- {
		// wait until BP is ready for an upload
		chunks, err := waitForBlackPearl(wc.client, jobID)
		if err != nil {
			return err
		}

		for _, chunk := range chunks {
			log.Println(chunk)

			// go to the right place
			_, err = wc.tempfile.Seek(chunk.offset, os.SEEK_SET)
			if err != nil {
				return err
			}

			err := wc.uploadbuffer(jobID, wc.tempfile, chunk)
			if err != nil {
				log.Println("BlackPearl Close:", wc, err)
			}
		}
	}

	return err
}

func (wc *bpWriteCloser) uploadbuffer(jobID string, r io.Reader, c chunk) error {
	rr := &limitedSizeReader{io.LimitedReader{R: r, N: c.length}}
	input := ds3models.NewPutObjectRequest(wc.bucket, wc.key, rr).
		WithJob(jobID).
		WithOffset(c.offset)

	_, err := wc.client.PutObject(input)
	return err
}

// sizeReader adds a Size() function. This is needed for the BlackPearl SDK.
type limitedSizeReader struct {
	io.LimitedReader
}

func (s *limitedSizeReader) Size() (int64, error) {
	return s.N, nil
}

type chunk struct {
	name   string
	offset int64
	length int64
}

// waitForBlackPearl will block until the BlackPearl is ready for the next
// upload of the given jobid. It will return early if there an an error.
func waitForBlackPearl(client *ds3.Client, jobID string) ([]chunk, error) {
	// wait until BP is ready for an upload
	for {
		input := ds3models.NewGetJobChunksReadyForClientProcessingSpectraS3Request(jobID)
		resp, err := client.GetJobChunksReadyForClientProcessingSpectraS3(input)
		if err != nil {
			// TODO figure out what to do
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
