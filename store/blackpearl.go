package store

// The BlackPearl code was forked from the S3 client code. I am unsure how
// similar/different they really are. For now it seemed easier to treat them as
// different.

import (
	"bytes"
	"io"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
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
	TempDir string          // where to make temp files. "" uses default place
	m       sync.RWMutex    // protects everything below
	cache   map[string]head // cache for item sizes
}

// NewBlackPearl creates a new BlackPearl store. It will use the given bucket
// and will prepend prefix to all keys. This is to allow for a bucket to be
// used for more than one store. For example if prefix were "cache/" then an
// Open("hello") would look for the key "cache/hello" in the bucket. The
// authorization method and credentials in the session are used for all
// accesses.
func NewBlackPearl(bucket, prefix string, client *ds3.Client) *BlackPearl {
	bp := &BlackPearl{
		Bucket: bucket,
		Prefix: prefix,
		client: client,
		cache:  make(map[string]head),
	}
	go bp.background()
	return bp
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

// Open will return a ReadAtCloser to get the content for the given key. Data
// is paged in as needed, and up to 50 MB or so is cached at a time.
func (bp *BlackPearl) Open(key string) (ReadAtCloser, int64, error) {
	// check that the key exists, and if so get its size
	size, err := bp.stat(key)
	if err != nil {
		return nil, 0, err
	}
	result := &bpReadAtCloser{
		client: bp.client,
		bucket: bp.Bucket,
		key:    bp.Prefix + key,
		size:   size,
	}
	return result, size, nil
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
	bp.setkeysize(key, 0) // make 0 in case this key was previously deleted
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
		bp.setkeysize(key, sizeDeleted)
	}
	return err
}

// stat will check if a key exists, and if so it returns the size. If the item
// does not exist an error is returned. The prefix is added to the key before
// checking.
func (bp *BlackPearl) stat(key string) (int64, error) {
	// Cache the key sizes as we see them. This drastically cuts down on the
	// number of HEAD requests.
	bp.m.Lock()
	defer bp.m.Unlock()
	entry := bp.cache[key]
	if entry.size > 0 {
		return entry.size, nil
	}
	if entry.size < 0 {
		// we have previously determined this key does not exist
		return 0, ErrNotExist
	}
	// key is not cached, so do the HEAD request
	size, err := bp.stat0(key)
	bp.setkeysize0(key, size)
	return size, err
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

/////// size cache

// setkeysize caches a size to use for the given key.
// use sizeDeleted to mark the key as missing.
//
// Do not hold the s.m lock when calling this.
func (bp *BlackPearl) setkeysize(key string, size int64) {
	bp.m.Lock()
	bp.setkeysize0(key, size)
	bp.m.Unlock()
}

// setkeysize0 is just like setkeysize but assumes caller already has a lock
// on s.m
func (bp *BlackPearl) setkeysize0(key string, size int64) {
	ttl := defaultHitTTL
	switch {
	case size < 0:
		ttl = defaultMissTTL
	case size == 0:
		ttl = 0
	}
	bp.cache[key] = head{ttl: ttl, size: size}
}

// ageSizeCache will age all the cache entries, and remove the ones that have
// become too old. It holds m the entire time.
func (bp *BlackPearl) ageSizeCache() {
	bp.m.Lock()
	defer bp.m.Unlock()
	for k, v := range bp.cache {
		v.ttl--
		if v.ttl < 0 {
			delete(bp.cache, k) // remove aged entries
		} else {
			bp.cache[k] = v
		}
	}
}

func (bp *BlackPearl) background() {
	for {
		time.Sleep(time.Hour) // arbitrary length

		log.Println("Start BlackPearl ageSizeCache", bp.Bucket, bp.Prefix)
		start := time.Now()
		bp.ageSizeCache()
		log.Println("End BlackPearl ageSizeCache", bp.Bucket, bp.Prefix, time.Now().Sub(start))
	}
}

// }}} size cache

// bpReadAtCloser adapts the Reader we get for loading content
// to the ReadAt interface. It keeps a LRU cache of downloaded pages.
//
// The pages can start at any offset, and it is possible pages in memory may
// overlap. Though, in the expected case of a sequential read through the file,
// the pages will be disjoint.
//
// It is not safe to use access this from more than one goroutine.
type bpReadAtCloser struct {
	client *ds3.Client
	bucket string
	key    string   // with prefix, if any
	pages  []bpPage // cache of data we've downloaded
	size   int64    // size of this item in bytes
}

type bpPage struct {
	data   []byte
	offset int64
}

// ReadAt implements the io.ReadAt interface.
func (rac *bpReadAtCloser) ReadAt(p []byte, offset int64) (int, error) {
	// since the readat interface allows jumping around the file, we don't
	// use the bulk get since that
	//  1) returns chunks in potentially random order, and
	//	2) doesn't let us control which chunks we want with range headers.
	// Instead we use the S3 object GET interface, which does support what
	// we want. This may turn out to be a bad idea.
	var err error
	startOffset := offset
	for len(p) > 0 {
		if offset >= rac.size {
			break
		}
		var page bpPage
		page, err = rac.getpage(offset)
		if err != nil {
			// don't return, in case we have already copied some data
			break
		}
		n := copy(p, page.data[offset-page.offset:])
		p = p[n:]
		offset += int64(n)
	}
	// If we copied data and have an EOF, dont return the EOF yet. Conversely
	// if we did not end up copying any data and there is no error, then assume
	// we reached the end and return EOF.
	if err == io.EOF && startOffset != offset {
		err = nil
	} else if err == nil && startOffset == offset {
		err = io.EOF
	}
	return int(offset - startOffset), err
}

// getpage will find or load a page for the given offset
func (rac *bpReadAtCloser) getpage(offset int64) (bpPage, error) {
	i := rac.findpage(offset)
	if i == -1 {
		// page was not found, try to get it
		page, err := rac.loadpage(offset)
		if err != nil {
			return bpPage{}, err
		}
		// if the cache is not too big yet, add it to the end
		// otherwise replace the last entry with it
		if len(rac.pages) < defaultNumPages {
			rac.pages = append(rac.pages, page)
		}
		i = len(rac.pages) - 1
		rac.pages[i] = page
	}
	page := rac.pages[i]
	if i > 0 {
		// move page to front of cache
		copy(rac.pages[1:], rac.pages[:i]) // don't need to copy entry i
		rac.pages[0] = page
	}
	return page, nil
}

// findpage sees if any page in the cache contains the data for the byte at
// offset. If so, it returns the index of the page in the cache. Otherwise -1
// is returned.
func (rac *bpReadAtCloser) findpage(offset int64) int {
	for i, page := range rac.pages {
		base := page.offset
		limit := base + int64(len(page.data))
		if base <= offset && offset < limit {
			return i
		}
	}
	return -1
}

// loadpage will read one page of data. It tries to read defaultPageSize bytes,
// but it may be smaller at the end of the file. Hence pages may be of various
// sizes. It also choses a starting offset that is a multiple of
// defaultPageSize, so all pages in memory are disjoint.
func (rac *bpReadAtCloser) loadpage(offset int64) (bpPage, error) {
	// try to fill up the page, but don't ask for more than
	// the file has. The BlackPearl doesn't like that.
	// Take the page start to be the greatest multiple of defaultPageSize less
	// than the given offset
	startpos := (offset / defaultPageSize) * defaultPageSize
	endpos := startpos + defaultPageSize
	if endpos > rac.size {
		endpos = rac.size
	}
	// not sure how to use the bulk get when we only know of one item at this
	// point in time. ALSO we don't have a palce to store the other items since
	// we mostly just stream the contents (up to keeping a few pages around in our
	// page cache).
	request := ds3models.NewGetObjectRequest(rac.bucket, rac.key).
		WithRanges(ds3models.Range{startpos, endpos - 1})
	output, err := rac.client.GetObject(request)
	if err != nil {
		log.Println("BlackPearl loadpage:", rac, offset, err)
		// what kind of errors might we get?
		return bpPage{}, err
	}
	data := &bytes.Buffer{} // using Buffer since we need an io.Writer interface
	n, err := io.Copy(data, output.Content)
	output.Content.Close()
	if n == 0 && err == nil {
		// nothing was transferred and there was no error...?
		err = io.EOF
	}
	return bpPage{data: data.Bytes(), offset: startpos}, err
}

// Close will close this file.
func (rac *bpReadAtCloser) Close() error {
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
		chunks, err := wc.waitForBlackPearl(jobID)
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
func (wc *bpWriteCloser) waitForBlackPearl(jobID string) ([]chunk, error) {
	// wait until BP is ready for an upload
	for {
		input := ds3models.NewGetJobChunksReadyForClientProcessingSpectraS3Request(jobID).
			WithPreferredNumberOfChunks(1)
		resp, err := wc.client.GetJobChunksReadyForClientProcessingSpectraS3(input)
		if err != nil {
			// TODO figure out what to do
			return nil, err
		}

		// Can any chunks be processed?
		numberOfChunks := len(resp.MasterObjectList.Objects)
		switch {
		case numberOfChunks > 1:
			log.Println(
				"BlackPearl: Expected only one chunk at a time. Received",
				numberOfChunks)
			fallthrough
		case numberOfChunks == 1:
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
		default:
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
