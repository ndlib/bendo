package store

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	raven "github.com/getsentry/raven-go"
)

// A S3 store represents a store that is kept on AWS S3 storage.
// Do not change Bucket or Prefix concurrently with calls using the structure.
type S3 struct {
	svc    *s3.S3
	Bucket string
	Prefix string
	sizes  *sizecache // keep HEAD info
}

// NewS3 creates a new S3 store. It will use the given bucket and will prepend
// prefix to all keys. This is to allow for a bucket to be used for more than
// one store. For example if prefix were "cache/" then an Open("hello") would
// look for the key "cache/hello" in the bucket. The authorization method and
// credentials in the session are used for all accesses.
func NewS3(bucket, prefix string, awsSession *session.Session) *S3 {
	return &S3{
		Bucket: bucket,
		Prefix: prefix,
		svc:    s3.New(awsSession),
		sizes:  newSizeCache(),
	}
}

// List returns a list of all the keys in this store. It will only return ones
// that satisfy the store's Prefix, so it is safe to use this on a bucket
// containing other items.
func (s *S3) List() <-chan string {
	out := make(chan string)
	go func() {
		defer close(out)
		input := &s3.ListObjectsV2Input{
			Bucket: aws.String(s.Bucket),
			Prefix: aws.String(s.Prefix),
		}
		err := s.svc.ListObjectsV2Pages(input,
			func(page *s3.ListObjectsV2Output, lastpage bool) bool {
				for _, item := range page.Contents {
					out <- strings.TrimPrefix(*item.Key, s.Prefix)
				}
				return !lastpage
			})
		if err != nil {
			log.Println("S3 List:", s.Prefix, err)
			raven.CaptureError(err, map[string]string{"Bucket": s.Bucket, "Prefix": s.Prefix})
		}
	}()
	return out
}

// ListPrefix returns the keys in this store that have the given prefix.
// The argument prefix is added to the store's Prefix.
func (s *S3) ListPrefix(prefix string) ([]string, error) {
	var result []string
	input := &s3.ListObjectsV2Input{
		Bucket: aws.String(s.Bucket),
		Prefix: aws.String(s.Prefix + prefix),
	}
	err := s.svc.ListObjectsV2Pages(input,
		func(page *s3.ListObjectsV2Output, lastpage bool) bool {
			for _, item := range page.Contents {
				result = append(result, strings.TrimPrefix(*item.Key, s.Prefix))
			}
			return !lastpage
		})
	if err != nil {
		log.Println("S3 ListPrefix:", s.Prefix, prefix, err)
		raven.CaptureError(err, map[string]string{"Bucket": s.Bucket, "Prefix": s.Prefix, "Pattern": prefix})
	}
	return result, err
}

// Open will return a ReadAtCloser to get the content for the given key. Data
// is paged in from S3 as needed, and up to 50 MB or so is cached at a time.
func (s *S3) Open(key string) (ReadAtCloser, int64, error) {
	// check that the key exists, and if so get its size
	size, err := s.stat(key)
	if err != nil {
		return nil, 0, err
	}
	result := &s3ReadAtCloser{
		svc:    s.svc,
		bucket: s.Bucket,
		key:    s.Prefix + key,
		size:   size,
	}
	return result, size, nil
}

// Create will return a WriteCloser to upload content to the given key. Data is
// batched and uploaded to S3 using the Multipart interface. The part sizes
// increase, so objects up to the 5 TB limit S3 imposes is theoretically
// possible.
func (s *S3) Create(key string) (io.WriteCloser, error) {
	_, err := s.stat(key)
	if err == nil {
		return nil, ErrKeyExists
	}
	s.sizes.Set(key, 0) // make 0 in case this key was previously deleted
	fullkey := s.Prefix + key
	return &s3WriteCloser{
		svc:    s.svc,
		bucket: s.Bucket,
		key:    fullkey,
	}, nil
}

// Delete will remove the given key from the store. The store's Prefix is
// prepended first. It is not an error to delete something that doesn't exist.
func (s *S3) Delete(key string) error {
	_, err := s.svc.DeleteObject(&s3.DeleteObjectInput{
		Bucket: aws.String(s.Bucket),
		Key:    aws.String(s.Prefix + key),
	})
	if err != nil {
		log.Println("S3 Delete:", s.Prefix, key, err)
		raven.CaptureError(err, map[string]string{"Bucket": s.Bucket, "Prefix": s.Prefix, "Key": key})
	} else {
		s.sizes.Set(key, sizeDeleted)
	}
	return err
}

// stat will check if a key exists, and if so it returns the size. If the item
// does not exist an error is returned. The prefix is added to the key before
// checking.
func (s *S3) stat(key string) (int64, error) {
	// Cache the key sizes as we see them. This drastically cuts down on the
	// number of HEAD requests.
	return s.sizes.Get(key, s.stat0)
}

// stat0 implements the actual HEAD request to s3. Returns either an error
// or the size. You probably want to call stat().
func (s *S3) stat0(key string) (int64, error) {
	input := &s3.HeadObjectInput{
		Bucket: aws.String(s.Bucket),
		Key:    aws.String(s.Prefix + key),
	}
	info, err := s.svc.HeadObject(input)
	if err != nil {
		return 0, err
	}
	return *info.ContentLength, nil
}

// s3ReadAtCloser adapts the Reader we get for loading content via s3
// to the ReadAt interface. It keeps a LRU cache of pages from s3.
//
// It does not know the size of the file being downloaded, and tries to
// estimate it from noticing incomplete ranges being returned or from invalid
// range error responses.
//
// The pages can start at any offset, and it is possible pages in memory may
// overlap. Though, in the expected case of a sequential read through the file,
// the pages will be disjoint.
//
// It is not safe to use access this from more than one goroutine.
type s3ReadAtCloser struct {
	svc    *s3.S3
	bucket string
	key    string
	pages  []s3Page // cache of data we've downloaded
	size   int64
}

type s3Page struct {
	data   []byte
	offset int64
}

// ReadAt implements the io.ReadAt interface.
func (rac *s3ReadAtCloser) ReadAt(p []byte, offset int64) (int, error) {
	//todo: does readat() return EOF?
	var err error
	startOffset := offset
	for len(p) > 0 {
		if offset >= rac.size {
			break
		}
		var page s3Page
		page, err = rac.getpage(offset)
		if err != nil {
			// don't return, in case we have already copied some data in
			// a previous loop.
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

// The number of pages we keep in the cache. After this we will evict the LRU.
const defaultNumPages = 5

// getpage will find in memory or load a page for the given offset
func (rac *s3ReadAtCloser) getpage(offset int64) (s3Page, error) {
	i := rac.findpage(offset)
	if i == -1 {
		// page was not found, try to get it
		page, err := rac.loadpage(offset)
		if err != nil {
			return s3Page{}, err
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
func (rac *s3ReadAtCloser) findpage(offset int64) int {
	for i, page := range rac.pages {
		base := page.offset
		limit := base + int64(len(page.data))
		if base <= offset && offset < limit {
			return i
		}
	}
	return -1
}

const defaultPageSize = 10 * 1024 * 1024 // 10 MiB

// loadpage will read one page of data from S3. It tries to read defaultPageSize
// bytes, but less may be returned, e.g. at the end of the file. Hence pages
// may be of various sizes. It also choses a starting offset that is a multiple
// of defaultPageSize, so all pages in memory are disjoint.
func (rac *s3ReadAtCloser) loadpage(offset int64) (s3Page, error) {
	// take the page start to be the greatest multiple of defaultPageSize less
	// than the given offset
	startpos := (offset / defaultPageSize) * defaultPageSize
	endpos := startpos + defaultPageSize
	input := &s3.GetObjectInput{
		Bucket: aws.String(rac.bucket),
		Key:    aws.String(rac.key),
		Range:  aws.String(fmt.Sprintf("bytes=%d-%d", startpos, endpos-1)),
	}
	output, err := rac.svc.GetObject(input)
	if err != nil {
		log.Println("S3 loadpage:", rac, offset, err)
		// if we get an invalid range error then we have gone too far
		e, ok := err.(awserr.RequestFailure)
		if ok && e.StatusCode() == http.StatusRequestedRangeNotSatisfiable {
			err = io.EOF
		}
		return s3Page{}, err
	}
	data := &bytes.Buffer{} // using Buffer since we need an io.Writer interface
	n, err := io.Copy(data, output.Body)
	output.Body.Close()
	// TODO(dbrower): should there be a retry for transmission errors?
	if n == 0 && err == nil {
		// nothing was transferred and there was no error...?
		err = io.EOF
	}
	return s3Page{data: data.Bytes(), offset: startpos}, err
}

// Close will close this file.
func (rac *s3ReadAtCloser) Close() error {
	return nil
}

// s3WriteCloser does an upload to s3. If the entire file fits into one buffer
// it will do a single PUT. Otherwise it will use the s3 multipart upload
// interface.
//
// A challenge is that we do not know the ultimate size of the object while we
// are writing it. To accommodate large file sizes, we vary the size of each
// part. Varying the part sizes lets us use small parts for small files, but
// still be able to handle large files, e.g. larger than 50 GB (which would be
// the max if we used a constant part size of 5 MB).
//
// AWS restricts part sizes to be between 5 MB and 5 GB.
//
// We set the upload threshold of part i to size(i) = min(a*2^i, b) where
// constants a and b are a = 64 * 1024 * 1024 (64 MB) and
// b = 4 * 1024 * 1024 * 1024 (4 GB)
//
//      File Size       # Parts (using this system)
//      ---------       -------
//           1 GB             5
//          10 GB             8
//         100 GB            36
//        1000 GB           301
type s3WriteCloser struct {
	svc      *s3.S3
	bucket   string
	key      string
	buf      *bytes.Buffer // current buffer we are writing to
	isMulti  bool          // true if this is a multipart upload
	uploadID string        // the multipart id that s3 gave us
	part     int           // the part number we are currently filling up (0-based. n.b. AWS is 1-based)
	etags    []string      // list of etags for all our uploaded parts, index i == etag for part i
	abort    bool          // true to abort upload at close
}

// These are constants, but beware! The relationship that
// wcBaseSize << 6 == wcMaxSize is baked into the code below
const (
	wcBaseSize = 64 * 1024 * 1024
	wcMaxSize  = 4 * 1024 * 1024 * 1024
)

var (
	// wcBufferPool contains spare buffers to use for uploading. It is shared
	// between all the s3WriteCloser instances.
	wcBufferPool sync.Pool

	ErrNoETag   = errors.New("No ETag was returned from AWS")
	ErrNotExist = errors.New("Key does not exist")
)

func (wc *s3WriteCloser) Write(p []byte) (int, error) {
	// lazily initialize stuff
	if wc.buf == nil {
		wc.buf = wc.getbuf()
	}
	n, err := wc.buf.Write(p)
	if n == 0 && err != nil {
		wc.abort = true
		return n, err
	}
	// see if we need to upload this buffer
	lowerlimit := wcMaxSize
	if wc.part < 6 {
		lowerlimit = wcBaseSize << wc.part
	}
	if wc.buf.Len() > lowerlimit {
		err = wc.uploadpart(wc.part, wc.buf)
		wc.buf.Reset() // clear the buffer
		if err != nil {
			wc.abort = true
			return 0, err
		}
		wc.part++
	}
	return n, nil
}

// Close will flush any temporary buffers to S3, and then wait for everything
// to be uploaded. If there were any errors (either now, or while calling
// Write()), the entire upload will be deleted. Otherwise it will be saved
// into S3.
func (wc *s3WriteCloser) Close() error {
	if wc.buf != nil {
		defer func() {
			// we're done with the buffer, so return it for someone else
			wcBufferPool.Put(wc.buf)
			wc.buf = nil
		}()
	}

	// if we haven't started a multipart transaction yet, just send what is in
	// the buffer
	if !wc.isMulti {
		if wc.abort {
			return nil
		}
		return wc.uploadfull(wc.buf)
	}

	// should this multipart transaction be abandoned?
	var err error
abort:
	if wc.abort {
		_, err2 := wc.svc.AbortMultipartUpload(&s3.AbortMultipartUploadInput{
			Bucket:   aws.String(wc.bucket),
			Key:      aws.String(wc.key),
			UploadId: aws.String(wc.uploadID),
		})
		if err2 != nil {
			log.Println("S3 Abort Close:", wc, err2)
		}
		// if there was not a previous error, send whatever this one is
		if err == nil {
			err = err2
		}
		return err
	}

	// upload anything left in the buffer
	if wc.buf.Len() > 0 {
		err = wc.uploadpart(wc.part, wc.buf)
		if err != nil {
			wc.abort = true
			goto abort
		}
	}
	err = wc.finishMultipart()
	if err != nil {
		// this message is redundant.
		log.Println("S3 Complete Close:", wc, err)
	}
	return err
}

func (wc *s3WriteCloser) getbuf() *bytes.Buffer {
	b, ok := wcBufferPool.Get().(*bytes.Buffer)
	if !ok {
		b = &bytes.Buffer{}
		b.Grow(2 * wcBaseSize) // guess a beginning capacity
	}
	b.Reset()
	return b
}

func (wc *s3WriteCloser) startMultipart() error {
	if wc.isMulti {
		// already started one??
		return nil
	}
	result, err := wc.svc.CreateMultipartUpload(&s3.CreateMultipartUploadInput{
		Bucket: aws.String(wc.bucket),
		Key:    aws.String(wc.key),
	})
	if err != nil {
		log.Println("S3 startMultipart:", wc.key, err)
		raven.CaptureError(err, map[string]string{"Bucket": wc.bucket, "Key": wc.key})
		return err
	}
	wc.isMulti = true
	wc.uploadID = *result.UploadId
	return nil
}

func (wc *s3WriteCloser) finishMultipart() error {
	// need to upload all the part number/etag pairs
	var completed []*s3.CompletedPart
	for i, etag := range wc.etags {
		completed = append(completed, &s3.CompletedPart{
			ETag:       aws.String(etag),
			PartNumber: aws.Int64(int64(i + 1)), // part numbers are 1-based
		})
	}
	_, err := wc.svc.CompleteMultipartUpload(
		&s3.CompleteMultipartUploadInput{
			Bucket:   aws.String(wc.bucket),
			Key:      aws.String(wc.key),
			UploadId: aws.String(wc.uploadID),
			MultipartUpload: &s3.CompletedMultipartUpload{
				Parts: completed,
			},
		})
	return err
}

func (wc *s3WriteCloser) uploadpart(partno int, buf *bytes.Buffer) error {
	if !wc.isMulti {
		wc.startMultipart()
	}
	//log.Println("s3: uploading", wc.key, partno, buf.Len())
	input := &s3.UploadPartInput{
		Body:       bytes.NewReader(buf.Bytes()), // need Seek()
		Bucket:     aws.String(wc.bucket),
		Key:        aws.String(wc.key),
		PartNumber: aws.Int64(int64(partno + 1)), // parts are 1-based in AWS
		UploadId:   aws.String(wc.uploadID),
	}
	output, err := wc.svc.UploadPart(input)
	// can we detect and retry in event of transient errors?
	if err != nil {
		log.Println("S3 uploadpart:", wc, partno+1, err)
		return err
	}
	if output.ETag == nil {
		log.Println("S3 nil ETag for part", partno, "key=", wc.key)
		return ErrNoETag
	}
	wc.etags = append(wc.etags, *output.ETag)
	return nil
}

func (wc *s3WriteCloser) uploadfull(buf *bytes.Buffer) error {
	// it is possible to get here with buf == nil. This happens when we are
	// closed without any calls to Write()
	source := &bytes.Reader{} // need Seek(), and bytes.Buffer doesn't have it
	if buf != nil {
		source.Reset(buf.Bytes())
	}
	input := &s3.PutObjectInput{
		Body:          source,
		Bucket:        aws.String(wc.bucket),
		Key:           aws.String(wc.key),
		ContentLength: aws.Int64(int64(source.Len())),
	}
	_, err := wc.svc.PutObject(input)
	// can we detect and retry in event of transient errors?
	if err != nil {
		log.Println("S3 uploadfull:", wc, err)
	}
	return err
}
