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
}

// NewS3 creates a new S3 store. It will use the given bucket and will prepend
// prefix to all keys. This is to allow for a bucket to be used for more than
// one store. For example if prefix were "cache/" then an Open("hello") would
// look for the key "cache/hello" in the bucket. The authorization method and
// credentials in the session are used for all accesses.
func NewS3(bucket, prefix string, s *session.Session) *S3 {
	return &S3{
		Bucket: bucket,
		Prefix: prefix,
		svc:    s3.New(s),
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
			log.Println("List:", err)
			raven.CaptureError(err, nil)
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
	fullkey := s.Prefix + key
	result, err := s.svc.CreateMultipartUpload(&s3.CreateMultipartUploadInput{
		Bucket: aws.String(s.Bucket),
		Key:    aws.String(fullkey),
	})
	if err != nil {
		return nil, err
	}
	return &s3WriteCloser{
		svc:      s.svc,
		bucket:   s.Bucket,
		key:      fullkey,
		uploadID: *result.UploadId,
	}, nil
}

// Delete will remove the given key from the store. The store's Prefix is
// prepended first. It is not an error to delete something that doesn't exist.
func (s *S3) Delete(key string) error {
	_, err := s.svc.DeleteObject(&s3.DeleteObjectInput{
		Bucket: aws.String(s.Bucket),
		Key:    aws.String(s.Prefix + key),
	})
	return err
}

// stat will check if a key exists, and if so it returns the size. If the item
// does not exist an error is returned. The prefix is added to the key before
// checking.
func (s *S3) stat(key string) (int64, error) {
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
	size   int64    // 0 == unknown size, otherwise will be >= actual size
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
		if rac.size > 0 && offset >= rac.size {
			break
		}
		var page s3Page
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

// The number of pages we keep in the cache. After this we will evict the LRU.
const defaultNumPages = 5

// getpage will find or load a page for the given offset
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
// may be of various sizes.
func (rac *s3ReadAtCloser) loadpage(offset int64) (s3Page, error) {
	endpos := offset + defaultPageSize
	input := &s3.GetObjectInput{
		Bucket: aws.String(rac.bucket),
		Key:    aws.String(rac.key),
		Range:  aws.String(fmt.Sprintf("bytes=%d-%d", offset, endpos-1)),
	}
	output, err := rac.svc.GetObject(input)
	if err != nil {
		// if we get an invalid range error then we have gone too far
		e, ok := err.(awserr.RequestFailure)
		if ok && e.StatusCode() == http.StatusRequestedRangeNotSatisfiable {
			err = io.EOF
			// can we upper bound the size?
			if rac.size == 0 || rac.size > offset {
				rac.size = offset
			}
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
	// Try to bound the file size from above by assuming a partial range
	// returned means we hit the end. (this may be a terrible assumption).
	if rac.size == 0 && *output.ContentLength < defaultPageSize {
		rac.size = offset + *output.ContentLength
	}
	return s3Page{data: data.Bytes(), offset: offset}, err
}

// Close will close this file.
func (rac *s3ReadAtCloser) Close() error {
	return nil
}

// s3WriteCloser adapts the s3 multipart upload interface to the WriteCloser
// interface returned by Create().
//
// A challenge is that we do not know the ultimate size of the object while we
// are writing it. To accommodate large file sizes, we vary the size of each
// part. Varying the part sizes lets us use small parts for small files, but
// still be able to handle files larger than 50 GB (which would be the max if
// we used a constant part size of 5 MB).
//
// The size of part i in bytes is bounded below by the function size(i) = b + a*i.
// We are using a = 128 * 1024 and b = 5 * 1024 * 1024
// The maximum upload file size for these values of a and b is ~6.6 TB.
type s3WriteCloser struct {
	svc      *s3.S3
	bucket   string
	key      string
	uploadID string
	buf      *bytes.Buffer // current buffer we are writing to
	part     int           // the part number we are currently filling up (0-based. n.b. AWS is 1-based)
	etags    []string      // list of etags for all our uploaded parts, index i == etag for part i
	abort    bool          // true to abort upload at close
}

const (
	wcBaseSize = 5 * 1024 * 1024
	wcIncSize  = 128 * 1024
)

var (
	// wcBufferPool contains spare buffers to use for uploading. It is shared
	// between all the s3WriteCloser instances.
	wcBufferPool sync.Pool

	ErrNoETag = errors.New("No ETag was returned from AWS")
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
	lowerlimit := wcBaseSize + wcIncSize*wc.part
	if wc.buf.Len() >= lowerlimit {
		err = wc.uploadpart(wc.part, wc.buf)
		if err != nil {
			// do we need to do anything with the buffer content?
			// it will have (probably) been read out of buf, so...can we do anything at this point?
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
	// if wc.buf == nil then nothing was written
	// TODO(dbrower): don't bother if wc.err != nil

	// keep err here so if there is one, we can send it after aborting the upload on S3
	var err error
	if wc.buf != nil {
		// upload anything left in the buffer
		if wc.buf.Len() > 0 {
			err = wc.uploadpart(wc.part, wc.buf)
			if err != nil {
				wc.abort = true
			}
			wc.part++
		}
		// we're done with the buffer, so return it for someone else
		wcBufferPool.Put(wc.buf)
		wc.buf = nil
	}
	// if there was an upload error, remove the item
	if wc.abort {
		_, err2 := wc.svc.AbortMultipartUpload(&s3.AbortMultipartUploadInput{
			Bucket:   aws.String(wc.bucket),
			Key:      aws.String(wc.key),
			UploadId: aws.String(wc.uploadID),
		})
		// if there was not a previous error, send whatever this one is
		if err == nil {
			err = err2
		} else if err2 != nil {
			// both err and err2 are set. log err2 so it isn't lost
			log.Println("s3:", wc.bucket, wc.key, err2)
		}
		return err
	}
	// need to upload all the part number/etag pairs
	var completed []*s3.CompletedPart
	for i, etag := range wc.etags {
		completed = append(completed, &s3.CompletedPart{
			ETag:       aws.String(etag),
			PartNumber: aws.Int64(int64(i + 1)), // part numbers are 1-based
		})
	}
	_, err = wc.svc.CompleteMultipartUpload(
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

func (wc *s3WriteCloser) getbuf() *bytes.Buffer {
	b, ok := wcBufferPool.Get().(*bytes.Buffer)
	if !ok {
		b = &bytes.Buffer{}
		b.Grow(2 * wcBaseSize) // guess a beginning capacity
	}
	b.Reset()
	return b
}

func (wc *s3WriteCloser) uploadpart(partno int, buf *bytes.Buffer) error {
	//log.Println("s3: uploading", wc.key, partno, buf.Len())
	input := &s3.UploadPartInput{
		Body:       bytes.NewReader(buf.Bytes()),
		Bucket:     aws.String(wc.bucket),
		Key:        aws.String(wc.key),
		PartNumber: aws.Int64(int64(partno + 1)), // parts are 1-based in AWS
		UploadId:   aws.String(wc.uploadID),
	}
	output, err := wc.svc.UploadPart(input)
	buf.Reset() // mark the buffer as empty. Invalidates earlier pointer from buf.Bytes()
	// can we detect and retry in event of transient errors?
	if err != nil {
		return err
	}
	if output.ETag == nil {
		log.Println("nil ETag for part", partno, "key=", wc.key)
		return ErrNoETag
	}
	wc.etags = append(wc.etags, *output.ETag)
	return nil
}
