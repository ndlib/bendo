package bclientapi

import (
	"bytes"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"io"
	"log"
	"net/http"
	"sync"
)

// upload copies the content from the ReadSeeker to the remote server, giving it
// the temporary name of `uploadname`. It uses the provided FileInfo to do this.
// If MD5 is not provided in the FileInfo, it will be calculated before doing
// the transfer. If the file has already been uploaded or only uploaded partially,
// we will resume the transfer where it was left off.
func (c *Connection) upload(uploadname string, r io.ReadSeeker, info FileInfo) error {
	if len(info.MD5) == 0 {
		// Since no md5 sum was suppled, calculate it. Need to do this before
		// uploading the file
		hw := md5.New()
		_, err := io.Copy(hw, r)
		if err != nil {
			return err
		}
		info.MD5 = hw.Sum(nil)
	}
	if info.Size == 0 {
		// no size was provided, so figure it out (since we have Seeker)
		size, err := r.Seek(0, io.SeekEnd)
		if err != nil {
			return err
		}
		info.Size = size
	}

	// if there is an error, we assume the file just hasn't been uploaded yet
	remoteinfo, _ := c.getUploadInfo(uploadname)
	if len(remoteinfo.MD5) > 0 && !bytes.Equal(remoteinfo.MD5, info.MD5) {
		// the prior upload was for something different?
		// should delete and upload from beginning.
		// TODO(dbrower): delete and upload from beginning
		return ErrUnexpectedResp
	}
	if remoteinfo.Size == info.Size {
		// it is already uploaded
		return nil
	}
	// start upload where we left off, in case we were interrupted
	_, err := r.Seek(remoteinfo.Size, io.SeekStart)
	if err != nil {
		return err
	}

	// special case zero length files.
	if info.Size == 0 {
		emptyMD5 := []byte{
			0xd4, 0x1d, 0x8c, 0xd9, 0x8f, 0x00, 0xb2, 0x04, 0xe9, 0x80, 0x09, 0x98, 0xec, 0xf8, 0x42, 0x7e,
		}
		err = c.upload0(uploadname, nil, emptyMD5, info)
		return err
	}

	// upload the file in chunks
	var chunk []byte
	if c.chunkpool == nil {
		c.chunkpool = &sync.Pool{}
	}
	if b := c.chunkpool.Get(); b != nil {
		chunk = b.([]byte)
		if len(chunk) != c.ChunkSize {
			// the buffer we got is the wrong size. forget about it
			chunk = nil
		}
	}
	if chunk == nil {
		if c.ChunkSize == 0 {
			c.ChunkSize = 10 * (1 << 20) // default is 10 MB
		}
		chunk = make([]byte, c.ChunkSize)
	}
	defer c.chunkpool.Put(chunk)
bigloop:
	for {
		n, err := r.Read(chunk)
		if err != nil && err != io.EOF {
			return err
		}
		if n == 0 {
			// nothing more to read?
			return nil
		}

		chunkMD5 := md5.Sum(chunk[:n])

		// try to upload a chunk at most 5 times
		for i := 0; i < 5; i++ {
			err = c.upload0(uploadname, chunk[:n], chunkMD5[:], info)
			if err == nil {
				continue bigloop
			}
			// otherwise there was some kind of error. Try again.
		}
		// too many retries
		return err
	}
}

// upload0 sends a single fragment of a file to the server.
func (c *Connection) upload0(uploadname string, chunk []byte, chunkmd5sum []byte, info FileInfo) error {
	path := c.HostURL + "/upload/" + uploadname

	req, _ := http.NewRequest("POST", path, bytes.NewReader(chunk))
	req.Header.Set("X-Upload-Md5", hex.EncodeToString(chunkmd5sum))
	if info.Mimetype != "" {
		req.Header.Add("Content-Type", info.Mimetype)
	}
	if len(info.MD5) > 0 {
		req.Header.Add("X-Content-MD5", hex.EncodeToString(info.MD5))
	}
	resp, err := c.do(req)
	if err != nil {
		return err
	}

	defer resp.Body.Close()

	switch resp.StatusCode {
	case 200:
		return nil
	case 412:
		return ErrChecksumMismatch
	default:
		message := make([]byte, 512)
		resp.Body.Read(message)
		log.Printf("Received HTTP status %d for %s\n", resp.StatusCode, path)
		log.Println(string(message))
		return errors.New(string(message))
	}
}
