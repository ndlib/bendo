package bclientapi

import (
	"crypto/md5"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path"
)

func (c *Connection) chunkAndUpload(item string, srcFile string, srcFileMd5 []byte, mimetype string) (string, error) {
	var fileID = item + "-" + hex.EncodeToString(srcFileMd5)

	// check to see if metadata exists for this fileID
	// if so, get size of data already uploaded
	var offset int64
	json, err := c.getUploadMeta(fileID)
	if err == nil {
		offset, _ = json.GetInt64("Size")
	}

	sourceFile, err := os.Open(srcFile)
	if err != nil {
		return fileID, err
	}
	defer sourceFile.Close()

	// start upload where we left off, in case we were interrupted
	_, err = sourceFile.Seek(offset, io.SeekStart)
	if err != nil {
		return "", err
	}

	chunk := make([]byte, c.ChunkSize)

	// upload the file in chunks

	// we need to special case zero-length files to force an empty body to be
	// sent. How do we know if a file is zero length? We don't. We make sure to
	// send a (possibly empty) chunk the first time through the loop, with an
	// optimization that files we are resuming are not empty.
	needSendEmptyChunk := offset == 0
	for {
		var n int
		n, err = sourceFile.Read(chunk)

		if err != nil && err != io.EOF {
			break
		}
		err = nil // if there was an error, it was io.EOF
		// need flag so loop will exit the second time through
		if n == 0 && !needSendEmptyChunk {
			break
		}
		needSendEmptyChunk = false
		chMd5 := md5.Sum(chunk[:n])
		// try to upload a chunk at most 5 times
		var retryCount = 0
	retry:
		if retryCount >= 5 {
			err = errors.New("Too many attempts to upload chunk")
			break
		}
		err = c.PostUpload(chunk[:n], chMd5[:], srcFileMd5, mimetype, fileID)
		if err == ErrChecksumMismatch || err == ErrServerError {
			retryCount++
			goto retry
		}
		if err != nil {
			break
		}
	}

	return path.Base(fileID), err
}
