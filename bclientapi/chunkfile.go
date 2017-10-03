package bclientapi

import (
	"crypto/md5"
	"encoding/hex"
	"io"
	"os"
	"path"
)

func (ia *ItemAttributes) chunkAndUpload(srcFile string, srcFileMd5 []byte, mimetype string) (string, error) {
	var fileID = ia.item + "-" + hex.EncodeToString(srcFileMd5)

	// check to see if metadata exists for this fileID
	// if so, get size of data already uploaded
	var offset int64
	json, err := ia.getUploadMeta(fileID)
	if err == nil {
		offset, _ = json.GetInt64("Size")
	}

	sourceFile, err := os.Open(srcFile)
	if err != nil {
		return fileID, err
	}
	defer sourceFile.Close()

	_, err = sourceFile.Seek(offset, io.SeekStart)
	if err != nil {
		return "", err
	}

	chunk := make([]byte, ia.chunkSize)

	// upload the chunk
	for {
		var n int
		n, err = sourceFile.Read(chunk)

		if err != nil && err != io.EOF {
			break
		}
		err = nil // if there was an error, it was io.EOF
		if n == 0 {
			break
		}
		chMd5 := md5.Sum(chunk[:n])
		fileID, err = ia.PostUpload(chunk[:n], chMd5[:], srcFileMd5, mimetype, fileID)
		if err != nil {
			break
		}
	}

	return path.Base(fileID), err
}
