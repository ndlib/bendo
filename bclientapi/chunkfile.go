package bclientapi

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path"
	"time"
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

	// Get the file size from the file status
	fileInfo, err := sourceFile.Stat()
	if err != nil {
		return "", err
	}

	fileSize := fileInfo.Size()
	chunk := make([]byte, ia.chunkSize)
	start := time.Now()

	fmt.Printf("Start Upload of %s/%s at offset %d, size %d, at %s\n", fileID, srcFile, offset, fileSize, start.String())

	// upload the chunk
	for {
		bytesRead, readErr := sourceFile.Read(chunk)

		if bytesRead > 0 {
			chMd5 := md5.Sum(chunk[:bytesRead])

			fileID, err = ia.PostUpload(chunk[:bytesRead], chMd5[:], srcFileMd5, mimetype, fileID)
			if err != nil {
				fmt.Println(err)
			}
			continue
		}

		if readErr != nil && readErr != io.EOF {
			return fileID, readErr
		}

		// byteRead =0 && err is nill or EOF
		break
	}

	end := time.Since(start)
	fmt.Printf("Finished Upload of %s/%s in %v seconds\n", ia.item, srcFile, end.Seconds())

	return path.Base(fileID), nil
}
