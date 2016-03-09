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

const BogusFileId string = ""

func (ia *itemAttributes) chunkAndUpload(srcFile string, srcFileMd5 []byte, fileChunkSize int) (string, error) {

	// check to see if metadata exists for this fileId
	// if so, get size of data already uploaded

	var fileId string = ia.item + "-" + hex.EncodeToString(srcFileMd5)
	var offset int64

	json, error := ia.getUploadMeta(fileId)

	if error != nil {
		offset = 0
	} else {
		offset, _ = json.GetInt64("Size")
	}

	sourceFile, err := os.Open(path.Join(ia.fileroot, srcFile))

	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	defer sourceFile.Close()

	_, err2 := sourceFile.Seek(offset, 0)

	if err2 != nil {
		fmt.Println(err2)
		os.Exit(1)
	}

	// Get the file size from the file status

	fileInfo, infoErr := sourceFile.Stat()

	if infoErr != nil {
		fmt.Println(infoErr)
		os.Exit(1)
	}

	fileSize := fileInfo.Size()

	chunk := make([]byte, fileChunkSize)

	start := time.Now()

	fmt.Printf("Start Upload of %s/%s at offset %d, size %d, chunkSize %d at %s\n", fileId, srcFile, offset, fileSize, fileChunkSize, start.String())

	// upload the chunk

	for {
		bytesRead, readErr := sourceFile.Read(chunk)

		if bytesRead > 0 {

			//filename := chunkFileName

			chMd5 := md5.Sum(chunk[:bytesRead])

			fileId, err = ia.PostUpload(chunk[:bytesRead], chMd5[:], srcFileMd5, fileId)

			if err != nil {
				fmt.Println(err.Error())
			}

			continue
		}

		if readErr != nil && readErr != io.EOF {
			fmt.Println(readErr.Error())
			return fileId, readErr
		}

		// byteRead =0 && err is nill or EOF
		break
	}

	end := time.Since(start)
	fmt.Printf("Finished Upload of %s/%s in %v seconds\n", ia.item, srcFile, end.Seconds())

	return path.Base(fileId), nil
}
