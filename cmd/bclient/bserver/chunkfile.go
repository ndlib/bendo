package bserver

import (
	"crypto/md5"
	"fmt"
	"io"
	"os"
	"path"
)

const BogusFileId string = "Uninitialized"

func chunkAndUpload(fileroot string, srcFile string, srcFileMd5 []byte, item string, fileChunkSize int) (string, error) {

	sourceFile, err := os.Open(path.Join(fileroot, srcFile))

	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	defer sourceFile.Close()

	chunk := make([]byte, fileChunkSize)

	var fileId string = BogusFileId

	// upload the chunk

	for {
		bytesRead, readErr := sourceFile.Read(chunk)

		if bytesRead > 0 {

			//filename := chunkFileName

			chMd5 := md5.Sum(chunk[:bytesRead])

			fileId, err = PostUpload(chunk[:bytesRead], chMd5[:], srcFileMd5, fileId)

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

	return path.Base(fileId), nil
}
