package bserver

import (
	"crypto/md5"
	"fmt"
	"os"
)

const BogusFileId string = "Uninitialized"

func chunkAndUpload(srcFile string, srcFileMd5 []byte, item string, fileChunkSize int) error {

	sourceFile, err := os.Open(srcFile)

	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	defer sourceFile.Close()

	chunk := make([]byte, fileChunkSize)

	var fileId string = BogusFileId

	for chunkNo := int64(1); chunkNo > 0; chunkNo++ {
		bytesRead, readErr := sourceFile.Read(chunk)

		if bytesRead > 0 {

			//filename := chunkFileName

			chMd5 := md5.Sum(chunk)

			fileId, err = PostUpload(chunk, chMd5[:], srcFileMd5, fileId)

			if err != nil {
				fmt.Println(err.Error())
			}
		}

		if readErr != nil {
			break
		}
	}

	return nil
}
