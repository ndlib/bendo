package main

import (
	"bufio"
	"bytes"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/ndlib/bendo/bclientapi"
)

// doUpload will upload the directory/file passed in to the given item.
func doUpload(item string, file string) int {
	root := *fileroot
	if root[len(root)-1] != '/' {
		root = root + "/"
	}

	thisItem := bclientapi.New(*server, item, *fileroot, *chunksize, *wait, *token)
	var localfiles *FileList
	var remotefiles *FileList

	fmt.Println("Scanning", path.Join(root, file))

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		localfiles, _ = LoadLocalTree(root, file)
		wg.Done()
	}()

	// While checksums are going, try to get remote tree
	fmt.Println("Looking up item", item, "on remote server")
	json, err := thisItem.GetItemInfo()
	if err == nil {
		remotefiles = New(root)
		remotefiles.BuildListFromJSON(json)
	} else if err == bclientapi.ErrNotFound {
		// not an error if item does not exist on remote server
		err = nil
	}
	// Wait for scan to finish
	wg.Wait()
	if err != nil {
		// If GetItemInfo returns other error, bendo unvavailable for upload- abort!
		fmt.Println(err)
		return 1
	}

	// This compares the local list with the remote list (if the item already exists)
	// and eliminates any unneeded duplicates
	fmt.Println("Resolving differences")
	todo := ResolveLocalBlobs(localfiles, remotefiles)

	if len(todo) == 0 {
		fmt.Printf("Nothing to do:\nThe versions of All Files given for upload in item %s\nare already present on the server\n", item)
		return 0
	}
	if *verbose {
		fmt.Println(len(todo), "update commands")
		for _, a := range todo {
			fmt.Println(a)
		}
	}
	// Upload Any blobs
	fmt.Println("Uploading files")
	err = UploadBlobs(thisItem, todo)
	if err != nil {
		fmt.Println("error:", err)
		return 1
	}

	// chunks uploaded- submit transaction to add FileIDs to item
	transaction, err := PostTransaction(item, thisItem, todo)

	if err != nil {
		fmt.Println(err)
		return 1
	}

	if *verbose {
		fmt.Printf("\n Transaction id is %s\n", transaction)
	}

	if *wait {
		err = thisItem.WaitForCommitFinish(transaction)
		if err != nil {
			fmt.Println(err)
			return 1
		}
	}

	return 0
}

func LoadLocalTree(root string, start string) (*FileList, error) {
	// Since the pipeline does a fan-in, we need one wait group to
	// wait for everything in the fan, and a second to wait for
	// the goroutine that puts everything into the FileList.
	var wg sync.WaitGroup
	var wgend sync.WaitGroup

	local := New(root)
	checksumchan := make(chan string)
	manifestchan := make(chan string)
	filechan := make(chan File)

	// Source
	wg.Add(1)
	go func() {
		ScanFilesystem(path.Join(root, start), checksumchan, manifestchan)
		close(checksumchan)
		close(manifestchan)
		wg.Done()
	}()

	// checksum
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() {
			ChecksumLocalFiles(root, checksumchan, filechan)
			wg.Done()
		}()
	}

	// read manifests
	wg.Add(1)
	go func() {
		for manifest := range manifestchan {
			ParseManifest(root, manifest, filechan)
		}
		wg.Done()
	}()

	// Sink
	wgend.Add(1)
	go func() { local.AddFiles(filechan); wgend.Done() }()

	// need to do this two-step since we cannot close filechan until
	// everything using it has stopped. Then once we close it we need to
	// wait for the sink to quit.
	wg.Wait()
	close(filechan)
	wgend.Wait()
	return local, nil
}

// ScanFilesystem will start at the directory (or file) `file`, treating
// the path in `root` as the initial segment to strip.
// Directories beginning with a dot are discarded. Otherwise
// the file names (with the prefix `root` removed) are sent out `c` and
// directories are recursed into.
func ScanFilesystem(startpath string, c chan<- string, manifests chan<- string) {
	filepath.Walk(startpath, func(abspath string, info os.FileInfo, err error) error {
		if err != nil {
			fmt.Println(err)
			return err
		}
		filename := path.Base(abspath)

		// skip files and directories beginning with a dot
		if strings.HasPrefix(filename, ".") {
			if info.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		if info.IsDir() {
			// recurse into directories, but don't send them down the channel
			return nil
		}
		if filename == "bclient-manifest" {
			manifests <- abspath
		} else {
			c <- abspath
		}
		return nil
	})
}

// Checksum local files
func ChecksumLocalFiles(root string, in <-chan string, out chan<- File) {
	md5w := md5.New()

	for abspath := range in {
		// Open the local file
		r, err := os.Open(abspath)
		if err != nil {
			fmt.Println(err)
			continue
		}

		md5w.Reset()
		// Copy from the Reader into the Writer (this will compute the CheckSums)
		io.Copy(md5w, r)
		r.Close()

		// Get the Checksums
		md5Sum := md5w.Sum(nil)

		relname := strings.TrimPrefix(abspath, root)
		out <- File{
			Name:    relname,
			AbsPath: abspath,
			MD5:     md5Sum[:],
		}
	}
}

// ParseManifest reads the file `manifest`, and sends the contents down the out
// channel. The root is needed to form the relativized filenames.
func ParseManifest(root string, manifest string, out chan<- File) error {
	dir, _ := filepath.Split(manifest)
	r, err := os.Open(manifest)
	if err != nil {
		return err
	}
	defer r.Close()
	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		// each checksum line should look like
		// filename|md5|sha256|mimetype
		line := scanner.Text()
		if len(line) == 0 || line[0] == '#' {
			continue
		}
		pieces := strings.Split(line, "|")
		if len(pieces) != 4 {
			continue
		}
		md5, _ := hex.DecodeString(pieces[1])
		sha256, _ := hex.DecodeString(pieces[2])
		abspath := path.Join(dir, pieces[0])
		// TODO?(dbrower): make sure file exists?
		relname := strings.TrimPrefix(abspath, root)
		out <- File{
			Name:     relname,
			MD5:      md5,
			SHA256:   sha256,
			MimeType: pieces[3],
		}
	}
	return scanner.Err()
}

type ActionKind int

const (
	AUnknown ActionKind = iota
	ANewBlob
	AUpdateMimeType
	AUpdateFile
)

type Action struct {
	What ActionKind
	// the exact fields used depends on What.
	Source   string // absolute path of file to upload
	MD5      []byte // checksum of Source
	MimeType string // new mime type
	BlobID   int64  // for blobs already on server
	Name     string // the name for the file on server
}

func (a Action) String() string {
	switch a.What {
	case ANewBlob:
		return fmt.Sprintf("<Action ANewBlob, Source=%s, MimeType=%s>",
			a.Source,
			a.MimeType)
	case AUpdateMimeType:
		return fmt.Sprintf("<Action AUpdateMimeType, Blob=%d, MimeType=%s>",
			a.BlobID,
			a.MimeType)
	case AUpdateFile:
		return fmt.Sprintf("<Action AUpdateFile, Name=%s, Blob=%d, MD5=%x>",
			a.Name,
			a.BlobID,
			a.MD5)
	}
	return fmt.Sprintf("<Action AUnknown>")
}

// ResolveLocalBlobs compares the local to the remote file lists and returns
// a list of actions to do to update the remote tree.
func ResolveLocalBlobs(local, remote *FileList) []Action {
	var todo []Action

	for localfile, localinfo := range local.Files {
		hexMD5 := hex.EncodeToString(localinfo.MD5)
		if remote != nil {
			// is this blob on remote server?
			remoteinfo := remote.Files[localfile]
			if len(remoteinfo.MD5) != 0 &&
				bytes.Equal(localinfo.MD5, remoteinfo.MD5) {
				// this file's contents have not changed.
				// See if mime type needs to be updated.
				if localinfo.MimeType != "" &&
					localinfo.MimeType != remoteinfo.MimeType {
					todo = append(todo, Action{
						What:     AUpdateMimeType,
						BlobID:   remoteinfo.BlobID,
						MimeType: localinfo.MimeType,
					})
				}
				continue
			}

			// are there any other matching blobs on the server?
			id := remote.Blobs[hexMD5]
			if id > 0 {
				todo = append(todo, Action{
					What:   AUpdateFile,
					Name:   localfile,
					BlobID: id,
				})
				// TODO(dbrower): check mime-type and also update that, if needed
				continue
			}
		}
		// is there another local file with the same content?
		id := local.Blobs[hexMD5]
		if id == 0 {
			// there is not an existing local blob, so upload this file
			local.Blobs[hexMD5] = -1 // mark this blob as known
			todo = append(todo, Action{
				What:     ANewBlob,
				Source:   localinfo.AbsPath,
				MD5:      localinfo.MD5,
				MimeType: localinfo.MimeType,
			})
		}
		// TODO(dbrower): if there is a matching blob, see if it needs a mime type
		// now update this file entry to point to the uploaded blob
		todo = append(todo, Action{
			What: AUpdateFile,
			MD5:  localinfo.MD5,
			Name: localfile,
		})
	}

	return todo
}

// UploadBlobs will go through a FileList and send any new blobs to the server
// given by ItemAttributes. The first error is returned.
func UploadBlobs(ia *bclientapi.ItemAttributes, todo []Action) error {
	var wg sync.WaitGroup

	c := make(chan Action)
	errorchan := make(chan error, 1)

	//Spin off desired number of upload workers
	for i := 0; i < *numuploaders; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for t := range c {
				if *verbose {
					fmt.Println("Uploading", t.Source)
				}
				err := ia.UploadFile(t.Source, t.MD5, t.MimeType)
				if err != nil {
					select {
					case errorchan <- err:
					default:
					}
				}
			}
		}()
	}

	var err error
loop:
	for _, t := range todo {
		if t.What != ANewBlob {
			continue
		}
		select {
		case err = <-errorchan:
			break loop
		case c <- t:
		}
	}
	close(c)

	// wait for all file chunks to be uploaded
	wg.Wait()

	// this will either get another error or pass back the one from earlier
	// either way, if there is >=1 error, an error will be returned
	select {
	case err = <-errorchan:
	default:
	}
	return err
}

func PostTransaction(item string, ia *bclientapi.ItemAttributes, todo []Action) (string, error) {
	cmdlist := MakeTransactionCommands(item, todo)
	buf, _ := json.Marshal(cmdlist)
	return ia.CreateTransaction(buf)
}

// MakeTransactionCommands turns an Action list into a list of transaction
// commands to send to the bendo server.
func MakeTransactionCommands(item string, todo []Action) [][]string {
	var cmdlist [][]string

	for _, t := range todo {
		switch t.What {
		case ANewBlob:
			fileID := item + "-" + hex.EncodeToString(t.MD5)
			cmdlist = append(cmdlist, []string{"add", fileID})
		case AUpdateMimeType:
			id := strconv.FormatInt(t.BlobID, 10)
			cmdlist = append(cmdlist, []string{"mimetype", id, t.MimeType})
		case AUpdateFile:
			var fileID string
			// are we using a remote blob or a newly uploaded one?
			if t.BlobID > 0 {
				fileID = strconv.FormatInt(t.BlobID, 10)
			} else {
				fileID = item + "-" + hex.EncodeToString(t.MD5)
			}
			cmdlist = append(cmdlist, []string{"slot", t.Name, fileID})
		}
	}
	return cmdlist
}
