package server

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"html/template"
	"io"
	"math/rand"
	"net/http"
	"strconv"

	"github.com/julienschmidt/httprouter"

	"github.com/ndlib/bendo/fragment"
	"github.com/ndlib/bendo/util"
)

//{"GET", "/upload", ListFileHandler},
func ListFileHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	q := r.URL.Query()
	if len(q["label"]) > 0 {
		writeHTMLorJSON(w, r, listFileTemplate, FileStore.ListFiltered(q["label"]))
	} else {
		writeHTMLorJSON(w, r, listFileTemplate, FileStore.List())
	}
}

var (
	listFileTemplate = template.Must(template.New("listfile").Parse(`<html>
<h1>Files</h1>
<ol>
{{ range . }}
	<li><a href="/upload/{{ . }}">{{ . }}</a></li>
{{ else }}
	<li>No Files</li>
{{ end }}
</ol>
</html>`))
)

//{"GET", "/upload/:fileid/metadata", GetFileInfoHandler},
func GetFileInfoHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	id := ps.ByName("fileid")
	f := FileStore.Lookup(id)
	if f == nil {
		w.WriteHeader(404)
		fmt.Fprintln(w, "cannot find file")
		return
	}
	writeHTMLorJSON(w, r, fileInfoTemplate, f)
}

var (
	fileInfoTemplate = template.Must(template.New("fileinfo").Parse(`<html>
<h1>File Info</h1>
{{ $fileid := .ID }}
<dl>
<dt>ID</dt><dd>{{ .ID }}</dd>
<dt>Size</dt><dd>{{ .Size }}</dd>
<dt>Fragments</dt><dd>{{ .Children | len }}</dd>
<dt>Created</dt><dd>{{ .Created }}</dd>
<dt>Modified</dt><dd>{{ .Modified }}</dd>
<dt>Creator</dt><dd>{{ .Creator }}</dd>
<dt>Labels</dt><dd>{{ range .Labels }}{{ . }}<br/>{{ end }}</dd>
<dt>Payload</dt><dd>{{ .Payload }}</dd>
</dl>
<a href="/upload">Back</a>
</html>`))
)

//{"POST", "/upload", AppendFileHandler},
//{"POST", "/upload/:fileid", AppendFileHandler},
func AppendFileHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	md5hash64 := r.Header.Get("X-Upload-Md5")
	sha256hash64 := r.Header.Get("X-Upload-Sha256")
	if md5hash64 == "" && sha256hash64 == "" {
		w.WriteHeader(400)
		fmt.Fprintf(w, "Need at least one of X-Upload-Md5 or X-Upload-Sha256")
		return
	}
	var md5bytes []byte
	var err error
	if md5hash64 != "" {
		md5bytes, err = hex.DecodeString(md5hash64)
		if err != nil {
			w.WriteHeader(400)
			fmt.Fprintf(w, "bad MD5 string")
			return
		}
	}
	fileid := ps.ByName("fileid")
	var f *fragment.File // the file to append to
	// if no file was given, make a new one
	// if a file id was given, but doesn't exist...create it
	if fileid == "" {
		for f == nil {
			id := randomid()
			f = FileStore.New(id)
		}
	} else {
		// New returns nil if the file already exists!
		f = FileStore.New(fileid)
		if f == nil {
			f = FileStore.Lookup(fileid)
		}
		// f should not be nil at this point...
		if f == nil {
			w.WriteHeader(500)
			fmt.Fprintln(w, "could not make new file")
			return
		}
	}
	if r.Body == nil {
		w.WriteHeader(400)
		fmt.Fprintln(w, "no body")
		return
	}
	wr, err := f.Append()
	if err != nil {
		w.WriteHeader(500)
		fmt.Fprintln(w, err.Error())
		return
	}
	hw := util.NewMD5Writer(wr)
	_, err = io.Copy(hw, r.Body)
	err2 := wr.Close()
	r.Body.Close()
	w.Header().Set("Location", "/upload/"+f.ID)
	if err != nil {
		w.WriteHeader(500)
		fmt.Fprintln(w, err.Error())
		return
	}
	if err2 != nil {
		w.WriteHeader(500)
		fmt.Fprintln(w, err2.Error())
		return
	}
	if len(md5bytes) > 0 {
		_, ok := hw.CheckMD5(md5bytes)
		if !ok {
			w.WriteHeader(412)
			fmt.Fprintln(w, "MD5 mismatch")
			f.Rollback()
			return
		}
	}
}

func randomid() string {
	var n = rand.Int31()
	return strconv.FormatInt(int64(n), 36)
}

//{"DELETE", "/upload/:fileid", DeleteFileHandler},
// This deletes a blob which has been uploaded to a transactions, but not committed
// into an item yet. If an item has already been committed, then a "delete"
// command is needed instead. I know, it is confusing.
func DeleteFileHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	fileid := ps.ByName("fileid")
	err := FileStore.Delete(fileid)
	if err != nil {
		w.WriteHeader(500)
		fmt.Fprintln(w, err.Error())
	}
}

//{"PUT", "/upload/:fileid/metadata", SetFileInfoHandler},
func SetFileInfoHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	fileid := ps.ByName("fileid")
	f := FileStore.Lookup(fileid)
	if f == nil {
		w.WriteHeader(404)
		fmt.Fprintln(w, "cannot find file")
		return
	}
	// TODO(dbrower): use a limit reader to 1MB(?) for this
	var metadata fragment.File
	err := json.NewDecoder(r.Body).Decode(&metadata)
	if err != nil {
		w.WriteHeader(400)
		fmt.Fprintln(w, err.Error())
		return
	}
	if len(metadata.Payload) > 0 {
		f.SetPayload(metadata.Payload)
	}
	if len(metadata.Labels) > 0 {
		f.SetLabels(metadata.Labels)
	}
}

//{"GET", "/upload/:fileid", GetFileHandler},
func GetFileHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	fileid := ps.ByName("fileid")
	f := FileStore.Lookup(fileid)
	if f == nil {
		w.WriteHeader(404)
		fmt.Fprintln(w, "Unknown file identifier")
		return
	}
	fd := f.Open()
	io.Copy(w, fd)
	fd.Close()
}
