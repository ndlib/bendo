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

// ListFileHandler handles requests to GET /upload
func (s *RESTServer) ListFileHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	q := r.URL.Query()
	if len(q["label"]) > 0 {
		writeHTMLorJSON(w, r, listFileTemplate, s.FileStore.ListFiltered(q["label"]))
	} else {
		writeHTMLorJSON(w, r, listFileTemplate, s.FileStore.List())
	}
}

var (
	listFileTemplate = template.Must(template.New("listfile").Parse(`<html>
<h1>Files</h1>
<ol>
{{ range . }}
	<li><a href="/upload/{{ . }}/metadata">{{ . }}</a></li>
{{ else }}
	<li>No Files</li>
{{ end }}
</ol>
</html>`))
)

// GetFileInfoHandler handles requests to GET /upload/:fileid/metadata
func (s *RESTServer) GetFileInfoHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	id := ps.ByName("fileid")
	f := s.FileStore.Lookup(id)
	if f == nil {
		w.WriteHeader(404)
		fmt.Fprintln(w, "cannot find file")
		return
	}
	fstat := f.Stat()
	writeHTMLorJSON(w, r, fileInfoTemplate, fstat)
}

var (
	fileInfoTemplate = template.Must(template.New("fileinfo").Parse(`<html>
<h1>File Info</h1>
{{ $fileid := .ID }}
<dl>
<dt>ID</dt><dd>{{ .ID }}</dd>
<dt>Size</dt><dd>{{ .Size }}</dd>
<dt>Fragments</dt><dd>{{ .NFragments }}</dd>
<dt>Created</dt><dd>{{ .Created }}</dd>
<dt>Modified</dt><dd>{{ .Modified }}</dd>
<dt>Creator</dt><dd>{{ .Creator }}</dd>
<dt>Extra</dt><dd>{{ .Extra }}</dd>
<dt>MD5</dt><dd>{{ .MD5 }}</dd>
<dt>SHA256</dt><dd>{{ .SHA256 }}</dd>
<dt>Labels</dt><dd>{{ range .Labels }}{{ . }}<br/>{{ end }}</dd>
</dl>
<a href="/upload/{{ $fileid }}">View content</a></br>
<a href="/upload">Back</a>
</html>`))
)

// AppendFileHandler handles requests to both POST /upload and POST /upload/:fileid
func (s *RESTServer) AppendFileHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
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
	var f fragment.FileEntry // the file to append to
	// if no file was given, make a new one
	// if a file id was given, but doesn't exist...create it
	if fileid == "" {
		for f == nil {
			id := randomid()
			f = s.FileStore.New(id)
		}
	} else {
		// New returns nil if the file already exists!
		f = s.FileStore.New(fileid)
		if f == nil {
			f = s.FileStore.Lookup(fileid)
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
	w.Header().Set("Location", "/upload/"+f.Stat().ID)
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

// DeleteFileHandler handles requests to DELETE /upload/:fileid
// This deletes a file which has been uploaded and is in the temporary
// holding area.
func (s *RESTServer) DeleteFileHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	fileid := ps.ByName("fileid")
	err := s.FileStore.Delete(fileid)
	if err != nil {
		w.WriteHeader(500)
		fmt.Fprintln(w, err.Error())
	}
}

// SetFileInfoHandler handles requests to PUT /upload/:fileid/metadata
func (s *RESTServer) SetFileInfoHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	fileid := ps.ByName("fileid")
	f := s.FileStore.Lookup(fileid)
	if f == nil {
		w.WriteHeader(404)
		fmt.Fprintln(w, "cannot find file")
		return
	}
	// TODO(dbrower): use a limit reader to 1MB(?) for this
	var metadata fragment.Stat
	err := json.NewDecoder(r.Body).Decode(&metadata)
	if err != nil {
		w.WriteHeader(400)
		fmt.Fprintln(w, err.Error())
		return
	}
	if len(metadata.Labels) > 0 {
		f.SetLabels(metadata.Labels)
	}
	if len(metadata.Extra) > 0 {
		f.SetExtra(metadata.Extra)
	}
}

// GetFileHandler handles requests to GET /upload/:fileid
func (s *RESTServer) GetFileHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	fileid := ps.ByName("fileid")
	f := s.FileStore.Lookup(fileid)
	if f == nil {
		w.WriteHeader(404)
		fmt.Fprintln(w, "Unknown file identifier")
		return
	}
	fd := f.Open()
	io.Copy(w, fd)
	fd.Close()
}
