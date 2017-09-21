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
<dt>MimeType</dt><dd>{{ .MimeType }}</dd>
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
	uploadMD5 := getHexadecimalHeader(r, "X-Upload-Md5")
	uploadSHA256 := getHexadecimalHeader(r, "X-Upload-Sha256")
	if len(uploadMD5)+len(uploadSHA256) == 0 {
		w.WriteHeader(400)
		fmt.Fprintf(w, "At least one of X-Upload-Md5 or X-Upload-Sha256 must be provided")
		return
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
	hw := util.NewHashWriter(wr)
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
	var ok = true
	if len(uploadMD5) > 0 {
		_, ok = hw.CheckMD5(uploadMD5)
	}
	if ok && len(uploadSHA256) > 0 {
		_, ok = hw.CheckSHA256(uploadSHA256)
	}
	if !ok {
		w.WriteHeader(412)
		fmt.Fprintln(w, "Checksum mismatch")
		f.Rollback()
		return
	}
	v := r.Header.Get("Content-Type")
	if v != "" {
		f.SetMimeType(v)
	}
}

// getHexadecimalHeader returns the value for `header`, after first
// translating it from hexadecimal to binary. If the header doesn't exist
// or is not valid hexadecimal, returns an empty slice.
func getHexadecimalHeader(r *http.Request, header string) []byte {
	v := r.Header.Get(header)
	if v == "" {
		return nil
	}
	result, _ := hex.DecodeString(v)
	return result
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
	if metadata.MimeType != "" {
		f.SetMimeType(metadata.MimeType)
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
