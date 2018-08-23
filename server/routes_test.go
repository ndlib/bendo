package server

import (
	"testing"

	"github.com/ndlib/bendo/store"
)

const (
	typeMemory = iota
	typeFileSystem
	typeS3
)

func TestGetCacheStore(t *testing.T) {
	var table = []struct {
		cachedir string
		typ      int
		bucket   string
		prefix   string
	}{
		{"", typeMemory, "", ""},
		{"rel/path", typeFileSystem, "", ""},
		{"/abs/path/", typeFileSystem, "", ""},
		{"file:/rel/path", typeFileSystem, "", ""},
		{"file:rel/path", typeFileSystem, "", ""},
		{"s3:/bucket", typeS3, "bucket", ""},
		{"s3://localhost:9000/bucket/prefix/", typeS3, "bucket", "prefix/"},
	}

	for _, row := range table {
		t.Log(row.cachedir)
		s := &RESTServer{CacheDir: row.cachedir}
		result := s.getcachestore("")
		switch x := result.(type) {
		case *store.Memory:
			if row.typ != typeMemory {
				t.Errorf("unexpected received %#v", result)
			}
		case *store.FileSystem:
			if row.typ != typeFileSystem {
				t.Errorf("unexpected received %#v", result)
			}
		case *store.S3:
			if row.typ != typeS3 {
				t.Errorf("unexpected received %#v", result)
			}
			if x.Bucket != row.bucket {
				t.Error("expected bucket", row.bucket, "received", x.Bucket)
			}
			if x.Prefix != row.prefix {
				t.Error("expected prefix", row.prefix, "received", x.Prefix)
			}
		}
	}
}
