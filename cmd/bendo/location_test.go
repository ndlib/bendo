package main

import (
	"os"
	"testing"

	"github.com/ndlib/bendo/store"
)

const (
	typeMemory = iota
	typeFileSystem
	typeS3
	typeBlackPearl
)

func TestSplitBucketPrefix(t *testing.T) {
	var table = []struct {
		location string
		addition string
		bucket   string
		prefix   string
	}{
		{"", "", "", ""},
		{"rel/path", "", "rel", "path/"},
		{"/abs/path/", "", "abs", "path/"},
		{"/bucket", "", "bucket", ""},
		{"/bucket", "more", "bucket", "more/"},
		{"/bucket/prefix/", "", "bucket", "prefix/"},
		{"/bucket/prefix", "", "bucket", "prefix/"},
		{"/bucket/prefix", "more", "bucket", "prefix/more/"},
		{"/bucket/prefix/", "more", "bucket", "prefix/more/"},
	}

	for _, row := range table {
		t.Log(row.location, row.addition)
		bucket, prefix := splitBucketPrefix(row.location, row.addition)
		if bucket != row.bucket {
			t.Error("expected bucket", row.bucket, "received", bucket)
		}
		if prefix != row.prefix {
			t.Error("expected prefix", row.prefix, "received", prefix)
		}
	}
}

func TestParseLocation(t *testing.T) {
	var table = []struct {
		location string
		addition string
		typ      int
		bucket   string
		prefix   string
	}{
		{"", "", typeMemory, "", ""},
		{"rel/path", "", typeFileSystem, "", ""},
		{"/abs/path/", "", typeFileSystem, "", ""},
		{"file:/rel/path", "", typeFileSystem, "", ""},
		{"file:rel/path", "", typeFileSystem, "", ""},
		{"s3:/bucket", "", typeS3, "bucket", ""},
		{"s3:/bucket", "more", typeS3, "bucket", "more/"},
		{"s3://localhost:9000/bucket/prefix/", "", typeS3, "bucket", "prefix/"},
		{"s3://localhost:9000/bucket/prefix/", "more", typeS3, "bucket", "prefix/more/"},
		{"blackpearl:/bucket", "", typeBlackPearl, "bucket", ""},
		{"blackpearl:/bucket", "more", typeBlackPearl, "bucket", "more/"},
		{"blackpearl://192.168.1.70:9000/bucket/prefix/", "", typeBlackPearl, "bucket", "prefix/"},
		{"blackpearl://localhost:9000/bucket/prefix/", "more", typeBlackPearl, "bucket", "prefix/more/"},
		{"blackpearls://localhost:9000/bucket/prefix/", "more", typeBlackPearl, "bucket", "prefix/more/"},
	}

	for _, row := range table {
		t.Log(row.location, row.addition)
		result := parselocation(row.location, row.addition)
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
		case *store.BlackPearl:
			if row.typ != typeBlackPearl {
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

func init() {
	// set these env variables if not already set.
	if x := os.Getenv("DS3_ACCESS_KEY"); x == "" {
		os.Setenv("DS3_ACCESS_KEY", "192.168.1.70:8008")
	}
	if x := os.Getenv("DS3_SECRET_KEY"); x == "" {
		os.Setenv("DS3_SECRET_KEY", "192.168.1.70:8008")
	}
}
